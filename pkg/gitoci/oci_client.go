package gitoci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

// PushBinaryRequest configures packaging and pushing a single static Go binary
// as a compliant OCI Image v1 manifest to an OCI Distribution Spec v2 registry.
type PushBinaryRequest struct {
	RegistryBaseURL string
	Repository      string
	Tag             string
	BinaryPath      string
	BearerToken     string
	CredentialMode  string
	Annotations     map[string]string
	HTTPClient      *http.Client
}

// PushBinaryResult reports the digests published by PushBinaryOCIImage.
type PushBinaryResult struct {
	ManifestDigest string
	ConfigDigest   string
	LayerDigest    string
}

type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

type ociImageConfig struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Config       struct {
		Entrypoint   []string            `json:"Entrypoint"`
		ExposedPorts map[string]struct{} `json:"ExposedPorts,omitempty"`
	} `json:"config"`
	RootFS struct {
		Type    string   `json:"type"`
		DiffIDs []string `json:"diff_ids"`
	} `json:"rootfs"`
}

type ociImageManifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Config        ociDescriptor     `json:"config"`
	Layers        []ociDescriptor   `json:"layers"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// PushBinaryOCIImage packages BinaryPath into a deterministic single-layer
// linux/amd64 OCI v1 image and pushes config + layer blobs + manifest to RegistryBaseURL.
func PushBinaryOCIImage(ctx context.Context, req PushBinaryRequest) (PushBinaryResult, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(req.RegistryBaseURL), "/")
	if baseURL == "" {
		return PushBinaryResult{}, errors.New("RegistryBaseURL is required")
	}
	if !ociRepoNameRe.MatchString(req.Repository) {
		return PushBinaryResult{}, fmt.Errorf("invalid Repository %q", req.Repository)
	}
	if !ociTagRe.MatchString(req.Tag) {
		return PushBinaryResult{}, fmt.Errorf("invalid Tag %q", req.Tag)
	}
	if strings.TrimSpace(req.BinaryPath) == "" {
		return PushBinaryResult{}, errors.New("BinaryPath is required")
	}

	binBytes, err := os.ReadFile(req.BinaryPath)
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("read binary %s: %w", req.BinaryPath, err)
	}
	binName := path.Base(strings.ReplaceAll(req.BinaryPath, "\\", "/"))
	if binName == "" || binName == "." || binName == "/" {
		binName = DefaultRepoName
	}

	// 1. Build deterministic uncompressed tar and gzipped layer bytes.
	var rawTar bytes.Buffer
	tw := tar.NewWriter(&rawTar)
	hdr := &tar.Header{
		Name:     binName,
		Mode:     0o755,
		Size:     int64(len(binBytes)),
		ModTime:  time.Unix(0, 0).UTC(),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return PushBinaryResult{}, fmt.Errorf("write tar header: %w", err)
	}
	if _, err := tw.Write(binBytes); err != nil {
		return PushBinaryResult{}, fmt.Errorf("write tar payload: %w", err)
	}
	if err := tw.Close(); err != nil {
		return PushBinaryResult{}, fmt.Errorf("close tar: %w", err)
	}

	diffIDSum := sha256.Sum256(rawTar.Bytes())
	diffID := "sha256:" + hex.EncodeToString(diffIDSum[:])

	var gzLayer bytes.Buffer
	gzw, err := gzip.NewWriterLevel(&gzLayer, gzip.BestCompression)
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("init gzip: %w", err)
	}
	gzw.ModTime = time.Unix(0, 0).UTC()
	if _, err := gzw.Write(rawTar.Bytes()); err != nil {
		return PushBinaryResult{}, fmt.Errorf("gzip layer: %w", err)
	}
	if err := gzw.Close(); err != nil {
		return PushBinaryResult{}, fmt.Errorf("close gzip: %w", err)
	}

	layerBytes := gzLayer.Bytes()
	layerSum := sha256.Sum256(layerBytes)
	layerDigest := "sha256:" + hex.EncodeToString(layerSum[:])

	// 2. Build OCI Image Config JSON.
	var imgCfg ociImageConfig
	imgCfg.Architecture = "amd64"
	imgCfg.OS = "linux"
	imgCfg.Config.Entrypoint = []string{"/" + binName}
	imgCfg.Config.ExposedPorts = map[string]struct{}{"8080/tcp": {}}
	imgCfg.RootFS.Type = "layers"
	imgCfg.RootFS.DiffIDs = []string{diffID}

	cfgBytes, err := json.Marshal(imgCfg)
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("marshal image config: %w", err)
	}
	cfgSum := sha256.Sum256(cfgBytes)
	cfgDigest := "sha256:" + hex.EncodeToString(cfgSum[:])

	client := req.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	// 3. Upload config and layer blobs using chunked POST -> PATCH -> PUT.
	if err := uploadOCIBlob(ctx, client, baseURL, req.Repository, req.BearerToken, req.CredentialMode, cfgDigest, cfgBytes); err != nil {
		return PushBinaryResult{}, fmt.Errorf("upload config blob: %w", err)
	}
	if err := uploadOCIBlob(ctx, client, baseURL, req.Repository, req.BearerToken, req.CredentialMode, layerDigest, layerBytes); err != nil {
		return PushBinaryResult{}, fmt.Errorf("upload layer blob: %w", err)
	}

	// 4. Push OCI Image Manifest.
	manifest := ociImageManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config: ociDescriptor{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Digest:    cfgDigest,
			Size:      int64(len(cfgBytes)),
		},
		Layers: []ociDescriptor{
			{
				MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
				Digest:    layerDigest,
				Size:      int64(len(layerBytes)),
			},
		},
		Annotations: req.Annotations,
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("marshal image manifest: %w", err)
	}

	manifestURL := fmt.Sprintf("%s/v2/%s/manifests/%s", baseURL, req.Repository, req.Tag)
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, manifestURL, bytes.NewReader(manifestBytes))
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("build manifest PUT request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	applyAuthHeaders(putReq, req.BearerToken, req.CredentialMode)

	putResp, err := client.Do(putReq)
	if err != nil {
		return PushBinaryResult{}, fmt.Errorf("PUT manifest: %w", err)
	}
	respBody, _ := io.ReadAll(io.LimitReader(putResp.Body, 4096))
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		return PushBinaryResult{}, fmt.Errorf("PUT manifest status %d: %s", putResp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	manifestDigest := strings.TrimSpace(putResp.Header.Get("Docker-Content-Digest"))
	if manifestDigest == "" {
		sum := sha256.Sum256(manifestBytes)
		manifestDigest = "sha256:" + hex.EncodeToString(sum[:])
	}

	return PushBinaryResult{
		ManifestDigest: manifestDigest,
		ConfigDigest:   cfgDigest,
		LayerDigest:    layerDigest,
	}, nil
}

func uploadOCIBlob(ctx context.Context, client *http.Client, baseURL, repo, token, credMode, digest string, payload []byte) error {
	startURL := fmt.Sprintf("%s/v2/%s/blobs/uploads/", baseURL, repo)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, startURL, nil)
	if err != nil {
		return err
	}
	applyAuthHeaders(postReq, token, credMode)

	postResp, err := client.Do(postReq)
	if err != nil {
		return err
	}
	postBody, _ := io.ReadAll(io.LimitReader(postResp.Body, 4096))
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("start upload status %d: %s", postResp.StatusCode, strings.TrimSpace(string(postBody)))
	}

	loc, err := resolveRegistryLocation(baseURL, postResp.Header.Get("Location"))
	if err != nil {
		return err
	}

	patchReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, loc, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	patchReq.Header.Set("Content-Type", "application/octet-stream")
	applyAuthHeaders(patchReq, token, credMode)

	patchResp, err := client.Do(patchReq)
	if err != nil {
		return err
	}
	patchBody, _ := io.ReadAll(io.LimitReader(patchResp.Body, 4096))
	patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("patch upload status %d: %s", patchResp.StatusCode, strings.TrimSpace(string(patchBody)))
	}

	if nextLoc := strings.TrimSpace(patchResp.Header.Get("Location")); nextLoc != "" {
		loc, err = resolveRegistryLocation(baseURL, nextLoc)
		if err != nil {
			return err
		}
	}

	putURL, err := appendDigestQuery(loc, digest)
	if err != nil {
		return err
	}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, nil)
	if err != nil {
		return err
	}
	applyAuthHeaders(putReq, token, credMode)

	putResp, err := client.Do(putReq)
	if err != nil {
		return err
	}
	putBody, _ := io.ReadAll(io.LimitReader(putResp.Body, 4096))
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusCreated {
		return fmt.Errorf("finalize upload status %d: %s", putResp.StatusCode, strings.TrimSpace(string(putBody)))
	}
	return nil
}

func applyAuthHeaders(r *http.Request, token, credMode string) {
	if strings.TrimSpace(token) != "" {
		r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	}
	if strings.TrimSpace(credMode) != "" {
		r.Header.Set("X-Tekton-Credential-Mode", strings.TrimSpace(credMode))
	}
}

func resolveRegistryLocation(baseURL, loc string) (string, error) {
	if strings.TrimSpace(loc) == "" {
		return "", errors.New("missing Location header from registry")
	}
	baseParsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	locParsed, err := url.Parse(loc)
	if err != nil {
		return "", err
	}
	return baseParsed.ResolveReference(locParsed).String(), nil
}

func appendDigestQuery(rawURL, digest string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("digest", digest)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
