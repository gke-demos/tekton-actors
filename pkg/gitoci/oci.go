package gitoci

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	maxOCIBlobBytes     = 256 << 20 // 256 MiB
	maxOCIManifestBytes = 4 << 20   // 4 MiB
)

var (
	ociRepoNameRe = regexp.MustCompile(`^[a-z0-9]+(([._-]|__|[-]*)[a-z0-9]+)*(/[a-z0-9]+(([._-]|__|[-]*)[a-z0-9]+)*)*$`)
	ociDigestRe   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	ociTagRe      = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)
	ociUploadIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type storedManifestMeta struct {
	Digest    string `json:"digest"`
	MediaType string `json:"mediaType"`
	Size      int64  `json:"size"`
}

func (s *Server) handleOCIV2(w http.ResponseWriter, r *http.Request) {
	authOK, hint := s.verifyBearer(r)
	if !authOK {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolOCI,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusUnauthorized,
			Authenticated:   false,
			TokenSourceHint: hint,
		})
		writeBearerChallenge(w, ProtocolOCI)
		return
	}

	setOCICommonHeaders(w)

	// Base version check: GET/HEAD /v2/
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, "", "", "")
			writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("{}\n"))
		}
		s.recordOCIAudit(r, http.StatusOK, hint, "", "", "")
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/v2/")

	switch {
	case strings.HasSuffix(rest, "/blobs/uploads/") || strings.HasSuffix(rest, "/blobs/uploads"):
		repo := strings.TrimSuffix(strings.TrimSuffix(rest, "/"), "/blobs/uploads")
		s.handleOCIStartUpload(w, r, repo, hint)
		return

	case strings.Contains(rest, "/blobs/uploads/"):
		idx := strings.LastIndex(rest, "/blobs/uploads/")
		repo := rest[:idx]
		uploadID := rest[idx+len("/blobs/uploads/"):]
		s.handleOCIUploadSession(w, r, repo, uploadID, hint)
		return

	case strings.Contains(rest, "/blobs/"):
		idx := strings.LastIndex(rest, "/blobs/")
		repo := rest[:idx]
		digest := rest[idx+len("/blobs/"):]
		s.handleOCIBlob(w, r, repo, digest, hint)
		return

	case strings.Contains(rest, "/manifests/"):
		idx := strings.LastIndex(rest, "/manifests/")
		repo := rest[:idx]
		ref := rest[idx+len("/manifests/"):]
		s.handleOCIManifest(w, r, repo, ref, hint)
		return

	case strings.HasSuffix(rest, "/tags/list"):
		repo := strings.TrimSuffix(rest, "/tags/list")
		s.handleOCITagsList(w, r, repo, hint)
		return

	default:
		s.recordOCIAudit(r, http.StatusNotFound, hint, "", "", "")
		writeOCIError(w, http.StatusNotFound, "NAME_UNKNOWN", "unsupported OCI v2 endpoint")
		return
	}
}

func (s *Server) handleOCIStartUpload(w http.ResponseWriter, r *http.Request, repo, hint string) {
	if !ociRepoNameRe.MatchString(repo) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "NAME_INVALID", "invalid repository name")
		return
	}
	if r.Method != http.MethodPost {
		s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, repo, "", "")
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}

	uploadID, err := randomHexID()
	if err != nil {
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed to allocate upload ID")
		return
	}
	uploadPath := filepath.Join(s.dataDir, "oci", "uploads", uploadID)
	f, err := os.OpenFile(uploadPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed to create upload file")
		return
	}

	// Support monolithic POST /v2/<name>/blobs/uploads/?digest=sha256:... directly as well.
	if digest := strings.TrimSpace(r.URL.Query().Get("digest")); digest != "" {
		limited := http.MaxBytesReader(w, r.Body, maxOCIBlobBytes)
		defer limited.Close()
		n, copyErr := io.Copy(f, limited)
		_ = f.Close()
		if copyErr != nil {
			_ = os.Remove(uploadPath)
			s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
			writeOCIError(w, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", "failed reading upload body")
			return
		}
		s.finalizeUploadFile(w, r, repo, uploadPath, n, digest, hint)
		return
	}
	_ = f.Close()

	s.ociMu.Lock()
	s.uploads[uploadID] = &ociUploadSession{
		repo:     repo,
		filePath: uploadPath,
		size:     0,
	}
	s.ociMu.Unlock()

	loc := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repo, uploadID)
	w.Header().Set("Location", loc)
	w.Header().Set("Docker-Upload-UUID", uploadID)
	w.Header().Set("Range", "0-0")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
	s.recordOCIAudit(r, http.StatusAccepted, hint, repo, "", "")
}

func (s *Server) handleOCIUploadSession(w http.ResponseWriter, r *http.Request, repo, uploadID, hint string) {
	if !ociRepoNameRe.MatchString(repo) || !ociUploadIDRe.MatchString(uploadID) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", "invalid upload target")
		return
	}

	switch r.Method {
	case http.MethodPatch:
		s.ociMu.Lock()
		sess, ok := s.uploads[uploadID]
		if !ok || sess.repo != repo {
			s.ociMu.Unlock()
			s.recordOCIAudit(r, http.StatusNotFound, hint, repo, "", "")
			writeOCIError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session not found")
			return
		}
		remaining := maxOCIBlobBytes - sess.size
		filePath := sess.filePath
		s.ociMu.Unlock()

		if remaining <= 0 {
			writeOCIError(w, http.StatusRequestEntityTooLarge, "SIZE_INVALID", "blob exceeds maximum size")
			return
		}
		limited := http.MaxBytesReader(w, r.Body, remaining)
		defer limited.Close()

		f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed to open upload session")
			return
		}
		n, copyErr := io.Copy(f, limited)
		_ = f.Close()
		if copyErr != nil {
			s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
			writeOCIError(w, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", "failed reading upload chunk")
			return
		}

		s.ociMu.Lock()
		sess, ok = s.uploads[uploadID]
		if !ok {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session closed")
			return
		}
		sess.size += n
		endRange := int64(0)
		if sess.size > 0 {
			endRange = sess.size - 1
		}
		s.ociMu.Unlock()

		loc := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repo, uploadID)
		w.Header().Set("Location", loc)
		w.Header().Set("Docker-Upload-UUID", uploadID)
		w.Header().Set("Range", fmt.Sprintf("0-%d", endRange))
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusAccepted)
		s.recordOCIAudit(r, http.StatusAccepted, hint, repo, "", "")
		return

	case http.MethodPut:
		digest := strings.TrimSpace(r.URL.Query().Get("digest"))
		s.ociMu.Lock()
		sess, ok := s.uploads[uploadID]
		if !ok || sess.repo != repo {
			s.ociMu.Unlock()
			s.recordOCIAudit(r, http.StatusNotFound, hint, repo, "", "")
			writeOCIError(w, http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "upload session not found")
			return
		}
		delete(s.uploads, uploadID)
		s.ociMu.Unlock()

		remaining := maxOCIBlobBytes - sess.size
		if remaining < 0 {
			_ = os.Remove(sess.filePath)
			writeOCIError(w, http.StatusRequestEntityTooLarge, "SIZE_INVALID", "blob exceeds maximum size")
			return
		}
		if r.Body != nil {
			limited := http.MaxBytesReader(w, r.Body, remaining)
			defer limited.Close()
			f, err := os.OpenFile(sess.filePath, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				_ = os.Remove(sess.filePath)
				writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed to open upload session")
				return
			}
			n, copyErr := io.Copy(f, limited)
			_ = f.Close()
			if copyErr != nil {
				_ = os.Remove(sess.filePath)
				s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
				writeOCIError(w, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", "failed reading final upload body")
				return
			}
			sess.size += n
		}
		s.finalizeUploadFile(w, r, repo, sess.filePath, sess.size, digest, hint)
		return

	default:
		s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, repo, "", "")
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
}

func (s *Server) finalizeUploadFile(w http.ResponseWriter, r *http.Request, repo, stagedPath string, _ int64, expectedDigest, hint string) {
	defer func() {
		_ = os.Remove(stagedPath)
	}()

	if !ociDigestRe.MatchString(expectedDigest) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid or missing sha256 digest parameter")
		return
	}

	f, err := os.Open(stagedPath)
	if err != nil {
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed to verify uploaded blob")
		return
	}
	hasher := sha256.New()
	_, copyErr := io.Copy(hasher, f)
	_ = f.Close()
	if copyErr != nil {
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed hashing uploaded blob")
		return
	}

	actualHex := hex.EncodeToString(hasher.Sum(nil))
	actualDigest := "sha256:" + actualHex
	if actualDigest != expectedDigest {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "DIGEST_INVALID", fmt.Sprintf("computed digest %s does not match %s", actualDigest, expectedDigest))
		return
	}

	s.ociMu.Lock()
	blobDst := filepath.Join(s.dataDir, "oci", "blobs", "sha256", actualHex)
	if _, statErr := os.Stat(blobDst); errors.Is(statErr, os.ErrNotExist) {
		if renameErr := os.Rename(stagedPath, blobDst); renameErr != nil {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed storing blob")
			return
		}
	}
	repoBlobDir := filepath.Join(s.repoDir(repo), "blobs")
	if mkErr := os.MkdirAll(repoBlobDir, 0o755); mkErr != nil {
		s.ociMu.Unlock()
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed linking repository blob")
		return
	}
	if wErr := os.WriteFile(filepath.Join(repoBlobDir, actualHex), []byte(actualDigest), 0o644); wErr != nil {
		s.ociMu.Unlock()
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed indexing repository blob")
		return
	}
	s.ociMu.Unlock()

	loc := fmt.Sprintf("/v2/%s/blobs/%s", repo, actualDigest)
	w.Header().Set("Location", loc)
	w.Header().Set("Docker-Content-Digest", actualDigest)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusCreated)
	s.recordOCIAudit(r, http.StatusCreated, hint, repo, "", actualDigest)
}

func (s *Server) handleOCIBlob(w http.ResponseWriter, r *http.Request, repo, digest, hint string) {
	if !ociRepoNameRe.MatchString(repo) || !ociDigestRe.MatchString(digest) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid repository or digest")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, repo, "", digest)
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}

	hexHash := strings.TrimPrefix(digest, "sha256:")

	s.ociMu.RLock()
	_, linkErr := os.Stat(filepath.Join(s.repoDir(repo), "blobs", hexHash))
	blobPath := filepath.Join(s.dataDir, "oci", "blobs", "sha256", hexHash)
	info, statErr := os.Stat(blobPath)
	s.ociMu.RUnlock()

	if linkErr != nil || statErr != nil || !info.Mode().IsRegular() {
		s.recordOCIAudit(r, http.StatusNotFound, hint, repo, "", digest)
		writeOCIError(w, http.StatusNotFound, "BLOB_UNKNOWN", "blob not found")
		return
	}

	f, err := os.Open(blobPath)
	if err != nil {
		writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed opening blob")
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = io.Copy(w, f)
	}
	s.recordOCIAudit(r, http.StatusOK, hint, repo, "", digest)
}

func (s *Server) handleOCIManifest(w http.ResponseWriter, r *http.Request, repo, ref, hint string) {
	if !ociRepoNameRe.MatchString(repo) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "NAME_INVALID", "invalid repository name")
		return
	}
	isDigestRef := ociDigestRe.MatchString(ref)
	isTagRef := !isDigestRef && ociTagRe.MatchString(ref)
	if !isDigestRef && !isTagRef {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "MANIFEST_INVALID", "invalid manifest reference")
		return
	}

	switch r.Method {
	case http.MethodPut:
		limited := http.MaxBytesReader(w, r.Body, maxOCIManifestBytes)
		defer limited.Close()
		body, err := io.ReadAll(limited)
		if err != nil || len(body) == 0 {
			s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
			writeOCIError(w, http.StatusBadRequest, "MANIFEST_INVALID", "failed reading manifest body")
			return
		}
		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err != nil {
			s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
			writeOCIError(w, http.StatusBadRequest, "MANIFEST_INVALID", "manifest must be valid JSON")
			return
		}

		sum := sha256.Sum256(body)
		hexHash := hex.EncodeToString(sum[:])
		digest := "sha256:" + hexHash
		if isDigestRef && ref != digest {
			s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
			writeOCIError(w, http.StatusBadRequest, "DIGEST_INVALID", "manifest digest mismatch")
			return
		}

		mediaType := strings.TrimSpace(r.Header.Get("Content-Type"))
		if mediaType == "" {
			if mt, ok := parsed["mediaType"].(string); ok && mt != "" {
				mediaType = mt
			} else {
				mediaType = "application/vnd.oci.image.manifest.v1+json"
			}
		}

		meta := storedManifestMeta{
			Digest:    digest,
			MediaType: mediaType,
			Size:      int64(len(body)),
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed encoding manifest metadata")
			return
		}

		s.ociMu.Lock()
		manifestsDir := filepath.Join(s.repoDir(repo), "manifests")
		tagsDir := filepath.Join(s.repoDir(repo), "tags")
		if err := os.MkdirAll(manifestsDir, 0o755); err != nil {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed creating manifests dir")
			return
		}
		if err := os.MkdirAll(tagsDir, 0o755); err != nil {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed creating tags dir")
			return
		}
		if err := os.WriteFile(filepath.Join(manifestsDir, hexHash+".json"), body, 0o644); err != nil {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed storing manifest")
			return
		}
		if err := os.WriteFile(filepath.Join(manifestsDir, hexHash+".meta.json"), metaBytes, 0o644); err != nil {
			s.ociMu.Unlock()
			writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed storing manifest metadata")
			return
		}
		tagRecorded := ""
		if isTagRef {
			tagRecorded = ref
			if err := os.WriteFile(filepath.Join(tagsDir, ref), []byte(digest), 0o644); err != nil {
				s.ociMu.Unlock()
				writeOCIError(w, http.StatusInternalServerError, "UNKNOWN", "failed updating tag")
				return
			}
		}
		s.ociMu.Unlock()

		w.Header().Set("Location", fmt.Sprintf("/v2/%s/manifests/%s", repo, digest))
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusCreated)
		s.recordOCIAudit(r, http.StatusCreated, hint, repo, tagRecorded, digest)
		return

	case http.MethodGet, http.MethodHead:
		s.ociMu.RLock()
		digest := ref
		tagRecorded := ""
		if isTagRef {
			tagRecorded = ref
			tagBytes, err := os.ReadFile(filepath.Join(s.repoDir(repo), "tags", ref))
			if err != nil {
				s.ociMu.RUnlock()
				s.recordOCIAudit(r, http.StatusNotFound, hint, repo, tagRecorded, "")
				writeOCIError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "tag not found")
				return
			}
			digest = strings.TrimSpace(string(tagBytes))
		}
		hexHash := strings.TrimPrefix(digest, "sha256:")
		manifestsDir := filepath.Join(s.repoDir(repo), "manifests")
		body, readErr := os.ReadFile(filepath.Join(manifestsDir, hexHash+".json"))
		metaBytes, metaErr := os.ReadFile(filepath.Join(manifestsDir, hexHash+".meta.json"))
		s.ociMu.RUnlock()

		if readErr != nil || metaErr != nil {
			s.recordOCIAudit(r, http.StatusNotFound, hint, repo, tagRecorded, digest)
			writeOCIError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest not found")
			return
		}

		var meta storedManifestMeta
		if err := json.Unmarshal(metaBytes, &meta); err != nil || meta.MediaType == "" {
			meta.MediaType = "application/vnd.oci.image.manifest.v1+json"
		}

		w.Header().Set("Content-Type", meta.MediaType)
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
		s.recordOCIAudit(r, http.StatusOK, hint, repo, tagRecorded, digest)
		return

	default:
		s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, repo, "", "")
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
}

func (s *Server) handleOCITagsList(w http.ResponseWriter, r *http.Request, repo, hint string) {
	if !ociRepoNameRe.MatchString(repo) {
		s.recordOCIAudit(r, http.StatusBadRequest, hint, repo, "", "")
		writeOCIError(w, http.StatusBadRequest, "NAME_INVALID", "invalid repository name")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.recordOCIAudit(r, http.StatusMethodNotAllowed, hint, repo, "", "")
		writeOCIError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}

	s.ociMu.RLock()
	entries, err := os.ReadDir(filepath.Join(s.repoDir(repo), "tags"))
	s.ociMu.RUnlock()

	tags := make([]string, 0, len(entries))
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				tags = append(tags, entry.Name())
			}
		}
		sort.Strings(tags)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}{
			Name: repo,
			Tags: tags,
		})
	}
	s.recordOCIAudit(r, http.StatusOK, hint, repo, "", "")
}

func (s *Server) repoDir(repo string) string {
	sum := sha256.Sum256([]byte(repo))
	return filepath.Join(s.dataDir, "oci", "repos", hex.EncodeToString(sum[:]))
}

func (s *Server) recordOCIAudit(r *http.Request, status int, hint, repo, tag, digest string) {
	s.recordAudit(AuditEvent{
		Protocol:        ProtocolOCI,
		Method:          r.Method,
		Path:            r.URL.Path,
		StatusCode:      status,
		Authenticated:   true,
		TokenSourceHint: hint,
		Repository:      repo,
		Tag:             tag,
		Digest:          digest,
	})
}

func setOCICommonHeaders(w http.ResponseWriter) {
	setSecurityHeaders(w)
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
}

func writeOCIError(w http.ResponseWriter, status int, code, msg string) {
	setOCICommonHeaders(w)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}{
		Errors: []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{
			{Code: code, Message: msg},
		},
	})
}

func randomHexID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
