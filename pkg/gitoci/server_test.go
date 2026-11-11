package gitoci_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/gitoci"
)

func randomTestToken(t *testing.T) string {
	t.Helper()
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return "test-tok-" + hex.EncodeToString(buf[:])
}

func TestGitSmartHTTPAuthCloneAndAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token := randomTestToken(t)
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: token,
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Readiness and health probes succeed without auth.
	for _, probePath := range []string{"/readyz", "/healthz"} {
		resp, err := http.Get(ts.URL + probePath)
		if err != nil {
			t.Fatalf("GET %s: %v", probePath, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", probePath, resp.StatusCode)
		}
	}

	repoURL := ts.URL + "/repo/fulfillment-api.git"

	// 2. Reject missing token, unrewritten ate-placeholder, and wrong token.
	rejectCases := []struct {
		name     string
		authHdr  string
		wantHint string
	}{
		{
			name:     "missing authorization header",
			authHdr:  "",
			wantHint: "missing",
		},
		{
			name:     "unrewritten ate-placeholder token",
			authHdr:  "Bearer ate-placeholder",
			wantHint: "unrewritten-ate-placeholder",
		},
		{
			name:     "wrong bearer token",
			authHdr:  "Bearer definitely-wrong-token",
			wantHint: "invalid-bearer",
		},
	}

	for _, rc := range rejectCases {
		t.Run(rc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, repoURL+"/info/refs?service=git-upload-pack", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if rc.authHdr != "" {
				req.Header.Set("Authorization", rc.authHdr)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, `Bearer realm="git-oci"`) {
				t.Fatalf("WWW-Authenticate = %q, want Bearer realm=\"git-oci\"", got)
			}
		})
	}

	// Also verify real git clone fails when unauthenticated.
	unauthDest := filepath.Join(t.TempDir(), "unauth-clone")
	unauthCmd := exec.CommandContext(ctx, "git", "clone", repoURL, unauthDest)
	unauthCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := unauthCmd.CombinedOutput(); err == nil {
		t.Fatalf("expected unauthenticated git clone to fail, but succeeded:\n%s", out)
	}

	// 3. Authenticated git clone of default branch (main) succeeds and persists zero secrets in .git/config.
	mainDest := filepath.Join(t.TempDir(), "main-clone")
	authHeader := fmt.Sprintf("Authorization: Bearer %s", token)
	cloneMain := exec.CommandContext(ctx, "git",
		"-c", "http.extraHeader="+authHeader,
		"-c", "http.extraHeader=X-Tekton-Credential-Mode: ate-egress",
		"clone", repoURL, mainDest,
	)
	cloneMain.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cloneMain.CombinedOutput(); err != nil {
		t.Fatalf("authenticated git clone (main) failed: %v\n%s", err, out)
	}

	gotMainSHA := gitRevParseHEAD(t, ctx, mainDest)
	if gotMainSHA == "" || gotMainSHA != srv.MainCommitSHA() {
		t.Fatalf("cloned main HEAD = %q, want %q", gotMainSHA, srv.MainCommitSHA())
	}

	mainPricing, err := os.ReadFile(filepath.Join(mainDest, "pricing.go"))
	if err != nil {
		t.Fatalf("read main pricing.go: %v", err)
	}
	if string(mainPricing) != gitoci.SampleAppCleanPricingGo {
		t.Fatalf("cloned main pricing.go mismatch")
	}

	gitConfigBytes, err := os.ReadFile(filepath.Join(mainDest, ".git", "config"))
	if err != nil {
		t.Fatalf("read .git/config: %v", err)
	}
	if strings.Contains(string(gitConfigBytes), token) {
		t.Fatalf(".git/config leaked bearer token!")
	}

	// 4. Authenticated git clone of buggy branch succeeds and matches BuggyCommitSHA.
	buggyDest := filepath.Join(t.TempDir(), "buggy-clone")
	cloneBuggy := exec.CommandContext(ctx, "git",
		"-c", "http.extraHeader="+authHeader,
		"-c", "http.extraHeader=X-Tekton-Credential-Mode: mounted-secret",
		"clone", "-b", "buggy", repoURL, buggyDest,
	)
	cloneBuggy.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cloneBuggy.CombinedOutput(); err != nil {
		t.Fatalf("authenticated git clone (buggy) failed: %v\n%s", err, out)
	}

	gotBuggySHA := gitRevParseHEAD(t, ctx, buggyDest)
	if gotBuggySHA == "" || gotBuggySHA != srv.BuggyCommitSHA() || gotBuggySHA == gotMainSHA {
		t.Fatalf("cloned buggy HEAD = %q, want distinct %q", gotBuggySHA, srv.BuggyCommitSHA())
	}
	buggyPricing, err := os.ReadFile(filepath.Join(buggyDest, "pricing.go"))
	if err != nil {
		t.Fatalf("read buggy pricing.go: %v", err)
	}
	if string(buggyPricing) != gitoci.SampleAppBuggyPricingGo {
		t.Fatalf("cloned buggy pricing.go mismatch")
	}

	// 5. Verify GET /api/audit captures rejected & authenticated Git events and never leaks raw token.
	auditResp, err := http.Get(ts.URL + "/api/audit")
	if err != nil {
		t.Fatalf("GET /api/audit: %v", err)
	}
	rawAuditBytes, err := io.ReadAll(auditResp.Body)
	auditResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/audit: %v", err)
	}
	if auditResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/audit status = %d, want 200", auditResp.StatusCode)
	}
	if strings.Contains(string(rawAuditBytes), token) {
		t.Fatalf("/api/audit leaked raw secret token!")
	}

	var audit gitoci.AuditResponse
	if err := json.Unmarshal(rawAuditBytes, &audit); err != nil {
		t.Fatalf("unmarshal AuditResponse: %v", err)
	}
	if audit.Summary.Rejected < 4 {
		t.Fatalf("Summary.Rejected = %d, want >= 4", audit.Summary.Rejected)
	}
	if audit.Summary.Authenticated < 2 {
		t.Fatalf("Summary.Authenticated = %d, want >= 2", audit.Summary.Authenticated)
	}
	if audit.Summary.GitRequests != audit.Summary.Total {
		t.Fatalf("GitRequests = %d, Total = %d", audit.Summary.GitRequests, audit.Summary.Total)
	}

	hintsSeen := map[string]bool{}
	for _, ev := range audit.Events {
		hintsSeen[ev.TokenSourceHint] = true
	}
	for _, wantHint := range []string{"missing", "unrewritten-ate-placeholder", "invalid-bearer", "ate-egress", "mounted-secret"} {
		if !hintsSeen[wantHint] {
			t.Fatalf("expected tokenSourceHint %q in audit events, got hints: %v", wantHint, hintsSeen)
		}
	}

	// 6. POST /api/audit/reset clears audit events.
	resetReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/audit/reset", nil)
	if err != nil {
		t.Fatalf("NewRequest reset: %v", err)
	}
	resetResp, err := http.DefaultClient.Do(resetReq)
	if err != nil {
		t.Fatalf("POST /api/audit/reset: %v", err)
	}
	resetResp.Body.Close()
	if resetResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/audit/reset status = %d, want 200", resetResp.StatusCode)
	}

	afterResetResp, err := http.Get(ts.URL + "/api/audit")
	if err != nil {
		t.Fatalf("GET /api/audit after reset: %v", err)
	}
	defer afterResetResp.Body.Close()
	var afterReset gitoci.AuditResponse
	if err := json.NewDecoder(afterResetResp.Body).Decode(&afterReset); err != nil {
		t.Fatalf("decode afterReset: %v", err)
	}
	if afterReset.Summary.Total != 0 || len(afterReset.Events) != 0 {
		t.Fatalf("expected empty audit after reset, got %+v", afterReset)
	}
}

func gitRevParseHEAD(t *testing.T, ctx context.Context, repoDir string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = repoDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestOCIRegistryV2PushPullAndAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token := randomTestToken(t)
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: token,
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Unauthenticated GET /v2/ and POST /v2/fulfillment-api/blobs/uploads/ return 401.
	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/v2/"},
		{method: http.MethodPost, path: "/v2/fulfillment-api/blobs/uploads/"},
	} {
		req, err := http.NewRequestWithContext(ctx, tc.method, ts.URL+tc.path, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do(%s %s): %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d, want 401", tc.method, tc.path, resp.StatusCode)
		}
		if got := resp.Header.Get("Docker-Distribution-Api-Version"); got != "registry/2.0" {
			t.Fatalf("Docker-Distribution-Api-Version = %q, want registry/2.0", got)
		}
	}

	// 2. Authenticated GET /v2/ returns 200 OK with Docker-Distribution-Api-Version: registry/2.0.
	v2Req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v2/", nil)
	if err != nil {
		t.Fatalf("NewRequest /v2/: %v", err)
	}
	v2Req.Header.Set("Authorization", "Bearer "+token)
	v2Resp, err := http.DefaultClient.Do(v2Req)
	if err != nil {
		t.Fatalf("GET /v2/: %v", err)
	}
	v2Resp.Body.Close()
	if v2Resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v2/ status = %d, want 200", v2Resp.StatusCode)
	}
	if got := v2Resp.Header.Get("Docker-Distribution-Api-Version"); got != "registry/2.0" {
		t.Fatalf("Docker-Distribution-Api-Version = %q, want registry/2.0", got)
	}

	// 3. Digest mismatch on blob upload is rejected with 400 Bad Request.
	startUploadReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v2/fulfillment-api/blobs/uploads/", nil)
	if err != nil {
		t.Fatalf("NewRequest start upload: %v", err)
	}
	startUploadReq.Header.Set("Authorization", "Bearer "+token)
	startUploadResp, err := http.DefaultClient.Do(startUploadReq)
	if err != nil {
		t.Fatalf("POST upload: %v", err)
	}
	startUploadResp.Body.Close()
	if startUploadResp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST upload status = %d, want 202", startUploadResp.StatusCode)
	}
	uploadLoc := startUploadResp.Header.Get("Location")
	if uploadLoc == "" {
		t.Fatal("missing Location header on 202 upload session")
	}

	badDigest := "sha256:" + strings.Repeat("0", 64)
	badPutURL := ts.URL + uploadLoc + "?digest=" + badDigest
	badPutReq, err := http.NewRequestWithContext(ctx, http.MethodPut, badPutURL, strings.NewReader("tampered-payload"))
	if err != nil {
		t.Fatalf("NewRequest bad PUT: %v", err)
	}
	badPutReq.Header.Set("Authorization", "Bearer "+token)
	badPutResp, err := http.DefaultClient.Do(badPutReq)
	if err != nil {
		t.Fatalf("PUT bad digest: %v", err)
	}
	badPutResp.Body.Close()
	if badPutResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT bad digest status = %d, want 400", badPutResp.StatusCode)
	}

	// 4. Package and push a real binary OCI v1 image (POST->PATCH->PUT chunked upload + manifest PUT),
	// then verify HEAD/GET manifest by tag and digest, HEAD/GET blobs, /tags/list, and GET /api/audit.
	fakeBinaryPath := filepath.Join(t.TempDir(), "fulfillment-api")
	fakeBinaryBytes := []byte("#!/bin/sh\necho fulfillment-api\n")
	if err := os.WriteFile(fakeBinaryPath, fakeBinaryBytes, 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	pushed, err := gitoci.PushBinaryOCIImage(ctx, gitoci.PushBinaryRequest{
		RegistryBaseURL: ts.URL,
		Repository:      "fulfillment-api",
		Tag:             "v0.1.0-actor",
		BinaryPath:      fakeBinaryPath,
		BearerToken:     token,
		CredentialMode:  "ate-egress",
		Annotations: map[string]string{
			"org.opencontainers.image.revision": srv.MainCommitSHA(),
			"dev.ate.tekton.secret-posture":     "ZERO_AMBIENT_SECRETS",
		},
	})
	if err != nil {
		t.Fatalf("PushBinaryOCIImage: %v", err)
	}
	if !strings.HasPrefix(pushed.ManifestDigest, "sha256:") || len(pushed.ManifestDigest) != 71 {
		t.Fatalf("unexpected ManifestDigest = %q", pushed.ManifestDigest)
	}

	// Verify HEAD & GET /v2/fulfillment-api/manifests/v0.1.0-actor and by @sha256 digest.
	for _, ref := range []string{"v0.1.0-actor", pushed.ManifestDigest} {
		headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, ts.URL+"/v2/fulfillment-api/manifests/"+ref, nil)
		if err != nil {
			t.Fatalf("NewRequest HEAD manifest: %v", err)
		}
		headReq.Header.Set("Authorization", "Bearer "+token)
		headResp, err := http.DefaultClient.Do(headReq)
		if err != nil {
			t.Fatalf("HEAD manifest %s: %v", ref, err)
		}
		headResp.Body.Close()
		if headResp.StatusCode != http.StatusOK {
			t.Fatalf("HEAD manifest %s status = %d, want 200", ref, headResp.StatusCode)
		}
		if got := headResp.Header.Get("Docker-Content-Digest"); got != pushed.ManifestDigest {
			t.Fatalf("HEAD Docker-Content-Digest = %q, want %q", got, pushed.ManifestDigest)
		}

		getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v2/fulfillment-api/manifests/"+ref, nil)
		if err != nil {
			t.Fatalf("NewRequest GET manifest: %v", err)
		}
		getReq.Header.Set("Authorization", "Bearer "+token)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatalf("GET manifest %s: %v", ref, err)
		}
		manifestBytes, err := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		if getResp.StatusCode != http.StatusOK {
			t.Fatalf("GET manifest %s status = %d, want 200", ref, getResp.StatusCode)
		}
		if !strings.Contains(string(manifestBytes), "ZERO_AMBIENT_SECRETS") {
			t.Fatalf("manifest missing expected annotation: %s", string(manifestBytes))
		}
	}

	// Verify HEAD & GET on layer blob.
	blobGetReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v2/fulfillment-api/blobs/"+pushed.LayerDigest, nil)
	if err != nil {
		t.Fatalf("NewRequest GET blob: %v", err)
	}
	blobGetReq.Header.Set("Authorization", "Bearer "+token)
	blobGetResp, err := http.DefaultClient.Do(blobGetReq)
	if err != nil {
		t.Fatalf("GET blob: %v", err)
	}
	layerBytes, err := io.ReadAll(blobGetResp.Body)
	blobGetResp.Body.Close()
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if blobGetResp.StatusCode != http.StatusOK || len(layerBytes) == 0 {
		t.Fatalf("GET blob status=%d len=%d", blobGetResp.StatusCode, len(layerBytes))
	}

	// Verify GET /v2/fulfillment-api/tags/list.
	tagsReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v2/fulfillment-api/tags/list", nil)
	if err != nil {
		t.Fatalf("NewRequest tags/list: %v", err)
	}
	tagsReq.Header.Set("Authorization", "Bearer "+token)
	tagsResp, err := http.DefaultClient.Do(tagsReq)
	if err != nil {
		t.Fatalf("GET tags/list: %v", err)
	}
	defer tagsResp.Body.Close()
	if tagsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET tags/list status = %d, want 200", tagsResp.StatusCode)
	}
	var tagList struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	if err := json.NewDecoder(tagsResp.Body).Decode(&tagList); err != nil {
		t.Fatalf("decode tags/list: %v", err)
	}
	if tagList.Name != "fulfillment-api" || len(tagList.Tags) != 1 || tagList.Tags[0] != "v0.1.0-actor" {
		t.Fatalf("unexpected tags/list: %+v", tagList)
	}

	// Verify GET /api/audit recorded OCIManifestPushes == 1 with Tag and Digest.
	auditResp, err := http.Get(ts.URL + "/api/audit")
	if err != nil {
		t.Fatalf("GET /api/audit: %v", err)
	}
	defer auditResp.Body.Close()
	var audit gitoci.AuditResponse
	if err := json.NewDecoder(auditResp.Body).Decode(&audit); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if audit.Summary.OCIManifestPushes != 1 {
		t.Fatalf("Summary.OCIManifestPushes = %d, want 1", audit.Summary.OCIManifestPushes)
	}
	foundManifestAudit := false
	for _, ev := range audit.Events {
		if ev.Protocol == gitoci.ProtocolOCI && ev.Tag == "v0.1.0-actor" && ev.Digest == pushed.ManifestDigest && ev.TokenSourceHint == "ate-egress" {
			foundManifestAudit = true
			break
		}
	}
	if !foundManifestAudit {
		t.Fatalf("did not find expected OCI manifest push AuditEvent in %+v", audit.Events)
	}
}

func TestSharedGitOCIMultiCIAndCodePushAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	token := "substrate-multici-demo-token-2026"
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: token,
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. GET /api/repo/status returns initial v1 clean baseline.
	statusResp, err := http.Get(ts.URL + "/api/repo/status")
	if err != nil {
		t.Fatalf("GET /api/repo/status: %v", err)
	}
	var st gitoci.RepoStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&st); err != nil {
		statusResp.Body.Close()
		t.Fatalf("decode status: %v", err)
	}
	statusResp.Body.Close()
	if st.Repo != "fulfillment-api" || st.Release != "v1" || st.Scenario != "clean" || st.SHA == "" {
		t.Fatalf("unexpected initial RepoStatus: %+v", st)
	}

	// 2. Clone via /git/tenant-01.git using HTTP Basic Auth (as go-git / Flux uses).
	basicURL := strings.Replace(ts.URL, "http://", "http://oauth2:"+token+"@", 1) + "/git/tenant-01.git"
	tenantCloneDir := filepath.Join(t.TempDir(), "tenant-01-clone")
	cloneCmd := exec.CommandContext(ctx, "git", "clone", basicURL, tenantCloneDir)
	cloneCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cloneCmd.CombinedOutput(); err != nil {
		t.Fatalf("git clone /git/tenant-01.git with Basic auth failed: %v\n%s", err, out)
	}

	for _, relPath := range []string{
		"go.mod",
		"main.go",
		"pricing.go",
		"pricing_test.go",
		"kustomization.yaml",
		"configmap.yaml",
		"service.yaml",
		"deployment.yaml",
		"deploy/overlays/tenant-01/kustomization.yaml",
		"deploy/overlays/tenant-24/kustomization.yaml",
	} {
		if _, err := os.Stat(filepath.Join(tenantCloneDir, filepath.FromSlash(relPath))); err != nil {
			t.Fatalf("expected seeded file %s missing: %v", relPath, err)
		}
	}

	// 3. POST /api/repo/push for release v2, buggy-code, oom-overlay, and reset.
	pushScenario := func(req gitoci.RepoPushRequest) gitoci.RepoStatus {
		t.Helper()
		raw, _ := json.Marshal(req)
		resp, err := http.Post(ts.URL+"/api/repo/push", "application/json", strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("POST /api/repo/push (%+v): %v", req, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST /api/repo/push (%+v) status = %d: %s", req, resp.StatusCode, string(body))
		}
		var out gitoci.RepoStatus
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode push status: %v", err)
		}
		return out
	}

	stV2 := pushScenario(gitoci.RepoPushRequest{Scenario: "release", Release: "v2"})
	if stV2.Release != "v2" || stV2.Scenario != "clean" || stV2.SHA == st.SHA {
		t.Fatalf("expected v2 release commit, got %+v", stV2)
	}

	stBuggy := pushScenario(gitoci.RepoPushRequest{Scenario: "buggy-code", Release: "v3-buggy"})
	if stBuggy.Scenario != "buggy-code" || stBuggy.SHA == stV2.SHA {
		t.Fatalf("expected buggy-code commit, got %+v", stBuggy)
	}

	stOOM := pushScenario(gitoci.RepoPushRequest{Scenario: "oom-overlay", Release: "v4-oom", Tenant: "tenant-01"})
	if stOOM.Scenario != "oom-overlay" || stOOM.Tenant != "tenant-01" || stOOM.SHA == stBuggy.SHA {
		t.Fatalf("expected oom-overlay commit, got %+v", stOOM)
	}

	stReset := pushScenario(gitoci.RepoPushRequest{Scenario: "reset"})
	if stReset.Scenario != "clean" || stReset.Release != "v1" || stReset.SHA == stOOM.SHA {
		t.Fatalf("expected reset commit, got %+v", stReset)
	}

	// 4. Verify git pull and real git push (Smart-HTTP git-receive-pack) work from the cloned repo.
	runGitInClone := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = tenantCloneDir
		cmd.Env = append(os.Environ(),
			"GIT_TERMINAL_PROMPT=0",
			"GIT_AUTHOR_NAME=Dev User",
			"GIT_AUTHOR_EMAIL=dev@example.com",
			"GIT_COMMITTER_NAME=Dev User",
			"GIT_COMMITTER_EMAIL=dev@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
	}

	runGitInClone("pull", "--ff-only", "origin", "main")
	if err := os.WriteFile(filepath.Join(tenantCloneDir, "README.md"), []byte("# fulfillment-api\n"), 0o644); err != nil {
		t.Fatalf("write README.md: %v", err)
	}
	runGitInClone("add", "README.md")
	runGitInClone("commit", "-m", "Direct developer git push over Smart-HTTP")
	runGitInClone("push", "origin", "HEAD:main")
	localHead := gitRevParseHEAD(t, ctx, tenantCloneDir)

	afterPushStatus := srv.RepoStatus()
	if afterPushStatus.SHA != localHead || !strings.Contains(afterPushStatus.Message, "Direct developer git push") {
		t.Fatalf("RepoStatus after git push = %+v, want SHA=%s", afterPushStatus, localHead)
	}
}
