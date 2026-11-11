package gitoci_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	"github.com/gke-demos/tekton-actors/pkg/supervisor"
)

func TestSampleAppCleanAndBuggyLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Clean ("main") sample-app builds and passes offline unit tests.
	cleanDir := filepath.Join(t.TempDir(), "clean-app")
	if err := gitoci.WriteSampleApp(cleanDir, false); err != nil {
		t.Fatalf("WriteSampleApp(clean): %v", err)
	}

	if out, err := runOfflineGo(ctx, cleanDir, "test", "-v", "./..."); err != nil {
		t.Fatalf("clean sample-app failed go test: %v\n%s", err, out)
	}

	binPath := filepath.Join(t.TempDir(), "fulfillment-api")
	if out, err := runOfflineGo(ctx, cleanDir, "build", "-o", binPath, "."); err != nil {
		t.Fatalf("clean sample-app failed go build: %v\n%s", err, out)
	}
	if _, err := os.Stat(binPath); err != nil {
		t.Fatalf("compiled binary missing: %v", err)
	}

	// 2. Buggy ("buggy") sample-app compiles cleanly (so clone-and-build succeeds),
	// passes smoke quote for zero-discount tier, and fails TestCalculateTotal in unit-test.
	workspaceDir := t.TempDir()
	buggyAppDir := filepath.Join(workspaceDir, "source", "app")
	if err := gitoci.WriteSampleApp(buggyAppDir, true); err != nil {
		t.Fatalf("WriteSampleApp(buggy): %v", err)
	}

	// Verify git apply --check also accepts SampleAppRepoFixDiff cleanly.
	gitApplyCheck := exec.CommandContext(ctx, "git", "apply", "--check")
	gitApplyCheck.Dir = buggyAppDir
	gitApplyCheck.Stdin = strings.NewReader(gitoci.SampleAppRepoFixDiff)
	if out, err := gitApplyCheck.CombinedOutput(); err != nil {
		t.Fatalf("git apply --check rejected SampleAppRepoFixDiff: %v\n%s", err, out)
	}

	buggyBin := filepath.Join(workspaceDir, "source", "bin", "fulfillment-api")
	if out, err := runOfflineGo(ctx, buggyAppDir, "build", "-o", buggyBin, "."); err != nil {
		t.Fatalf("buggy sample-app should still compile during clone-and-build: %v\n%s", err, out)
	}

	out, err := runOfflineGo(ctx, buggyAppDir, "test", "-v", "./...")
	if err == nil {
		t.Fatalf("expected buggy sample-app to fail go test, but succeeded:\n%s", out)
	}
	if !strings.Contains(out, "TestCalculateTotal") {
		t.Fatalf("expected TestCalculateTotal failure in output, got:\n%s", out)
	}

	// 3. Applying SampleAppWorkspaceFixDiff through Step Supervisor's /v1/rescue/patch
	// repairs source/app/pricing.go in-place and makes unit-test pass immediately.
	sup, err := supervisor.NewServer(supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: workspaceDir,
		TektonDir:    t.TempDir(),
		ResultsDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("supervisor.NewServer: %v", err)
	}
	ts := httptest.NewServer(sup.Handler())
	defer ts.Close()

	patchBody, err := json.Marshal(supervisor.RescuePatchRequest{
		Diff: gitoci.SampleAppWorkspaceFixDiff,
	})
	if err != nil {
		t.Fatalf("marshal RescuePatchRequest: %v", err)
	}
	resp, err := http.Post(ts.URL+"/v1/rescue/patch", "application/json", bytes.NewReader(patchBody))
	if err != nil {
		t.Fatalf("POST /v1/rescue/patch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/rescue/patch status = %d, want 200", resp.StatusCode)
	}
	var patchResp supervisor.RescuePatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&patchResp); err != nil {
		t.Fatalf("decode RescuePatchResponse: %v", err)
	}
	if len(patchResp.PatchedFiles) != 1 || patchResp.PatchedFiles[0] != gitoci.SampleAppWorkspaceRelPath {
		t.Fatalf("PatchedFiles = %v, want [%s]", patchResp.PatchedFiles, gitoci.SampleAppWorkspaceRelPath)
	}

	patchedBytes, err := os.ReadFile(filepath.Join(workspaceDir, filepath.FromSlash(gitoci.SampleAppWorkspaceRelPath)))
	if err != nil {
		t.Fatalf("read patched pricing.go: %v", err)
	}
	if string(patchedBytes) != gitoci.SampleAppCleanPricingGo {
		t.Fatalf("patched pricing.go does not match SampleAppCleanPricingGo")
	}

	if out, err := runOfflineGo(ctx, buggyAppDir, "test", "-v", "./..."); err != nil {
		t.Fatalf("patched sample-app failed go test: %v\n%s", err, out)
	}
}

func runOfflineGo(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GOPROXY=off",
		"GOSUMDB=off",
		"GOTOOLCHAIN=local",
		"CGO_ENABLED=0",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
