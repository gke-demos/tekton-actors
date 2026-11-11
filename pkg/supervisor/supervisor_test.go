package supervisor_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/supervisor"
)

type testInstance struct {
	addr   string
	url    string
	client *supervisor.Client
}

func startTestSupervisor(t *testing.T, cfg supervisor.Config) testInstance {
	t.Helper()

	srv, err := supervisor.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer(%+v): %v", cfg, err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(ln)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
		<-errCh
	})

	addr := ln.Addr().String()
	baseURL := "http://" + addr
	return testInstance{
		addr:   addr,
		url:    baseURL,
		client: supervisor.NewClient(baseURL, nil),
	}
}

func TestReadyz_ReportsReadyOnCoordinatorAndWorker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: filepath.Join(root, "workspace"),
		ResultsDir:   filepath.Join(root, "tekton", "results"),
	})
	worker := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleWorker,
		WorkspaceDir: filepath.Join(root, "workspace"),
		ResultsDir:   filepath.Join(root, "tekton", "results"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		inst testInstance
	}{
		{name: "coordinator", inst: coord},
		{name: "worker", inst: worker},
	} {
		if err := tc.inst.client.Ready(ctx); err != nil {
			t.Fatalf("%s Ready() unexpected error: %v", tc.name, err)
		}

		resp, err := http.Get(tc.inst.url + "/readyz")
		if err != nil {
			t.Fatalf("%s GET /readyz: %v", tc.name, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s GET /readyz status = %d, want 200", tc.name, resp.StatusCode)
		}
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("%s X-Content-Type-Options = %q, want %q", tc.name, got, "nosniff")
		}
		var body struct {
			OK bool `json:"ok"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("%s decode /readyz JSON: %v", tc.name, err)
		}
		if !body.OK {
			t.Fatalf("%s /readyz ok = false, want true", tc.name)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, tc.inst.url+"/readyz", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		badResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE /readyz: %v", err)
		}
		badResp.Body.Close()
		if badResp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s DELETE /readyz status = %d, want 405", tc.name, badResp.StatusCode)
		}
	}
}

func TestRunStep_ExecutesScriptAndExpandsKubernetesEnv(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workspaceDir := filepath.Join(root, "workspace")
	tektonDir := filepath.Join(root, "tekton")
	resultsDir := filepath.Join(tektonDir, "results")

	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: workspaceDir,
		ResultsDir:   resultsDir,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name: "prepare-env",
		Env: []supervisor.EnvEntry{
			{Name: "BASE_WORD", Value: "hello"},
			{Name: "COMPOSED", Value: "$(BASE_WORD)-substrate"},
			{Name: "CACHE_DIR", Value: "$(HOME)/.cache"},
			{Name: "ESCAPED", Value: "$$(BASE_WORD)"},
			{Name: "UNSET_REF", Value: "$(NO_SUCH_VAR_12345)"},
		},
		Script: `mkdir -p sub
cat > sub/env-report.txt <<EOF
HOME=${HOME}
COMPOSED=${COMPOSED}
CACHE_DIR=${CACHE_DIR}
ESCAPED=${ESCAPED}
UNSET_REF=${UNSET_REF}
EOF
`,
	})
	if err != nil {
		t.Fatalf("RunStep(prepare-env): %v", err)
	}
	if started.StepID == "" {
		t.Fatalf("expected generated stepId, got empty")
	}
	if started.State != supervisor.StepStateRunning {
		t.Fatalf("initial state = %q, want %q", started.State, supervisor.StepStateRunning)
	}
	if started.StartedAt.IsZero() {
		t.Fatalf("expected non-zero startedAt")
	}

	done, err := coord.client.WaitStep(ctx, started.StepID)
	if err != nil {
		t.Fatalf("WaitStep(%s): %v", started.StepID, err)
	}
	if done.State != supervisor.StepStateCompleted {
		t.Fatalf("final state = %q, want %q", done.State, supervisor.StepStateCompleted)
	}
	if done.ExitCode == nil || *done.ExitCode != 0 {
		t.Fatalf("exitCode = %v, want 0", done.ExitCode)
	}
	if done.FinishedAt == nil || done.FinishedAt.Before(done.StartedAt) {
		t.Fatalf("invalid finishedAt = %v (startedAt = %v)", done.FinishedAt, done.StartedAt)
	}

	for _, dir := range []string{
		resultsDir,
		filepath.Join(tektonDir, "home"),
		filepath.Join(tektonDir, "steps"),
	} {
		info, statErr := os.Stat(dir)
		if statErr != nil || !info.IsDir() {
			t.Fatalf("expected directory %s to exist, statErr=%v", dir, statErr)
		}
	}

	reportBytes, err := os.ReadFile(filepath.Join(workspaceDir, "sub", "env-report.txt"))
	if err != nil {
		t.Fatalf("read env-report.txt: %v", err)
	}
	wantReport := strings.Join([]string{
		"HOME=" + filepath.Join(tektonDir, "home"),
		"COMPOSED=hello-substrate",
		"CACHE_DIR=" + filepath.Join(tektonDir, "home", ".cache"),
		"ESCAPED=$(BASE_WORD)",
		"UNSET_REF=$(NO_SUCH_VAR_12345)",
	}, "\n") + "\n"
	if string(reportBytes) != wantReport {
		t.Fatalf("env-report.txt mismatch:\ngot:\n%s\nwant:\n%s", string(reportBytes), wantReport)
	}

	// Verify explicit shebang script + workingDir + positional args + direct command execution.
	shebangStarted, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		StepID:     "step-shebang-2",
		Name:       "shebang-step",
		WorkingDir: filepath.Join(workspaceDir, "sub"),
		Args:       []string{"alpha", "beta"},
		Script:     "#!/bin/sh\nprintf '%s:%s:%s' \"$(basename \"$(pwd)\")\" \"$1\" \"$2\" > shebang-out.txt\n",
	})
	if err != nil {
		t.Fatalf("RunStep(shebang-step): %v", err)
	}
	if shebangStarted.StepID != "step-shebang-2" {
		t.Fatalf("stepId = %q, want step-shebang-2", shebangStarted.StepID)
	}
	shebangDone, err := coord.client.WaitStep(ctx, shebangStarted.StepID)
	if err != nil {
		t.Fatalf("WaitStep(shebang): %v", err)
	}
	if shebangDone.State != supervisor.StepStateCompleted {
		t.Fatalf("shebang state = %q, want COMPLETED", shebangDone.State)
	}
	gotShebang, err := os.ReadFile(filepath.Join(workspaceDir, "sub", "shebang-out.txt"))
	if err != nil {
		t.Fatalf("read shebang-out.txt: %v", err)
	}
	if string(gotShebang) != "sub:alpha:beta" {
		t.Fatalf("shebang-out.txt = %q, want %q", string(gotShebang), "sub:alpha:beta")
	}

	cmdStarted, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:    "command-step",
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{"printf 'cmd-$(TARGET)' > $(OUT_FILE)"},
		Env: []supervisor.EnvEntry{
			{Name: "TARGET", Value: "ok"},
			{Name: "OUT_FILE", Value: filepath.Join(workspaceDir, "cmd-out.txt")},
		},
	})
	if err != nil {
		t.Fatalf("RunStep(command-step): %v", err)
	}
	cmdDone, err := coord.client.WaitStep(ctx, cmdStarted.StepID)
	if err != nil {
		t.Fatalf("WaitStep(command-step): %v", err)
	}
	if cmdDone.State != supervisor.StepStateCompleted {
		t.Fatalf("command-step state = %q, want COMPLETED", cmdDone.State)
	}
	gotCmd, err := os.ReadFile(filepath.Join(workspaceDir, "cmd-out.txt"))
	if err != nil {
		t.Fatalf("read cmd-out.txt: %v", err)
	}
	if string(gotCmd) != "cmd-ok" {
		t.Fatalf("cmd-out.txt = %q, want %q", string(gotCmd), "cmd-ok")
	}
}

func TestStepLogs_ByteOffsetFollowAndSSE(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workspaceDir := filepath.Join(root, "workspace")
	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: workspaceDir,
		ResultsDir:   filepath.Join(root, "tekton", "results"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name: "gated-stream-step",
		Script: `printf 'phase-1-ready\n'
while [ ! -f release.gate ]; do
  sleep 0.01
done
printf 'phase-2-stderr\n' >&2
printf 'phase-3-done\n'
`,
	})
	if err != nil {
		t.Fatalf("RunStep: %v", err)
	}

	// Poll follow=false until phase-1-ready appears while step is still RUNNING.
	var firstOffset int64
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, getErr := http.Get(coord.url + "/v1/steps/" + started.StepID + "/logs?offset=0&follow=false")
		if getErr != nil {
			t.Fatalf("GET logs follow=false: %v", getErr)
		}
		var chunk bytes.Buffer
		_, _ = chunk.ReadFrom(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET logs status = %d, want 200", resp.StatusCode)
		}
		if chunk.String() == "phase-1-ready\n" {
			offStr := resp.Header.Get("X-Log-Offset")
			parsedOff, parseErr := strconv.ParseInt(offStr, 10, 64)
			if parseErr != nil {
				t.Fatalf("invalid X-Log-Offset %q: %v", offStr, parseErr)
			}
			firstOffset = parsedOff
			if gotState := resp.Header.Get("X-Step-State"); gotState != string(supervisor.StepStateRunning) {
				t.Fatalf("X-Step-State while gated = %q, want RUNNING", gotState)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for phase-1-ready log; last=%q", chunk.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if firstOffset != int64(len("phase-1-ready\n")) {
		t.Fatalf("firstOffset = %d, want %d", firstOffset, len("phase-1-ready\n"))
	}

	// Attach both a chunked StreamLogs follower (starting at firstOffset) and an SSE follower (starting at 0).
	type streamResult struct {
		nextOffset int64
		output     string
		err        error
	}
	chunkStreamCh := make(chan streamResult, 1)
	go func() {
		var buf bytes.Buffer
		next, streamErr := coord.client.StreamLogs(ctx, started.StepID, firstOffset, true, &buf)
		chunkStreamCh <- streamResult{nextOffset: next, output: buf.String(), err: streamErr}
	}()

	type sseResult struct {
		combined   string
		doneOffset int64
		doneState  supervisor.StepState
		err        error
	}
	sseStreamCh := make(chan sseResult, 1)
	go func() {
		var combined strings.Builder
		var finalOff int64
		var finalState supervisor.StepState
		sseErr := coord.client.StreamLogsSSE(ctx, started.StepID, 0, func(ev supervisor.LogSSEEvent) error {
			if ev.Event == "done" {
				finalOff = ev.Offset
				finalState = ev.State
			} else {
				combined.WriteString(ev.Chunk)
			}
			return nil
		})
		sseStreamCh <- sseResult{
			combined:   combined.String(),
			doneOffset: finalOff,
			doneState:  finalState,
			err:        sseErr,
		}
	}()

	// Release the gate so phase 2 & 3 emit and the step completes.
	if err := os.WriteFile(filepath.Join(workspaceDir, "release.gate"), []byte("go"), 0o644); err != nil {
		t.Fatalf("write release.gate: %v", err)
	}

	wantTail := "phase-2-stderr\nphase-3-done\n"
	wantFull := "phase-1-ready\n" + wantTail

	chunkRes := <-chunkStreamCh
	if chunkRes.err != nil {
		t.Fatalf("StreamLogs(follow=true) error: %v", chunkRes.err)
	}
	if chunkRes.output != wantTail {
		t.Fatalf("StreamLogs tail = %q, want %q", chunkRes.output, wantTail)
	}
	if chunkRes.nextOffset != int64(len(wantFull)) {
		t.Fatalf("StreamLogs nextOffset = %d, want %d", chunkRes.nextOffset, len(wantFull))
	}

	sseRes := <-sseStreamCh
	if sseRes.err != nil {
		t.Fatalf("StreamLogsSSE error: %v", sseRes.err)
	}
	if sseRes.combined != wantFull {
		t.Fatalf("StreamLogsSSE combined = %q, want %q", sseRes.combined, wantFull)
	}
	if sseRes.doneOffset != int64(len(wantFull)) {
		t.Fatalf("StreamLogsSSE doneOffset = %d, want %d", sseRes.doneOffset, len(wantFull))
	}
	if sseRes.doneState != supervisor.StepStateCompleted {
		t.Fatalf("StreamLogsSSE doneState = %q, want COMPLETED", sseRes.doneState)
	}

	// Verify follow=false from firstOffset after completion returns exact tail and COMPLETED header.
	var snapshotTail bytes.Buffer
	nextOff, err := coord.client.StreamLogs(ctx, started.StepID, firstOffset, false, &snapshotTail)
	if err != nil {
		t.Fatalf("StreamLogs(follow=false) after completion: %v", err)
	}
	if snapshotTail.String() != wantTail || nextOff != int64(len(wantFull)) {
		t.Fatalf("post-complete follow=false got (%q, %d), want (%q, %d)", snapshotTail.String(), nextOff, wantTail, len(wantFull))
	}
}

func TestStepResults_CapturesPayloadsExceeding4KiB(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	resultsDir := filepath.Join(root, "tekton", "results")
	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: filepath.Join(root, "workspace"),
		ResultsDir:   resultsDir,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build an independent 64 KiB expected string: 1024 lines of 64 bytes each.
	const line64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n"
	wantLarge := strings.Repeat(line64, 1024) // 65,536 bytes (> 4 KiB kubelet limit)

	started, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name: "emit-large-results",
		Env: []supervisor.EnvEntry{
			{Name: "RESULTS_DIR", Value: resultsDir},
		},
		Script: `i=0
while [ "$i" -lt 1024 ]; do
  printf '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n'
  i=$((i + 1))
done > "${RESULTS_DIR}/large-sbom"
printf 'sha256:0123456789abcdef' > "${RESULTS_DIR}/image-digest"
`,
	})
	if err != nil {
		t.Fatalf("RunStep(emit-large-results): %v", err)
	}

	done, err := coord.client.WaitStep(ctx, started.StepID)
	if err != nil {
		t.Fatalf("WaitStep: %v", err)
	}
	if done.State != supervisor.StepStateCompleted {
		t.Fatalf("state = %q, want COMPLETED", done.State)
	}
	if got := done.Results["large-sbom"]; len(got) != len(wantLarge) || got != wantLarge {
		t.Fatalf("large-sbom len = %d, want %d (exact match=%v)", len(got), len(wantLarge), got == wantLarge)
	}
	if got := done.Results["image-digest"]; got != "sha256:0123456789abcdef" {
		t.Fatalf("image-digest = %q, want %q", got, "sha256:0123456789abcdef")
	}

	// Verify explicit ResultNames scoping and optional unwritten result tolerance.
	filtered, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:        "emit-filtered-results",
		ResultNames: []string{"selected-only", "unwritten-optional"},
		Env: []supervisor.EnvEntry{
			{Name: "RESULTS_DIR", Value: resultsDir},
		},
		Script: `printf 'picked-value' > "${RESULTS_DIR}/selected-only"`,
	})
	if err != nil {
		t.Fatalf("RunStep(emit-filtered-results): %v", err)
	}
	filteredDone, err := coord.client.WaitStep(ctx, filtered.StepID)
	if err != nil {
		t.Fatalf("WaitStep(filtered): %v", err)
	}
	if len(filteredDone.Results) != 1 || filteredDone.Results["selected-only"] != "picked-value" {
		t.Fatalf("filtered Results = %#v, want only selected-only=picked-value", filteredDone.Results)
	}

	// Verify traversal in ResultNames is rejected.
	if _, badErr := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:        "bad-result-name",
		ResultNames: []string{"../outside"},
		Script:      "true",
	}); badErr == nil {
		t.Fatalf("expected error for ../ traversal in resultNames, got nil")
	}
}

func TestMultiContainer_CoordinatorProxiesToSiblingWorkerAndSurvivesFailures(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workspaceDir := filepath.Join(root, "workspace")
	resultsDir := filepath.Join(root, "tekton", "results")

	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: workspaceDir,
		ResultsDir:   resultsDir,
	})
	worker := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleWorker,
		WorkspaceDir: workspaceDir,
		ResultsDir:   resultsDir,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Step 1: local on coordinator (c0), writes artifact into shared /workspace.
	step1, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		StepID: "c0-build",
		Name:   "build",
		Script: "printf 'built-artifact-v1' > artifact.txt\n",
	})
	if err != nil {
		t.Fatalf("RunStep(c0-build): %v", err)
	}
	step1Done, err := coord.client.WaitStep(ctx, step1.StepID)
	if err != nil {
		t.Fatalf("WaitStep(c0-build): %v", err)
	}
	if step1Done.State != supervisor.StepStateCompleted {
		t.Fatalf("c0-build state = %q, want COMPLETED", step1Done.State)
	}
	// Worker must NOT have local coordinator step c0-build.
	if _, workerErr := worker.client.GetStep(ctx, "c0-build"); workerErr == nil {
		t.Fatalf("expected worker GetStep(c0-build) to fail with 404, got nil")
	}

	// Step 2: submitted to coordinator with TargetAddr pointing to sibling worker (c1).
	wantWorkerResult := strings.Repeat("worker-scan-line-0123456789abcdef\n", 256) // > 8 KiB
	step2, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		StepID:      "c1-scan",
		Name:        "vuln-scan",
		TargetAddr:  worker.addr,
		ResultNames: []string{"scan-report"},
		Env: []supervisor.EnvEntry{
			{Name: "RESULTS_DIR", Value: resultsDir},
		},
		Script: `printf 'worker reading: %s\n' "$(cat artifact.txt)"
i=0
while [ "$i" -lt 256 ]; do
  printf 'worker-scan-line-0123456789abcdef\n'
  i=$((i + 1))
done > "${RESULTS_DIR}/scan-report"
`,
	})
	if err != nil {
		t.Fatalf("RunStep(c1-scan): %v", err)
	}

	var proxiedLogs bytes.Buffer
	if _, err := coord.client.StreamLogs(ctx, step2.StepID, 0, true, &proxiedLogs); err != nil {
		t.Fatalf("coord StreamLogs(c1-scan): %v", err)
	}
	if proxiedLogs.String() != "worker reading: built-artifact-v1\n" {
		t.Fatalf("proxied worker logs = %q, want %q", proxiedLogs.String(), "worker reading: built-artifact-v1\n")
	}

	step2Done, err := coord.client.WaitStep(ctx, step2.StepID)
	if err != nil {
		t.Fatalf("coord WaitStep(c1-scan): %v", err)
	}
	if step2Done.State != supervisor.StepStateCompleted || step2Done.ExitCode == nil || *step2Done.ExitCode != 0 {
		t.Fatalf("c1-scan status = %+v, want COMPLETED exit 0", step2Done)
	}
	if step2Done.Results["scan-report"] != wantWorkerResult {
		t.Fatalf("c1-scan result len = %d, want %d", len(step2Done.Results["scan-report"]), len(wantWorkerResult))
	}
	// Confirm c1-scan genuinely ran on the sibling worker instance.
	workerDirectStatus, err := worker.client.GetStep(ctx, "c1-scan")
	if err != nil {
		t.Fatalf("worker direct GetStep(c1-scan) failed (step was not forwarded to worker!): %v", err)
	}
	if workerDirectStatus.State != supervisor.StepStateCompleted {
		t.Fatalf("worker direct state = %q, want COMPLETED", workerDirectStatus.State)
	}

	// Step 3: failing step (exit 42) forwarded to worker; both worker & coordinator stay alive!
	step3, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		StepID:     "c1-failing-step",
		Name:       "failing-test",
		TargetAddr: worker.addr,
		Script:     "#!/bin/sh\nprintf 'assertion failed: got 5 want 4\\n' >&2\nexit 42\n",
	})
	if err != nil {
		t.Fatalf("RunStep(c1-failing-step): %v", err)
	}
	var failSSE strings.Builder
	var failDoneState supervisor.StepState
	if err := coord.client.StreamLogsSSE(ctx, step3.StepID, 0, func(ev supervisor.LogSSEEvent) error {
		if ev.Event == "done" {
			failDoneState = ev.State
		} else {
			failSSE.WriteString(ev.Chunk)
		}
		return nil
	}); err != nil {
		t.Fatalf("coord StreamLogsSSE(c1-failing-step): %v", err)
	}
	if failDoneState != supervisor.StepStateFailed {
		t.Fatalf("SSE done state = %q, want FAILED", failDoneState)
	}
	if failSSE.String() != "assertion failed: got 5 want 4\n" {
		t.Fatalf("failing SSE logs = %q", failSSE.String())
	}

	step3Done, err := coord.client.WaitStep(ctx, step3.StepID)
	if err != nil {
		t.Fatalf("WaitStep(c1-failing-step): %v", err)
	}
	if step3Done.State != supervisor.StepStateFailed || step3Done.ExitCode == nil || *step3Done.ExitCode != 42 {
		t.Fatalf("c1-failing-step = %+v, want FAILED exitCode=42", step3Done)
	}

	// Both coordinator and worker must remain healthy on /readyz and accept subsequent steps.
	if err := coord.client.Ready(ctx); err != nil {
		t.Fatalf("coordinator Ready after step failure: %v", err)
	}
	if err := worker.client.Ready(ctx); err != nil {
		t.Fatalf("worker Ready after step failure: %v", err)
	}

	// Also verify local /bin/sh -e failure and recovery on coordinator.
	localFail, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:   "local-sh-e-fail",
		Script: "printf 'before-fail\\n'\nfalse\nprintf 'unreachable\\n'\n",
	})
	if err != nil {
		t.Fatalf("RunStep(local-sh-e-fail): %v", err)
	}
	localFailDone, err := coord.client.WaitStep(ctx, localFail.StepID)
	if err != nil {
		t.Fatalf("WaitStep(local-sh-e-fail): %v", err)
	}
	if localFailDone.State != supervisor.StepStateFailed || localFailDone.ExitCode == nil || *localFailDone.ExitCode == 0 {
		t.Fatalf("local-sh-e-fail = %+v, want FAILED non-zero", localFailDone)
	}
	var localFailLogs bytes.Buffer
	if _, err := coord.client.StreamLogs(ctx, localFail.StepID, 0, false, &localFailLogs); err != nil {
		t.Fatalf("StreamLogs(local-sh-e-fail): %v", err)
	}
	if localFailLogs.String() != "before-fail\n" {
		t.Fatalf("local-sh-e-fail logs = %q, want %q", localFailLogs.String(), "before-fail\n")
	}

	afterFail, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:   "after-failure-recovery",
		Script: "printf 'still-alive\n'",
	})
	if err != nil {
		t.Fatalf("RunStep(after-failure-recovery): %v", err)
	}
	afterFailDone, err := coord.client.WaitStep(ctx, afterFail.StepID)
	if err != nil {
		t.Fatalf("WaitStep(after-failure-recovery): %v", err)
	}
	if afterFailDone.State != supervisor.StepStateCompleted {
		t.Fatalf("after-failure-recovery state = %q, want COMPLETED", afterFailDone.State)
	}

	// Reject SSRF non-loopback targetAddr on coordinator and any targetAddr on worker.
	for _, badTarget := range []string{"10.0.0.1:8091", "example.com:8091", "http://127.0.0.1:8091/evil"} {
		if _, badErr := coord.client.RunStep(ctx, supervisor.RunStepRequest{
			Name:       "bad-target",
			TargetAddr: badTarget,
			Script:     "true",
		}); badErr == nil {
			t.Fatalf("expected coordinator to reject targetAddr %q, got nil", badTarget)
		}
	}
	if _, loopErr := worker.client.RunStep(ctx, supervisor.RunStepRequest{
		Name:       "worker-forward-forbidden",
		TargetAddr: worker.addr,
		Script:     "true",
	}); loopErr == nil {
		t.Fatalf("expected worker role to reject non-empty targetAddr, got nil")
	}
}

func TestRescueAndFiles_SeedExecHotPatchAndTraversalDefense(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workspaceDir := filepath.Join(root, "workspace")
	outsideFile := filepath.Join(root, "outside-secret.txt")
	if err := os.WriteFile(outsideFile, []byte("untouched"), 0o600); err != nil {
		t.Fatalf("write outside-secret.txt: %v", err)
	}

	coord := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: workspaceDir,
		ResultsDir:   filepath.Join(root, "tekton", "results"),
	})
	worker := startTestSupervisor(t, supervisor.Config{
		Role:         supervisor.RoleWorker,
		WorkspaceDir: workspaceDir,
		ResultsDir:   filepath.Join(root, "tekton", "results"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Seed workspace files via POST /v1/files.
	brokenCalc := strings.Join([]string{
		"package calc",
		"",
		"// Header comment",
		"func Add(a, b int) int {",
		"\treturn a - b",
		"}",
		"",
		"func Label() string {",
		"\treturn \"broken\"",
		"}",
		"",
	}, "\n")
	upResp, err := coord.client.UploadFiles(ctx, supervisor.UploadFilesRequest{
		Files: map[string][]byte{
			"pkg/calc/calc.go": []byte(brokenCalc),
			"config/app.yaml":  []byte("env: staging\n"),
		},
		Mode: 0o640,
	})
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(upResp.WrittenFiles) != 2 {
		t.Fatalf("WrittenFiles = %v, want 2 files", upResp.WrittenFiles)
	}
	statInfo, err := os.Stat(filepath.Join(workspaceDir, "pkg", "calc", "calc.go"))
	if err != nil {
		t.Fatalf("stat seeded calc.go: %v", err)
	}
	if gotPerm := statInfo.Mode().Perm(); gotPerm != 0o640 {
		t.Fatalf("seeded file perm = %o, want 0640", gotPerm)
	}

	// 2. Inspect workspace via POST /v1/rescue/exec (both zero and non-zero exit codes).
	inspectRes, err := coord.client.RescueExec(ctx, supervisor.RescueExecRequest{
		Command: []string{"/bin/sh", "-c", "cat config/app.yaml"},
	})
	if err != nil {
		t.Fatalf("RescueExec(cat): %v", err)
	}
	if inspectRes.ExitCode != 0 || inspectRes.Output != "env: staging\n" {
		t.Fatalf("RescueExec(cat) = %+v, want exit 0 and 'env: staging\\n'", inspectRes)
	}

	checkFailRes, err := coord.client.RescueExec(ctx, supervisor.RescueExecRequest{
		Script: "grep -q 'return a + b' pkg/calc/calc.go",
	})
	if err != nil {
		t.Fatalf("RescueExec(grep): %v", err)
	}
	if checkFailRes.ExitCode == 0 {
		t.Fatalf("expected non-zero exit code when buggy line is present, got 0")
	}

	// 3. Apply multi-hunk + new-file unified diff via POST /v1/rescue/patch (no external patch/git needed).
	unifiedDiff := strings.Join([]string{
		"diff --git a/pkg/calc/calc.go b/pkg/calc/calc.go",
		"--- a/pkg/calc/calc.go",
		"+++ b/pkg/calc/calc.go",
		"@@ -3,8 +3,8 @@",
		" // Header comment",
		" func Add(a, b int) int {",
		"-\treturn a - b",
		"+\treturn a + b",
		" }",
		" ",
		" func Label() string {",
		"-\treturn \"broken\"",
		"+\treturn \"fixed\"",
		" }",
		"diff --git a/pkg/calc/HOTPATCHED b/pkg/calc/HOTPATCHED",
		"--- /dev/null",
		"+++ b/pkg/calc/HOTPATCHED",
		"@@ -0,0 +1,2 @@",
		"+patched-by=rescue-actor",
		"+verified=true",
		"",
	}, "\n")

	patchResp, err := coord.client.RescuePatch(ctx, supervisor.RescuePatchRequest{
		Diff: unifiedDiff,
		Files: map[string]string{
			"config/app.yaml": "env: production\n",
		},
	})
	if err != nil {
		t.Fatalf("RescuePatch(diff+files): %v", err)
	}
	if len(patchResp.PatchedFiles) != 3 {
		t.Fatalf("PatchedFiles = %v, want 3 files", patchResp.PatchedFiles)
	}

	// Verify patched contents and re-run verification step on supervisor.
	verifyStep, err := coord.client.RunStep(ctx, supervisor.RunStepRequest{
		Name: "verify-hotpatch",
		Script: `grep -q 'return a + b' pkg/calc/calc.go
grep -q 'return "fixed"' pkg/calc/calc.go
grep -q 'patched-by=rescue-actor' pkg/calc/HOTPATCHED
grep -q 'env: production' config/app.yaml
`,
	})
	if err != nil {
		t.Fatalf("RunStep(verify-hotpatch): %v", err)
	}
	verifyDone, err := coord.client.WaitStep(ctx, verifyStep.StepID)
	if err != nil {
		t.Fatalf("WaitStep(verify-hotpatch): %v", err)
	}
	if verifyDone.State != supervisor.StepStateCompleted {
		t.Fatalf("verify-hotpatch state = %q, want COMPLETED", verifyDone.State)
	}

	// 4. Reject mismatched diff hunk atomically without modifying files.
	badHunkDiff := strings.Join([]string{
		"--- a/pkg/calc/calc.go",
		"+++ b/pkg/calc/calc.go",
		"@@ -3,3 +3,3 @@",
		" // Header comment",
		"-func NonExistent() {",
		"+func Replaced() {",
		"",
	}, "\n")
	if _, badDiffErr := coord.client.RescuePatch(ctx, supervisor.RescuePatchRequest{
		Diff: badHunkDiff,
	}); badDiffErr == nil {
		t.Fatalf("expected error on mismatched diff context, got nil")
	}

	// 5. Reject path traversal and symlink escapes across /v1/files and /v1/rescue/patch.
	if err := os.Symlink(outsideFile, filepath.Join(workspaceDir, "escape-link.txt")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	for _, badPath := range []string{
		"../outside-secret.txt",
		"pkg/../../outside-secret.txt",
		"/etc/passwd",
		"..\\outside-secret.txt",
		"escape-link.txt",
	} {
		if _, err := coord.client.UploadFiles(ctx, supervisor.UploadFilesRequest{
			Files: map[string][]byte{badPath: []byte("pwned")},
		}); err == nil {
			t.Fatalf("UploadFiles accepted unsafe path %q", badPath)
		}
		if _, err := coord.client.RescuePatch(ctx, supervisor.RescuePatchRequest{
			Files: map[string]string{badPath: "pwned"},
		}); err == nil {
			t.Fatalf("RescuePatch(Files) accepted unsafe path %q", badPath)
		}
	}
	traversalDiff := strings.Join([]string{
		"--- a/../outside-secret.txt",
		"+++ b/../outside-secret.txt",
		"@@ -1 +1 @@",
		"-untouched",
		"+pwned",
		"",
	}, "\n")
	if _, err := coord.client.RescuePatch(ctx, supervisor.RescuePatchRequest{Diff: traversalDiff}); err == nil {
		t.Fatalf("RescuePatch(Diff) accepted ../ traversal header")
	}
	outsideData, err := os.ReadFile(outsideFile)
	if err != nil || string(outsideData) != "untouched" {
		t.Fatalf("outside-secret.txt was modified: %q (err=%v)", string(outsideData), err)
	}

	// 6. Worker role must reject rescue endpoints.
	if _, err := worker.client.RescueExec(ctx, supervisor.RescueExecRequest{Script: "true"}); err == nil {
		t.Fatalf("expected worker RescueExec to fail, got nil")
	}
	if _, err := worker.client.RescuePatch(ctx, supervisor.RescuePatchRequest{
		Files: map[string]string{"config/app.yaml": "nope\n"},
	}); err == nil {
		t.Fatalf("expected worker RescuePatch to fail, got nil")
	}
}
