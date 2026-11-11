package main_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/supervisor"
)

func reserveLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitForReady(t *testing.T, cli *supervisor.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := cli.Ready(ctx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for step-supervisor /readyz: %v", err)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestStepSupervisorBinary_FlagsEnvLivenessAndGracefulShutdown(t *testing.T) {
	t.Parallel()

	binPath := filepath.Join(t.TempDir(), "step-supervisor")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build step-supervisor: %v\n%s", err, string(out))
	}

	root := t.TempDir()
	workspaceDir := filepath.Join(root, "workspace")
	resultsDir := filepath.Join(root, "tekton", "results")

	coordAddr := reserveLoopbackAddr(t)
	workerAddr := reserveLoopbackAddr(t)

	// Start coordinator using CLI flags.
	coordProc := exec.Command(
		binPath,
		"--listen="+coordAddr,
		"--role=coordinator",
		"--workspace-dir="+workspaceDir,
		"--results-dir="+resultsDir,
	)
	if err := coordProc.Start(); err != nil {
		t.Fatalf("start coordinator binary: %v", err)
	}
	coordWaitCh := make(chan error, 1)
	go func() { coordWaitCh <- coordProc.Wait() }()
	t.Cleanup(func() {
		if coordProc.Process != nil {
			_ = coordProc.Process.Kill()
		}
	})

	// Start worker using STEP_SUPERVISOR_* environment variables.
	workerProc := exec.Command(binPath)
	workerProc.Env = append(
		os.Environ(),
		"STEP_SUPERVISOR_LISTEN="+workerAddr,
		"STEP_SUPERVISOR_ROLE=worker",
		"STEP_SUPERVISOR_WORKSPACE_DIR="+workspaceDir,
		"STEP_SUPERVISOR_RESULTS_DIR="+resultsDir,
	)
	if err := workerProc.Start(); err != nil {
		t.Fatalf("start worker binary: %v", err)
	}
	workerWaitCh := make(chan error, 1)
	go func() { workerWaitCh <- workerProc.Wait() }()
	t.Cleanup(func() {
		if workerProc.Process != nil {
			_ = workerProc.Process.Kill()
		}
	})

	coordClient := supervisor.NewClient("http://"+coordAddr, nil)
	workerClient := supervisor.NewClient("http://"+workerAddr, nil)
	waitForReady(t, coordClient)
	waitForReady(t, workerClient)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Run a step on worker via coordinator.
	st, err := coordClient.RunStep(ctx, supervisor.RunStepRequest{
		Name:       "binary-worker-step",
		TargetAddr: workerAddr,
		Env: []supervisor.EnvEntry{
			{Name: "RESULTS_DIR", Value: resultsDir},
		},
		Script: "printf 'binary-ok' > \"${RESULTS_DIR}/status\"\n",
	})
	if err != nil {
		t.Fatalf("RunStep(binary-worker-step): %v", err)
	}
	done, err := coordClient.WaitStep(ctx, st.StepID)
	if err != nil {
		t.Fatalf("WaitStep: %v", err)
	}
	if done.State != supervisor.StepStateCompleted || done.Results["status"] != "binary-ok" {
		t.Fatalf("unexpected step result: %+v", done)
	}

	// Run a non-zero exit step and ensure coordinator daemon stays alive!
	failSt, err := coordClient.RunStep(ctx, supervisor.RunStepRequest{
		Name:   "binary-fail-step",
		Script: "exit 17\n",
	})
	if err != nil {
		t.Fatalf("RunStep(binary-fail-step): %v", err)
	}
	failDone, err := coordClient.WaitStep(ctx, failSt.StepID)
	if err != nil {
		t.Fatalf("WaitStep(binary-fail-step): %v", err)
	}
	if failDone.State != supervisor.StepStateFailed || failDone.ExitCode == nil || *failDone.ExitCode != 17 {
		t.Fatalf("unexpected failDone: %+v", failDone)
	}

	select {
	case exitErr := <-coordWaitCh:
		t.Fatalf("coordinator exited prematurely after step failure: %v", exitErr)
	default:
	}
	if err := coordClient.Ready(ctx); err != nil {
		t.Fatalf("coordinator Ready after failing step: %v", err)
	}

	// Graceful SIGTERM shutdown must exit 0.
	if err := coordProc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal coordinator SIGTERM: %v", err)
	}
	if err := workerProc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal worker SIGTERM: %v", err)
	}

	select {
	case err := <-coordWaitCh:
		if err != nil {
			t.Fatalf("coordinator graceful shutdown returned non-zero: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for coordinator graceful shutdown")
	}

	select {
	case err := <-workerWaitCh:
		if err != nil {
			t.Fatalf("worker graceful shutdown returned non-zero: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for worker graceful shutdown")
	}
}
