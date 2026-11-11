package main_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
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

func randomToken(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return "cli-tok-" + hex.EncodeToString(b[:])
}

func TestGitOCIServerBinaryLifecycle(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "git-oci-server")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build git-oci-server: %v\n%s", err, string(out))
	}

	// 1. Fails closed when neither --auth-token nor GIT_OCI_AUTH_TOKEN is provided.
	noAuthCmd := exec.Command(binPath, "--listen=127.0.0.1:0", "--data-dir="+t.TempDir())
	noAuthCmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	if out, err := noAuthCmd.CombinedOutput(); err == nil {
		t.Fatalf("expected git-oci-server without token to fail closed, but succeeded:\n%s", string(out))
	}

	// 2. Starts cleanly with GIT_OCI_AUTH_TOKEN, serves /readyz, /repo/*, and /v2/, and exits 0 on SIGTERM.
	addr := reserveLoopbackAddr(t)
	token := randomToken(t)
	dataDir := t.TempDir()

	proc := exec.Command(binPath, "--listen="+addr, "--data-dir="+dataDir)
	proc.Env = append(os.Environ(), "GIT_OCI_AUTH_TOKEN="+token)
	if err := proc.Start(); err != nil {
		t.Fatalf("start git-oci-server: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- proc.Wait() }()
	t.Cleanup(func() {
		if proc.Process != nil {
			_ = proc.Process.Kill()
		}
	})

	baseURL := "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(baseURL + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for git-oci-server /readyz")
		}
		time.Sleep(25 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Verify /v2/ authenticated ping.
	v2Req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v2/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
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

	// Verify git clone against running binary.
	cloneDst := filepath.Join(t.TempDir(), "cloned")
	cloneCmd := exec.CommandContext(ctx, "git",
		"-c", "http.extraHeader=Authorization: Bearer "+token,
		"clone", baseURL+"/repo/fulfillment-api.git", cloneDst,
	)
	cloneCmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cloneCmd.CombinedOutput(); err != nil {
		t.Fatalf("git clone from binary failed: %v\n%s", err, string(out))
	}

	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatalf("graceful shutdown returned non-zero: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for graceful shutdown")
	}
}
