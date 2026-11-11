package main

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
)

func TestProbeHandler_HealthzAndReadyz(t *testing.T) {
	var ready atomic.Bool

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(127.0.0.1:0) error: %v", err)
	}
	srv := newProbeServer(ln.Addr().String(), ready.Load)
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	baseURL := "http://" + ln.Addr().String()
	httpClient := &http.Client{Timeout: 2 * time.Second}

	// /healthz returns 200 OK with security headers even before readiness.
	resp, err := httpClient.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz error: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
	}

	// Disallowed methods are rejected with 405.
	postResp, err := httpClient.Post(baseURL+"/healthz", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST /healthz error: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz status = %d, want %d", postResp.StatusCode, http.StatusMethodNotAllowed)
	}

	// /readyz returns 503 before cache sync, then 200 after ready=true.
	readyResp1, err := httpClient.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz (unready) error: %v", err)
	}
	_ = readyResp1.Body.Close()
	if readyResp1.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz (unready) status = %d, want %d", readyResp1.StatusCode, http.StatusServiceUnavailable)
	}

	ready.Store(true)
	readyResp2, err := httpClient.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz (ready) error: %v", err)
	}
	_ = readyResp2.Body.Close()
	if readyResp2.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz (ready) status = %d, want %d", readyResp2.StatusCode, http.StatusOK)
	}
}

func TestRunControllerWithProbe_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(127.0.0.1:0) error: %v", err)
	}
	probeAddr := ln.Addr().String()
	_ = ln.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, fake.NewSimpleClientset(), options{
			namespace:    "default",
			probeAddr:    probeAddr,
			workers:      1,
			resyncPeriod: 0,
		})
	}()

	httpClient := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	readyOK := false
	for time.Now().Before(deadline) {
		resp, reqErr := httpClient.Get("http://" + probeAddr + "/readyz")
		if reqErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				readyOK = true
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !readyOK {
		t.Fatal("timed out waiting for /readyz 200 OK")
	}

	cancel()
	select {
	case runErr := <-errCh:
		if runErr != nil {
			t.Fatalf("run() returned error on graceful shutdown: %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for run() graceful shutdown")
	}
}
