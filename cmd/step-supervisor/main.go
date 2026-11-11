// Command step-supervisor runs as PID 1 inside Agent Substrate Actor step
// containers, executing Tekton steps on demand over HTTP, streaming live logs,
// capturing /tekton/results/* payloads, and keeping the sandbox alive across
// step completions and failures.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/supervisor"
)

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	var (
		listenAddr   string
		role         string
		resultsDir   string
		workspaceDir string
		tektonDir    string
	)

	flag.StringVar(&listenAddr, "listen", envOrDefault("STEP_SUPERVISOR_LISTEN", supervisor.DefaultListenAddr), "TCP address to listen on (env STEP_SUPERVISOR_LISTEN)")
	flag.StringVar(&role, "role", envOrDefault("STEP_SUPERVISOR_ROLE", string(supervisor.RoleCoordinator)), "Supervisor role: coordinator or worker (env STEP_SUPERVISOR_ROLE)")
	flag.StringVar(&resultsDir, "results-dir", envOrDefault("STEP_SUPERVISOR_RESULTS_DIR", supervisor.DefaultResultsDir), "Directory for Tekton step results (env STEP_SUPERVISOR_RESULTS_DIR)")
	flag.StringVar(&workspaceDir, "workspace-dir", envOrDefault("STEP_SUPERVISOR_WORKSPACE_DIR", supervisor.DefaultWorkspaceDir), "Workspace directory (env STEP_SUPERVISOR_WORKSPACE_DIR)")
	flag.StringVar(&tektonDir, "tekton-dir", envOrDefault("STEP_SUPERVISOR_TEKTON_DIR", ""), "Tekton metadata root directory; defaults to parent of --results-dir (env STEP_SUPERVISOR_TEKTON_DIR)")
	flag.Parse()

	srv, err := supervisor.NewServer(supervisor.Config{
		Role:         supervisor.Role(role),
		WorkspaceDir: workspaceDir,
		ResultsDir:   resultsDir,
		TektonDir:    tektonDir,
	})
	if err != nil {
		log.Fatalf("step-supervisor configuration error: %v", err)
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("step-supervisor listen %s: %v", listenAddr, err)
	}

	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	serveErrCh := make(chan error, 1)
	go func() {
		log.Printf("step-supervisor (%s) listening on %s (workspace=%s results=%s)", role, ln.Addr().String(), workspaceDir, resultsDir)
		serveErrCh <- httpServer.Serve(ln)
	}()

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Fatalf("step-supervisor serve error: %v", serveErr)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("step-supervisor graceful shutdown error: %v", err)
		}
		if serveErr := <-serveErrCh; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Fatalf("step-supervisor serve error: %v", serveErr)
		}
	}
}
