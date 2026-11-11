package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/gitoci"
)

type options struct {
	listenAddr    string
	dataDir       string
	repoName      string
	authToken     string
	authTokenFile string
}

func main() {
	var opts options
	flag.StringVar(&opts.listenAddr, "listen", envOrDefault("GIT_OCI_LISTEN", ":8080"), "HTTP listen address")
	flag.StringVar(&opts.dataDir, "data-dir", envOrDefault("GIT_OCI_DATA_DIR", "/var/lib/git-oci"), "Directory storing bare Git repos and OCI blobs/manifests")
	flag.StringVar(&opts.repoName, "repo-name", envOrDefault("GIT_OCI_REPO_NAME", gitoci.DefaultRepoName), "Repository name served under /repo/<name>.git and /v2/<name>/")
	flag.StringVar(&opts.authToken, "auth-token", "", "Required Bearer token for /repo/* and /v2/* (prefers GIT_OCI_AUTH_TOKEN or --auth-token-file)")
	flag.StringVar(&opts.authTokenFile, "auth-token-file", os.Getenv("GIT_OCI_AUTH_TOKEN_FILE"), "Path to file containing the required Bearer token")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	token, err := resolveAuthToken(opts.authToken, opts.authTokenFile)
	if err != nil {
		logger.Error("refusing to start without configured bearer token", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger, opts, token); err != nil {
		logger.Error("git-oci-server exited with error", "err", err)
		os.Exit(1)
	}
}

func resolveAuthToken(flagToken, tokenFile string) (string, error) {
	if v := strings.TrimSpace(flagToken); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(os.Getenv("GIT_OCI_AUTH_TOKEN")); v != "" {
		return v, nil
	}
	if p := strings.TrimSpace(tokenFile); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			return "", fmt.Errorf("read auth token file: %w", err)
		}
		if v := strings.TrimSpace(string(b)); v != "" {
			return v, nil
		}
	}
	return "", errors.New("provide --auth-token, GIT_OCI_AUTH_TOKEN, or --auth-token-file")
}

func run(ctx context.Context, logger *slog.Logger, opts options, token string) error {
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   opts.dataDir,
		AuthToken: token,
		RepoName:  opts.repoName,
	})
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", opts.listenAddr, err)
	}

	httpSrv := &http.Server{
		Addr:              opts.listenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if serveErr := httpSrv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	logger.Info("git-oci-server listening",
		"addr", ln.Addr().String(),
		"repo", opts.repoName,
		"mainCommit", srv.MainCommitSHA(),
		"buggyCommit", srv.BuggyCommitSHA(),
	)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		return <-errCh
	case serveErr := <-errCh:
		return serveErr
	}
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
