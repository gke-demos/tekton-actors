package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// BuildCommit is stamped at compile time via -ldflags="-X main.BuildCommit=<sha>".
var BuildCommit = "dev"

func main() {
	var listenAddr string
	flag.StringVar(&listenAddr, "listen", "0.0.0.0:8080", "HTTP listen address")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("fulfillment-api listening", "addr", listenAddr, "commit", BuildCommit, "deployedBy", os.Getenv("DEPLOYED_BY"))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}
