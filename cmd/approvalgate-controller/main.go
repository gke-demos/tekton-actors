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
	"syscall"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/approvalgate"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type options struct {
	kubeconfig   string
	namespace    string
	probeAddr    string
	workers      int
	resyncPeriod time.Duration
}

func main() {
	var opts options
	flag.StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional; defaults to in-cluster config)")
	flag.StringVar(&opts.namespace, "namespace", "", "Namespace to watch for ApprovalGate CustomRuns (empty watches all namespaces)")
	flag.StringVar(&opts.probeAddr, "probe-addr", ":8081", "HTTP bind address for /healthz and /readyz probes")
	flag.IntVar(&opts.workers, "workers", 2, "Number of concurrent reconciler workers")
	flag.DurationVar(&opts.resyncPeriod, "resync-period", 30*time.Second, "Informer cache resync period")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	restCfg, err := buildRESTConfig(opts.kubeconfig)
	if err != nil {
		logger.Error("failed to load Kubernetes REST config", "err", err)
		os.Exit(1)
	}

	tektonClient, err := versioned.NewForConfig(restCfg)
	if err != nil {
		logger.Error("failed to construct Tekton clientset", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, tektonClient, opts); err != nil {
		logger.Error("approvalgate-controller exited with error", "err", err)
		os.Exit(1)
	}
}

func buildRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func run(ctx context.Context, tektonClient versioned.Interface, opts options) error {
	if opts.workers <= 0 {
		opts.workers = 2
	}

	reconciler := approvalgate.NewReconciler(tektonClient)
	ctrl := approvalgate.NewController(tektonClient, reconciler, approvalgate.ControllerConfig{
		Namespace:    opts.namespace,
		ResyncPeriod: opts.resyncPeriod,
		Workers:      opts.workers,
		Logger:       slog.Default(),
	})

	ln, err := net.Listen("tcp", opts.probeAddr)
	if err != nil {
		return fmt.Errorf("listen probe address %q: %w", opts.probeAddr, err)
	}

	probeSrv := newProbeServer(opts.probeAddr, ctrl.Ready)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	probeErrCh := make(chan error, 1)
	go func() {
		if serveErr := probeSrv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			probeErrCh <- serveErr
			return
		}
		probeErrCh <- nil
	}()

	ctrlErrCh := make(chan error, 1)
	go func() {
		ctrlErrCh <- ctrl.Run(runCtx)
	}()

	var firstErr error
	select {
	case <-ctx.Done():
	case err := <-probeErrCh:
		if err != nil {
			firstErr = fmt.Errorf("probe server failed: %w", err)
		}
		cancel()
	case err := <-ctrlErrCh:
		if err != nil {
			firstErr = fmt.Errorf("controller failed: %w", err)
		}
		shutdownProbe(probeSrv)
		return firstErr
	}

	cancel()
	shutdownProbe(probeSrv)
	if err := <-ctrlErrCh; err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func shutdownProbe(srv *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// newProbeServer returns an HTTP server exposing /healthz and /readyz for kubelet probes.
// TODO(security): Kubelet liveness/readiness probes are unauthenticated read-only endpoints by design;
// restrict network reachability to pod-local / kubelet traffic via Kubernetes NetworkPolicy in production.
func newProbeServer(addr string, readyFn func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		setProbeSecurityHeaders(w)
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("ok\n"))
		}
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		setProbeSecurityHeaders(w)
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if readyFn == nil || !readyFn() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("ok\n"))
		}
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}

func setProbeSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	h.Set("Cache-Control", "no-store")
}
