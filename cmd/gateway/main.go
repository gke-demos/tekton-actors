// Command gateway runs the Mission-Control Web UI and REST API for Tekton on Agent Substrate.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/gateway"
	"github.com/gke-demos/tekton-actors/pkg/rescue"
	tektonclient "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		listenAddr      = flag.String("listen-addr", envOrDefault("LISTEN_ADDR", ":8080"), "HTTP listen address")
		kubeconfig      = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "Path to kubeconfig (optional when running in-cluster)")
		namespace       = flag.String("namespace", envOrDefault("NAMESPACE", gateway.DefaultNamespace), "Kubernetes namespace for Tekton PipelineRuns and CustomRuns")
		atespace        = flag.String("atespace", envOrDefault("ATESPACE", gateway.DefaultAtespace), "Agent Substrate atespace for Workspace/Branch/Rescue Actors")
		systemNamespace = flag.String("ate-system-namespace", envOrDefault("ATE_SYSTEM_NAMESPACE", rescue.DefaultSystemNamespace), "Agent Substrate control plane namespace")
		pipelineName    = flag.String("pipeline-name", envOrDefault("PIPELINE_NAME", gateway.DefaultPipelineName), "Default Tekton Pipeline name")
		workerPoolName  = flag.String("worker-pool", envOrDefault("WORKER_POOL_NAME", gateway.DefaultWorkerPool), "Default Agent Substrate WorkerPool name")
		egressSecret    = flag.String("egress-secret", envOrDefault("EGRESS_SECRET", gateway.DefaultEgressSecret), "Default k8s-credential-provider secret reference")
		gitOCIURL       = flag.String("git-oci-url", envOrDefault("GIT_OCI_URL", gateway.DefaultGitOCIURL), "Base URL of the in-cluster Git + OCI server")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restCfg, err := buildRestConfig(*kubeconfig)
	if err != nil {
		log.Fatalf("build kubernetes rest config: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("create kubernetes client: %v", err)
	}

	tektonClient, err := tektonclient.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("create tekton client: %v", err)
	}

	var (
		controlClient ateapi.ControlClient
		rescueService *rescue.Service
	)
	bridge, err := rescue.NewClusterBridge(ctx, kubeClient, *systemNamespace)
	if err != nil {
		log.Printf("warning: Substrate ClusterBridge initialization failed (%v); running without live Actor control bridge", err)
	} else {
		defer func() { _ = bridge.Close() }()
		controlClient = bridge.Control()
		rescueService = rescue.NewService(controlClient, tektonClient, bridge.ActorEndpoint)
	}

	srv := gateway.NewServer(gateway.Config{
		Namespace:      *namespace,
		Atespace:       *atespace,
		PipelineName:   *pipelineName,
		WorkerPoolName: *workerPoolName,
		EgressSecret:   *egressSecret,
		GitOCIURL:      *gitOCIURL,
		KubeClient:     kubeClient,
		TektonClient:   tektonClient,
		ControlClient:  controlClient,
		RescueService:  rescueService,
	})
	srv.StartBackgroundReconciler(ctx)

	httpServer := &http.Server{
		Addr:              *listenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("tekton-actors gateway listening on %s (namespace=%s, atespace=%s)", *listenAddr, *namespace, *atespace)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("gateway HTTP server failed: %v", err)
	}
}

func buildRestConfig(kubeconfig string) (*rest.Config, error) {
	if strings.TrimSpace(kubeconfig) != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
