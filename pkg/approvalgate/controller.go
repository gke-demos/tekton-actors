package approvalgate

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	tektoninformers "github.com/tektoncd/pipeline/pkg/client/informers/externalversions"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// ControllerConfig configures the ApprovalGate informer/workqueue Controller.
type ControllerConfig struct {
	// Namespace restricts the watch to a single namespace when non-empty.
	// An empty string watches all namespaces.
	Namespace string
	// ResyncPeriod configures periodic informer cache resyncs.
	ResyncPeriod time.Duration
	// Workers is the number of concurrent reconciliation workers (defaults to 2).
	Workers int
	// Logger receives structured controller diagnostics (defaults to slog.Default()).
	Logger *slog.Logger
}

// Controller watches Tekton CustomRuns via an informer factory and reconciles
// ApprovalGate resources through a rate-limited workqueue.
type Controller struct {
	reconciler *Reconciler
	factory    tektoninformers.SharedInformerFactory
	queue      workqueue.TypedRateLimitingInterface[string]
	workers    int
	logger     *slog.Logger
	ready      atomic.Bool
}

// NewController constructs a new ApprovalGate Controller wired to a Tekton SharedInformerFactory.
func NewController(client versioned.Interface, reconciler *Reconciler, cfg ControllerConfig) *Controller {
	workers := cfg.Workers
	if workers <= 0 {
		workers = 2
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var factoryOpts []tektoninformers.SharedInformerOption
	if cfg.Namespace != "" {
		factoryOpts = append(factoryOpts, tektoninformers.WithNamespace(cfg.Namespace))
	}
	factory := tektoninformers.NewSharedInformerFactoryWithOptions(client, cfg.ResyncPeriod, factoryOpts...)

	c := &Controller{
		reconciler: reconciler,
		factory:    factory,
		queue:      workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		workers:    workers,
		logger:     logger,
	}

	informer := factory.Tekton().V1beta1().CustomRuns().Informer()
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueue,
		UpdateFunc: func(_, newObj any) {
			c.enqueue(newObj)
		},
	})

	return c
}

// Ready reports whether the informer cache has synced and workers are active.
func (c *Controller) Ready() bool {
	return c.ready.Load()
}

// Run starts the informer factory and worker loop until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		c.ready.Store(false)
		cancel()
		c.queue.ShutDown()
		c.factory.Shutdown()
	}()

	c.factory.Start(runCtx.Done())

	for typ, synced := range c.factory.WaitForCacheSync(runCtx.Done()) {
		if !synced {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to sync informer cache for %v", typ)
		}
	}

	c.ready.Store(true)
	c.logger.Info("ApprovalGate controller started", "workers", c.workers)

	var wg sync.WaitGroup
	for range c.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.processNextWorkItem(runCtx) {
			}
		}()
	}

	<-runCtx.Done()
	c.queue.ShutDown()
	wg.Wait()
	c.logger.Info("ApprovalGate controller stopped")
	return nil
}

func (c *Controller) enqueue(obj any) {
	cr, ok := extractCustomRun(obj)
	if !ok || !IsApprovalGate(cr) || cr.IsDone() {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(cr)
	if err != nil {
		return
	}
	c.queue.Add(key)
}

func extractCustomRun(obj any) (*v1beta1.CustomRun, bool) {
	if cr, ok := obj.(*v1beta1.CustomRun); ok {
		return cr, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	cr, ok := tombstone.Obj.(*v1beta1.CustomRun)
	return cr, ok
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		c.queue.Forget(key)
		return true
	}

	if err := c.reconciler.Reconcile(ctx, namespace, name); err != nil {
		if ctx.Err() == nil {
			c.logger.Error("reconciling ApprovalGate CustomRun failed", "key", key, "err", err)
			c.queue.AddRateLimited(key)
		}
		return true
	}

	c.queue.Forget(key)
	return true
}
