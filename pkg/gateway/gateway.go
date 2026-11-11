// Package gateway serves the Mission-Control Web UI (/ , /compare, /rescue, /about)
// and JSON REST API for Tekton on Agent Substrate.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/approvalgate"
	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	"github.com/gke-demos/tekton-actors/pkg/rescue"
	"github.com/gke-demos/tekton-actors/pkg/supervisor"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	tektonclient "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/api/ateapi/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/apis"
)

const (
	DefaultNamespace    = "tekton-actors"
	DefaultAtespace     = "tekton-actors"
	DefaultPipelineName = "release-pipeline"
	DefaultWorkerPool   = "tekton-actors-pool"
	DefaultEgressSecret = "git-oci-auth/token"
	DefaultGitOCIURL    = "http://git-oci.tekton-actors.svc.cluster.local"
)

// Config configures the Mission-Control Gateway server.
type Config struct {
	Namespace      string
	Atespace       string
	PipelineName   string
	WorkerPoolName string
	EgressSecret   string
	GitOCIURL      string
	KubeClient     kubernetes.Interface
	TektonClient   tektonclient.Interface
	ControlClient  ateapi.ControlClient
	RescueService  *rescue.Service
	HTTPClient     *http.Client
}

// Server implements the Mission-Control HTTP handler.
type Server struct {
	cfg     Config
	handler http.Handler
}

// NewServer constructs a Mission-Control Gateway Server.
func NewServer(cfg Config) *Server {
	if strings.TrimSpace(cfg.Namespace) == "" {
		cfg.Namespace = DefaultNamespace
	}
	if strings.TrimSpace(cfg.Atespace) == "" {
		cfg.Atespace = DefaultAtespace
	}
	if strings.TrimSpace(cfg.PipelineName) == "" {
		cfg.PipelineName = DefaultPipelineName
	}
	if strings.TrimSpace(cfg.WorkerPoolName) == "" {
		cfg.WorkerPoolName = DefaultWorkerPool
	}
	if strings.TrimSpace(cfg.EgressSecret) == "" {
		cfg.EgressSecret = DefaultEgressSecret
	}
	if strings.TrimSpace(cfg.GitOCIURL) == "" {
		cfg.GitOCIURL = DefaultGitOCIURL
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}

	s := &Server{cfg: cfg}
	mux := http.NewServeMux()

	// Health & Readiness probes
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Path-based SPA routes (/, /compare, /rescue, /about)
	mux.HandleFunc("GET /{$}", s.handleUI)
	mux.HandleFunc("GET /compare", s.handleUI)
	mux.HandleFunc("GET /rescue", s.handleUI)
	mux.HandleFunc("GET /about", s.handleUI)

	// REST API routes
	mux.HandleFunc("GET /api/state", s.handleGetState)
	mux.HandleFunc("POST /api/pipelineruns", s.handleCreatePipelineRun)
	mux.HandleFunc("POST /api/approvals/{name}", s.handleSetApprovalDecision)
	mux.HandleFunc("GET /api/logs", s.handleGetLogs)
	mux.HandleFunc("POST /api/rescue/fork", s.handleRescueFork)
	mux.HandleFunc("POST /api/rescue/exec", s.handleRescueExec)
	mux.HandleFunc("POST /api/rescue/patch", s.handleRescuePatch)
	mux.HandleFunc("POST /api/rescue/commit-and-resume", s.handleRescueCommitAndResume)
	mux.HandleFunc("POST /api/actors/{name}/suspend", s.handleSuspendActor)

	s.handler = withSecurityHeaders(mux)
	return s
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	return s.handler
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, indexHTML)
}

// StateSnapshot is the JSON payload returned by GET /api/state.
type StateSnapshot struct {
	Timestamp     time.Time            `json:"timestamp"`
	Namespace     string               `json:"namespace"`
	Atespace      string               `json:"atespace"`
	PipelineRuns  []PipelineRunView    `json:"pipelineRuns"`
	ApprovalGates []ApprovalGateView   `json:"approvalGates"`
	Substrate     SubstrateView        `json:"substrate"`
	GitOCIAudit   *gitoci.AuditSummary `json:"gitOciAudit,omitempty"`
	SampleFixDiff string               `json:"sampleFixDiff"`
}

// PipelineRunView summarizes a PipelineRun and its child DAG nodes.
type PipelineRunView struct {
	Name               string            `json:"name"`
	Mode               string            `json:"mode"` // "actor" or "pod-pvc"
	Status             string            `json:"status"`
	Reason             string            `json:"reason"`
	Message            string            `json:"message"`
	WorkspaceActor     string            `json:"workspaceActor,omitempty"`
	MilestoneTag       string            `json:"milestoneTag,omitempty"`
	SourceTag          string            `json:"sourceTag,omitempty"`
	ResumedFrom        string            `json:"resumedFrom,omitempty"`
	SkipTasks          []string          `json:"skipTasks,omitempty"`
	SkippedTasks       []string          `json:"skippedTasks,omitempty"`
	InjectBug          bool              `json:"injectBug"`
	StartTime          *time.Time        `json:"startTime,omitempty"`
	CompletionTime     *time.Time        `json:"completionTime,omitempty"`
	DurationSeconds    float64           `json:"durationSeconds"`
	Results            map[string]string `json:"results"`
	Tasks              []TaskNodeView    `json:"tasks"`
	PendingApprovalRun string            `json:"pendingApprovalRun,omitempty"`
}

// TaskNodeView summarizes a single DAG task inside a PipelineRun.
type TaskNodeView struct {
	TaskName        string            `json:"taskName"`
	RunName         string            `json:"runName,omitempty"`
	Kind            string            `json:"kind"` // "TaskRun" or "CustomRun"
	Status          string            `json:"status"`
	Reason          string            `json:"reason,omitempty"`
	Message         string            `json:"message,omitempty"`
	ActorMode       string            `json:"actorMode,omitempty"` // "trunk", "branch", "pod", "custom"
	ActorName       string            `json:"actorName,omitempty"`
	ActorTemplate   string            `json:"actorTemplate,omitempty"`
	WorkerPod       string            `json:"workerPod,omitempty"`
	PodName         string            `json:"podName,omitempty"`
	SourceTag       string            `json:"sourceTag,omitempty"`
	FailureTag      string            `json:"failureTag,omitempty"`
	CurrentStepID   string            `json:"currentStepId,omitempty"`
	DurationSeconds float64           `json:"durationSeconds"`
	Results         map[string]string `json:"results,omitempty"`
	Steps           []StepView        `json:"steps,omitempty"`
}

// StepView summarizes an individual step inside a TaskRun.
type StepView struct {
	Name       string `json:"name"`
	Container  string `json:"container"`
	Status     string `json:"status"`
	ExitCode   int32  `json:"exitCode"`
	Reason     string `json:"reason,omitempty"`
	StepID     string `json:"stepId,omitempty"`
}

// ApprovalGateView summarizes an ApprovalGate CustomRun.
type ApprovalGateView struct {
	Name            string     `json:"name"`
	PipelineRunName string     `json:"pipelineRunName"`
	Status          string     `json:"status"`
	Decision        string     `json:"decision"`
	Approver        string     `json:"approver,omitempty"`
	Note            string     `json:"note,omitempty"`
	Description     string     `json:"description,omitempty"`
	StartTime       *time.Time `json:"startTime,omitempty"`
}

// SubstrateView summarizes live Agent Substrate Actors, Snapshot Tags, and cluster resources.
type SubstrateView struct {
	Actors                 []ActorSummary  `json:"actors"`
	Tags                   []TagSummary    `json:"tags"`
	Templates              []string        `json:"templates"`
	WorkerPods             []WorkerPodView `json:"workerPods"`
	ActiveActorSlots       int             `json:"activeActorSlots"`
	SuspendedActorCount    int             `json:"suspendedActorCount"`
	TotalWorkerSlots       int             `json:"totalWorkerSlots"`
	PVCCount               int             `json:"pvcCount"`
	AffinityAssistantCount int             `json:"affinityAssistantCount"`
}

// ActorSummary summarizes an Actor in the Atespace.
type ActorSummary struct {
	Name       string `json:"name"`
	Template   string `json:"template"`
	State      string `json:"state"`
	SourceTag  string `json:"sourceTag,omitempty"`
	WorkerPod  string `json:"workerPod,omitempty"`
	Generation int64  `json:"generation"`
}

// TagSummary summarizes a Snapshot Tag in the Atespace.
type TagSummary struct {
	Name        string `json:"name"`
	SourceActor string `json:"sourceActor,omitempty"`
	TemplateUID string `json:"templateUid,omitempty"`
	SnapshotURI string `json:"snapshotUri,omitempty"`
	Kind        string `json:"kind"` // "milestone", "failure", "fixed", "custom"
}

// WorkerPodView summarizes a Substrate WorkerPool pod in Kubernetes.
type WorkerPodView struct {
	Name          string `json:"name"`
	NodeName      string `json:"nodeName"`
	Phase         string `json:"phase"`
	AssignedActor string `json:"assignedActor,omitempty"`
	SlotStatus    string `json:"slotStatus"` // "ASSIGNED" or "WARM_IDLE"
}

var canonicalDAGTasks = []string{
	"clone-and-build",
	"unit-test",
	"vuln-scan",
	"integration-smoke",
	"release-approval",
	"package-and-publish",
}

func (s *Server) handleGetState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns := s.cfg.Namespace
	atespace := s.cfg.Atespace

	snap := StateSnapshot{
		Timestamp:     time.Now().UTC(),
		Namespace:     ns,
		Atespace:      atespace,
		PipelineRuns:  []PipelineRunView{},
		ApprovalGates: []ApprovalGateView{},
		SampleFixDiff: gitoci.SampleAppWorkspaceFixDiff,
	}

	var trByPR map[string][]*v1.TaskRun
	var crByPR map[string][]*v1beta1.CustomRun

	if s.cfg.TektonClient != nil {
		trByPR = make(map[string][]*v1.TaskRun)
		if trList, err := s.cfg.TektonClient.TektonV1().TaskRuns(ns).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range trList.Items {
				tr := &trList.Items[i]
				prName := tr.Labels[pipeline.PipelineRunLabelKey]
				trByPR[prName] = append(trByPR[prName], tr)
			}
		}

		crByPR = make(map[string][]*v1beta1.CustomRun)
		if crList, err := s.cfg.TektonClient.TektonV1beta1().CustomRuns(ns).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range crList.Items {
				cr := &crList.Items[i]
				prName := cr.Labels[pipeline.PipelineRunLabelKey]
				crByPR[prName] = append(crByPR[prName], cr)
				if cr.Spec.CustomRef != nil && cr.Spec.CustomRef.Kind == approvalgate.Kind ||
					(cr.Spec.CustomSpec != nil && cr.Spec.CustomSpec.Kind == approvalgate.Kind) {
					snap.ApprovalGates = append(snap.ApprovalGates, buildApprovalGateView(cr))
				}
			}
		}

		if prList, err := s.cfg.TektonClient.TektonV1().PipelineRuns(ns).List(ctx, metav1.ListOptions{}); err == nil {
			sort.Slice(prList.Items, func(i, j int) bool {
				return prList.Items[i].CreationTimestamp.Time.After(prList.Items[j].CreationTimestamp.Time)
			})
			for i := range prList.Items {
				pr := &prList.Items[i]
				snap.PipelineRuns = append(snap.PipelineRuns, buildPipelineRunView(pr, trByPR[pr.Name], crByPR[pr.Name]))
			}
		}
	}

	snap.Substrate = s.buildSubstrateView(ctx, ns, atespace)
	snap.GitOCIAudit = s.fetchGitOCIAudit(ctx)

	writeJSON(w, http.StatusOK, snap)
}

func buildPipelineRunView(pr *v1.PipelineRun, trs []*v1.TaskRun, crs []*v1beta1.CustomRun) PipelineRunView {
	mode := "pod-pvc"
	var sourceTag string
	var skipTasks []string
	for _, wb := range pr.Spec.Workspaces {
		if wb.Actor != nil {
			mode = "actor"
			sourceTag = wb.Actor.SourceTag
			skipTasks = append([]string(nil), wb.Actor.SkipTasks...)
			break
		}
	}
	if sourceTag == "" {
		sourceTag = pr.Annotations["tekton.ate.dev/source-tag"]
	}

	injectBug := false
	for _, p := range pr.Spec.Params {
		if p.Name == "inject-bug" && strings.EqualFold(strings.TrimSpace(p.Value.StringVal), "true") {
			injectBug = true
		}
	}

	cond := pr.Status.GetCondition(apis.ConditionSucceeded)
	statusStr := "Pending"
	reasonStr := ""
	msgStr := ""
	if cond != nil {
		reasonStr = cond.Reason
		msgStr = cond.Message
		switch cond.Status {
		case corev1.ConditionTrue:
			statusStr = "Succeeded"
		case corev1.ConditionFalse:
			statusStr = "Failed"
		default:
			statusStr = "Running"
		}
	}

	var startTime, compTime *time.Time
	var dur float64
	if pr.Status.StartTime != nil {
		t := pr.Status.StartTime.Time
		startTime = &t
		if pr.Status.CompletionTime != nil {
			ct := pr.Status.CompletionTime.Time
			compTime = &ct
			dur = ct.Sub(t).Seconds()
		} else {
			dur = time.Since(t).Seconds()
		}
	}

	results := make(map[string]string, len(pr.Status.Results))
	for _, r := range pr.Status.Results {
		results[r.Name] = r.Value.StringVal
	}

	skippedSet := make(map[string]bool, len(pr.Status.SkippedTasks))
	var skippedList []string
	for _, sk := range pr.Status.SkippedTasks {
		skippedSet[sk.Name] = true
		skippedList = append(skippedList, sk.Name)
	}
	for _, sk := range skipTasks {
		if !skippedSet[sk] {
			skippedSet[sk] = true
			skippedList = append(skippedList, sk)
		}
	}

	trMap := make(map[string]*v1.TaskRun, len(trs))
	for _, tr := range trs {
		trMap[tr.Labels[pipeline.PipelineTaskLabelKey]] = tr
	}
	crMap := make(map[string]*v1beta1.CustomRun, len(crs))
	for _, cr := range crs {
		crMap[cr.Labels[pipeline.PipelineTaskLabelKey]] = cr
	}

	var pendingApprovalRun string
	tasks := make([]TaskNodeView, 0, len(canonicalDAGTasks))
	for _, taskName := range canonicalDAGTasks {
		if tr, ok := trMap[taskName]; ok {
			tasks = append(tasks, buildTaskRunNodeView(taskName, tr, mode))
			continue
		}
		if cr, ok := crMap[taskName]; ok {
			node := buildCustomRunNodeView(taskName, cr)
			if node.Status == "PendingApproval" {
				pendingApprovalRun = cr.Name
				if statusStr == "Running" {
					statusStr = "PendingApproval"
				}
			}
			tasks = append(tasks, node)
			continue
		}
		nodeStatus := "Queued"
		if skippedSet[taskName] {
			nodeStatus = "Skipped"
		} else if statusStr == "Failed" {
			nodeStatus = "Cancelled"
		}
		defaultActorMode := "pod"
		if mode == "actor" {
			switch taskName {
			case "unit-test", "vuln-scan", "integration-smoke":
				defaultActorMode = "branch"
			case "release-approval":
				defaultActorMode = "custom"
			default:
				defaultActorMode = "trunk"
			}
		} else if taskName == "release-approval" {
			defaultActorMode = "custom"
		}
		tasks = append(tasks, TaskNodeView{
			TaskName:  taskName,
			Kind:      "TaskRun",
			Status:    nodeStatus,
			ActorMode: defaultActorMode,
		})
	}

	return PipelineRunView{
		Name:               pr.Name,
		Mode:               mode,
		Status:             statusStr,
		Reason:             reasonStr,
		Message:            msgStr,
		WorkspaceActor:     pr.Annotations["tekton.ate.dev/workspace-actor"],
		MilestoneTag:       pr.Annotations["tekton.ate.dev/milestone-tag"],
		SourceTag:          sourceTag,
		ResumedFrom:        pr.Annotations[rescue.AnnotationResumedFromPipeline],
		SkipTasks:          skipTasks,
		SkippedTasks:       skippedList,
		InjectBug:          injectBug,
		StartTime:          startTime,
		CompletionTime:     compTime,
		DurationSeconds:    dur,
		Results:            results,
		Tasks:              tasks,
		PendingApprovalRun: pendingApprovalRun,
	}
}

func buildTaskRunNodeView(taskName string, tr *v1.TaskRun, prMode string) TaskNodeView {
	cond := tr.Status.GetCondition(apis.ConditionSucceeded)
	statusStr := "Pending"
	reasonStr := ""
	msgStr := ""
	if cond != nil {
		reasonStr = cond.Reason
		msgStr = cond.Message
		switch cond.Status {
		case corev1.ConditionTrue:
			statusStr = "Succeeded"
		case corev1.ConditionFalse:
			statusStr = "Failed"
		default:
			statusStr = "Running"
		}
	}

	var dur float64
	if tr.Status.StartTime != nil {
		if tr.Status.CompletionTime != nil {
			dur = tr.Status.CompletionTime.Time.Sub(tr.Status.StartTime.Time).Seconds()
		} else {
			dur = time.Since(tr.Status.StartTime.Time).Seconds()
		}
	}

	results := make(map[string]string, len(tr.Status.Results))
	for _, r := range tr.Status.Results {
		results[r.Name] = r.Value.StringVal
	}

	actorMode := tr.Annotations["tekton.ate.dev/actor-mode"]
	if actorMode == "" {
		if prMode == "actor" {
			actorMode = "trunk"
		} else {
			actorMode = "pod"
		}
	}
	failTag := tr.Annotations[rescue.AnnotationFailureTag]
	if failTag == "" && tr.Status.Annotations != nil {
		failTag = tr.Status.Annotations[rescue.AnnotationFailureTag]
	}

	steps := make([]StepView, 0, len(tr.Status.Steps))
	baseUID := string(tr.UID)
	if baseUID == "" {
		baseUID = tr.Namespace + "-" + tr.Name
	}
	for idx, st := range tr.Status.Steps {
		sStatus := "Waiting"
		var exitCode int32
		var sReason string
		if st.Running != nil {
			sStatus = "Running"
		} else if st.Terminated != nil {
			exitCode = st.Terminated.ExitCode
			sReason = st.Terminated.Reason
			if exitCode == 0 && sReason != "Skipped" {
				sStatus = "Succeeded"
			} else if sReason == "Skipped" {
				sStatus = "Skipped"
			} else {
				sStatus = "Failed"
			}
		}
		steps = append(steps, StepView{
			Name:      st.Name,
			Container: st.Container,
			Status:    sStatus,
			ExitCode:  exitCode,
			Reason:    sReason,
			StepID:    fmt.Sprintf("%s-r%d-step-%d", sanitizeStepUID(baseUID), len(tr.Status.RetriesStatus), idx),
		})
	}

	return TaskNodeView{
		TaskName:        taskName,
		RunName:         tr.Name,
		Kind:            "TaskRun",
		Status:          statusStr,
		Reason:          reasonStr,
		Message:         msgStr,
		ActorMode:       actorMode,
		ActorName:       tr.Annotations["tekton.ate.dev/actor-name"],
		ActorTemplate:   tr.Annotations["tekton.ate.dev/actor-template"],
		WorkerPod:       tr.Annotations["tekton.ate.dev/worker-pod"],
		PodName:         tr.Status.PodName,
		SourceTag:       tr.Annotations["tekton.ate.dev/source-tag"],
		FailureTag:      failTag,
		CurrentStepID:   tr.Annotations["tekton.ate.dev/current-step-id"],
		DurationSeconds: dur,
		Results:         results,
		Steps:           steps,
	}
}

func buildCustomRunNodeView(taskName string, cr *v1beta1.CustomRun) TaskNodeView {
	cond := cr.Status.GetCondition(apis.ConditionSucceeded)
	statusStr := "PendingApproval"
	reasonStr := ""
	msgStr := ""
	if cond != nil {
		reasonStr = cond.Reason
		msgStr = cond.Message
		switch cond.Status {
		case corev1.ConditionTrue:
			statusStr = "Succeeded"
		case corev1.ConditionFalse:
			statusStr = "Failed"
		default:
			if cond.Reason == approvalgate.ReasonPendingApproval {
				statusStr = "PendingApproval"
			} else {
				statusStr = "Running"
			}
		}
	}
	var dur float64
	if cr.Status.StartTime != nil {
		if cr.Status.CompletionTime != nil {
			dur = cr.Status.CompletionTime.Time.Sub(cr.Status.StartTime.Time).Seconds()
		} else {
			dur = time.Since(cr.Status.StartTime.Time).Seconds()
		}
	}
	results := make(map[string]string, len(cr.Status.Results))
	for _, r := range cr.Status.Results {
		results[r.Name] = r.Value
	}
	return TaskNodeView{
		TaskName:        taskName,
		RunName:         cr.Name,
		Kind:            "CustomRun",
		Status:          statusStr,
		Reason:          reasonStr,
		Message:         msgStr,
		ActorMode:       "custom",
		DurationSeconds: dur,
		Results:         results,
	}
}

func buildApprovalGateView(cr *v1beta1.CustomRun) ApprovalGateView {
	cond := cr.Status.GetCondition(apis.ConditionSucceeded)
	statusStr := "PendingApproval"
	if cond != nil {
		switch cond.Status {
		case corev1.ConditionTrue:
			statusStr = "Approved"
		case corev1.ConditionFalse:
			statusStr = "Rejected"
		default:
			statusStr = "PendingApproval"
		}
	}
	var desc string
	for _, p := range cr.Spec.Params {
		if p.Name == "description" {
			desc = p.Value.StringVal
		}
	}
	var startTime *time.Time
	if cr.Status.StartTime != nil {
		t := cr.Status.StartTime.Time
		startTime = &t
	}
	return ApprovalGateView{
		Name:            cr.Name,
		PipelineRunName: cr.Labels[pipeline.PipelineRunLabelKey],
		Status:          statusStr,
		Decision:        cr.Annotations[approvalgate.AnnotationDecision],
		Approver:        cr.Annotations[approvalgate.AnnotationApprovedBy],
		Note:            cr.Annotations[approvalgate.AnnotationNote],
		Description:     desc,
		StartTime:       startTime,
	}
}

func (s *Server) buildSubstrateView(ctx context.Context, ns, atespace string) SubstrateView {
	view := SubstrateView{
		Actors:     []ActorSummary{},
		Tags:       []TagSummary{},
		Templates:  []string{},
		WorkerPods: []WorkerPodView{},
	}

	assignedByPod := make(map[string]string)

	if s.cfg.ControlClient != nil {
		if actorsResp, err := s.cfg.ControlClient.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: atespace}); err == nil {
			for _, a := range actorsResp.GetActors() {
				stateStr := a.GetStatus().GetState().String()
				wp := a.GetStatus().GetWorkerAssignment().GetWorkerPod()
				if a.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
					view.ActiveActorSlots++
					if wp != "" {
						assignedByPod[wp] = a.GetMetadata().GetName()
					}
				} else if a.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED {
					view.SuspendedActorCount++
				}
				view.Actors = append(view.Actors, ActorSummary{
					Name:       a.GetMetadata().GetName(),
					Template:   a.GetActorTemplate().GetName(),
					State:      stateStr,
					SourceTag:  a.GetSourceTag().GetName(),
					WorkerPod:  wp,
					Generation: int64(a.GetStatus().GetLastAssignedGeneration()),
				})
			}
		}

		if tagsResp, err := s.cfg.ControlClient.ListTags(ctx, &ateapi.ListTagsRequest{Atespace: atespace}); err == nil {
			for _, t := range tagsResp.GetTags() {
				name := t.GetMetadata().GetName()
				kind := "custom"
				switch {
				case strings.HasPrefix(name, "ms-"):
					kind = "milestone"
				case strings.HasPrefix(name, "failure-"):
					kind = "failure"
				case strings.HasPrefix(name, "fixed-"):
					kind = "fixed"
				}
				view.Tags = append(view.Tags, TagSummary{
					Name:        name,
					SourceActor: t.GetSourceActor().GetName(),
					TemplateUID: t.GetStatus().GetActorTemplateUid(),
					SnapshotURI: t.GetStatus().GetSnapshot().GetUuid(),
					Kind:        kind,
				})
			}
		}

		if tmplResp, err := s.cfg.ControlClient.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{Atespace: atespace}); err == nil {
			for _, t := range tmplResp.GetActorTemplates() {
				view.Templates = append(view.Templates, t.GetMetadata().GetName())
			}
		}
	}

	if s.cfg.KubeClient != nil {
		if pods, err := s.cfg.KubeClient.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{}); err == nil {
			for _, pod := range pods.Items {
				if strings.HasPrefix(pod.Name, "ate-worker-") || pod.Labels["ate.dev/pool"] != "" || pod.Labels["ate.dev/worker-pool"] != "" || pod.Labels["workload"] == "tekton-actors" {
					assigned := assignedByPod[pod.Name]
					slotStatus := "WARM_IDLE"
					if assigned != "" {
						slotStatus = "ASSIGNED"
					}
					view.WorkerPods = append(view.WorkerPods, WorkerPodView{
						Name:          pod.Name,
						NodeName:      pod.Spec.NodeName,
						Phase:         string(pod.Status.Phase),
						AssignedActor: assigned,
						SlotStatus:    slotStatus,
					})
				}
			}
			view.TotalWorkerSlots = len(view.WorkerPods)
		}
		if pvcs, err := s.cfg.KubeClient.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{}); err == nil {
			view.PVCCount = len(pvcs.Items)
		}
		if sts, err := s.cfg.KubeClient.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{}); err == nil {
			view.AffinityAssistantCount = len(sts.Items)
		}
	}

	return view
}

func (s *Server) fetchGitOCIAudit(ctx context.Context) *gitoci.AuditSummary {
	u := strings.TrimRight(s.cfg.GitOCIURL, "/") + "/api/audit"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var summary gitoci.AuditSummary
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&summary); err != nil {
		return nil
	}
	return &summary
}

// CreatePipelineRunRequest is the JSON request body for POST /api/pipelineruns.
type CreatePipelineRunRequest struct {
	Mode            string `json:"mode"` // "actor", "pod-pvc", or "both"
	InjectBug       bool   `json:"injectBug"`
	RequireApproval bool   `json:"requireApproval"`
	ImageTag        string `json:"imageTag,omitempty"`
	RetainActor     bool   `json:"retainActor"`
}

func (s *Server) handleCreatePipelineRun(w http.ResponseWriter, r *http.Request) {
	if s.cfg.TektonClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Tekton client is not configured")
		return
	}
	var req CreatePipelineRunRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "actor"
	}
	if mode != "actor" && mode != "pod-pvc" && mode != "both" {
		writeError(w, http.StatusBadRequest, "mode must be 'actor', 'pod-pvc', or 'both'")
		return
	}

	suffix := fmt.Sprintf("%d", time.Now().Unix()%100000)
	var createdNames []string

	if mode == "actor" || mode == "both" {
		tag := strings.TrimSpace(req.ImageTag)
		if tag == "" {
			tag = "actor-" + suffix
		}
		pr := s.buildActorPipelineRun("release-actor-"+suffix, tag, req.InjectBug, req.RequireApproval, req.RetainActor)
		created, err := s.cfg.TektonClient.TektonV1().PipelineRuns(s.cfg.Namespace).Create(r.Context(), pr, metav1.CreateOptions{})
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("create Actor PipelineRun: %v", err))
			return
		}
		createdNames = append(createdNames, created.Name)
	}

	if mode == "pod-pvc" || mode == "both" {
		tag := strings.TrimSpace(req.ImageTag)
		if tag == "" || mode == "both" {
			tag = "pod-pvc-" + suffix
		}
		pr := s.buildPodPVCPipelineRun("release-pod-pvc-"+suffix, tag, req.InjectBug, req.RequireApproval)
		created, err := s.cfg.TektonClient.TektonV1().PipelineRuns(s.cfg.Namespace).Create(r.Context(), pr, metav1.CreateOptions{})
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("create Pod+PVC PipelineRun: %v", err))
			return
		}
		createdNames = append(createdNames, created.Name)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"created": createdNames,
	})
}

func (s *Server) buildActorPipelineRun(name, imageTag string, injectBug, requireApproval, retainActor bool) *v1.PipelineRun {
	injectStr := "false"
	if injectBug {
		injectStr = "true"
	}
	approvalStr := "approved"
	if requireApproval {
		approvalStr = ""
	}
	annotations := map[string]string{}
	if retainActor {
		annotations["tekton.ate.dev/retain-workspace-actor"] = "true"
	}
	return &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   s.cfg.Namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app.kubernetes.io/part-of":   "tekton-actors",
				"tekton.ate.dev/runtime-mode": "actor",
			},
		},
		Spec: v1.PipelineRunSpec{
			PipelineRef: &v1.PipelineRef{Name: s.cfg.PipelineName},
			Params: []v1.Param{
				{Name: "image-tag", Value: *v1.NewStructuredValues(imageTag)},
				{Name: "inject-bug", Value: *v1.NewStructuredValues(injectStr)},
				{Name: "approval-decision", Value: *v1.NewStructuredValues(approvalStr)},
			},
			Workspaces: []v1.WorkspaceBinding{
				{
					Name: "source",
					Actor: &v1.ActorWorkspaceSource{
						Atespace: s.cfg.Atespace,
						WorkerSelector: map[string]string{
							"workload": "tekton-actors",
						},
						EgressSecret: s.cfg.EgressSecret,
					},
				},
			},
		},
	}
}

func (s *Server) buildPodPVCPipelineRun(name, imageTag string, injectBug, requireApproval bool) *v1.PipelineRun {
	injectStr := "false"
	if injectBug {
		injectStr = "true"
	}
	approvalStr := "approved"
	if requireApproval {
		approvalStr = ""
	}
	return &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.cfg.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/part-of":   "tekton-actors",
				"tekton.ate.dev/runtime-mode": "pod-pvc",
			},
		},
		Spec: v1.PipelineRunSpec{
			PipelineRef: &v1.PipelineRef{Name: s.cfg.PipelineName},
			Params: []v1.Param{
				{Name: "image-tag", Value: *v1.NewStructuredValues(imageTag)},
				{Name: "inject-bug", Value: *v1.NewStructuredValues(injectStr)},
				{Name: "approval-decision", Value: *v1.NewStructuredValues(approvalStr)},
			},
			Workspaces: []v1.WorkspaceBinding{
				{
					Name: "source",
					VolumeClaimTemplate: &corev1.PersistentVolumeClaim{
						Spec: corev1.PersistentVolumeClaimSpec{
							AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
							Resources: corev1.VolumeResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceStorage: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
				{
					Name: "creds",
					Secret: &corev1.SecretVolumeSource{
						SecretName: "git-oci-auth",
					},
				},
			},
		},
	}
}

// ApprovalDecisionRequest is the JSON request body for POST /api/approvals/{name}.
type ApprovalDecisionRequest struct {
	Decision string `json:"decision"` // "approved" or "rejected"
	Approver string `json:"approver,omitempty"`
	Note     string `json:"note,omitempty"`
}

func (s *Server) handleSetApprovalDecision(w http.ResponseWriter, r *http.Request) {
	if s.cfg.TektonClient == nil {
		writeError(w, http.StatusServiceUnavailable, "Tekton client is not configured")
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "CustomRun name is required")
		return
	}
	var req ApprovalDecisionRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	approver := strings.TrimSpace(req.Approver)
	if approver == "" {
		approver = "mission-control-operator"
	}
	note := strings.TrimSpace(req.Note)
	if note == "" {
		note = "Decision recorded via Mission-Control Studio"
	}

	if err := approvalgate.SetDecision(r.Context(), s.cfg.TektonClient, s.cfg.Namespace, name, req.Decision, approver, note); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.cfg.TektonClient.TektonV1beta1().CustomRuns(s.cfg.Namespace).Get(r.Context(), name, metav1.GetOptions{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, buildApprovalGateView(updated))
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	atespace := strings.TrimSpace(q.Get("atespace"))
	if atespace == "" {
		atespace = s.cfg.Atespace
	}
	actorName := strings.TrimSpace(q.Get("actor"))
	stepID := strings.TrimSpace(q.Get("stepId"))
	taskRunName := strings.TrimSpace(q.Get("taskRun"))

	// 1. Try streaming live logs from the Actor's Step Supervisor if running.
	if s.cfg.RescueService != nil && actorName != "" && stepID != "" {
		if logs, err := s.cfg.RescueService.FetchStepLogs(r.Context(), atespace, actorName, stepID, 0); err == nil && logs != "" {
			writeJSON(w, http.StatusOK, map[string]any{
				"source": "step-supervisor",
				"actor":  actorName,
				"stepId": stepID,
				"logs":   logs,
			})
			return
		}
	}

	// 2. Synthesize structured step & result summary from the TaskRun in Kubernetes.
	if s.cfg.TektonClient != nil && taskRunName != "" {
		tr, err := s.cfg.TektonClient.TektonV1().TaskRuns(s.cfg.Namespace).Get(r.Context(), taskRunName, metav1.GetOptions{})
		if err == nil {
			var b strings.Builder
			fmt.Fprintf(&b, "==> TaskRun: %s/%s\n", tr.Namespace, tr.Name)
			if actor := tr.Annotations["tekton.ate.dev/actor-name"]; actor != "" {
				fmt.Fprintf(&b, "==> Substrate Actor: %s/%s (mode=%s, template=%s, worker=%s)\n",
					tr.Annotations["tekton.ate.dev/atespace"],
					actor,
					tr.Annotations["tekton.ate.dev/actor-mode"],
					tr.Annotations["tekton.ate.dev/actor-template"],
					tr.Annotations["tekton.ate.dev/worker-pod"],
				)
				if srcTag := tr.Annotations["tekton.ate.dev/source-tag"]; srcTag != "" {
					fmt.Fprintf(&b, "==> Seeded from Snapshot Tag: %s\n", srcTag)
				}
				if failTag := tr.Annotations[rescue.AnnotationFailureTag]; failTag != "" {
					fmt.Fprintf(&b, "==> Minted Failure Snapshot Tag: %s\n", failTag)
				}
			} else if tr.Status.PodName != "" {
				fmt.Fprintf(&b, "==> Kubernetes Pod: %s\n", tr.Status.PodName)
			}

			for i, st := range tr.Status.Steps {
				// Try live Step Supervisor log for each step if Actor is still running.
				baseUID := string(tr.UID)
				if baseUID == "" {
					baseUID = tr.Namespace + "-" + tr.Name
				}
				candidateStepID := fmt.Sprintf("%s-r%d-step-%d", sanitizeStepUID(baseUID), len(tr.Status.RetriesStatus), i)
				if s.cfg.RescueService != nil && actorName != "" {
					if stepLog, logErr := s.cfg.RescueService.FetchStepLogs(r.Context(), atespace, actorName, candidateStepID, 0); logErr == nil && stepLog != "" {
						fmt.Fprintf(&b, "\n--- Step %d: %s (%s) ---\n%s\n", i, st.Name, st.Container, strings.TrimRight(stepLog, "\n"))
						continue
					}
				}
				switch {
				case st.Terminated != nil:
					fmt.Fprintf(&b, "\n--- Step %d: %s (container=%s, exitCode=%d, reason=%s) ---\n",
						i, st.Name, st.Container, st.Terminated.ExitCode, st.Terminated.Reason)
					if st.Terminated.Message != "" {
						fmt.Fprintf(&b, "%s\n", st.Terminated.Message)
					}
				case st.Running != nil:
					fmt.Fprintf(&b, "\n--- Step %d: %s (container=%s, state=RUNNING) ---\n", i, st.Name, st.Container)
				default:
					fmt.Fprintf(&b, "\n--- Step %d: %s (container=%s, state=WAITING) ---\n", i, st.Name, st.Container)
				}
			}
			if len(tr.Status.Results) > 0 {
				fmt.Fprintf(&b, "\n==> Emitted Tekton Results:\n")
				for _, res := range tr.Status.Results {
					fmt.Fprintf(&b, "  • %s = %s\n", res.Name, res.Value.StringVal)
				}
			}
			if cond := tr.Status.GetCondition(apis.ConditionSucceeded); cond != nil {
				fmt.Fprintf(&b, "\n==> Condition Succeeded=%s (%s): %s\n", cond.Status, cond.Reason, cond.Message)
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"source":  "taskrun-status",
				"taskRun": tr.Name,
				"logs":    b.String(),
			})
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"source": "empty",
		"logs":   "Select a running or completed task node to inspect its execution trace.",
	})
}

func (s *Server) handleRescueFork(w http.ResponseWriter, r *http.Request) {
	if s.cfg.RescueService == nil {
		writeError(w, http.StatusServiceUnavailable, "Rescue service is not configured")
		return
	}
	var req rescue.ForkRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	if strings.TrimSpace(req.Atespace) == "" {
		req.Atespace = s.cfg.Atespace
	}
	resp, err := s.cfg.RescueService.ForkRescueActor(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// RescueExecAPIRequest wraps Rescue Actor identification and the command to run.
type RescueExecAPIRequest struct {
	Atespace        string   `json:"atespace,omitempty"`
	RescueActorName string   `json:"rescueActorName"`
	Command         []string `json:"command,omitempty"`
	Script          string   `json:"script,omitempty"`
	WorkingDir      string   `json:"workingDir,omitempty"`
}

func (s *Server) handleRescueExec(w http.ResponseWriter, r *http.Request) {
	if s.cfg.RescueService == nil {
		writeError(w, http.StatusServiceUnavailable, "Rescue service is not configured")
		return
	}
	var req RescueExecAPIRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	atespace := strings.TrimSpace(req.Atespace)
	if atespace == "" {
		atespace = s.cfg.Atespace
	}
	resp, err := s.cfg.RescueService.InspectWorkspace(r.Context(), atespace, req.RescueActorName, supervisor.RescueExecRequest{
		Command:    req.Command,
		Script:     req.Script,
		WorkingDir: req.WorkingDir,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// RescuePatchAPIRequest wraps Rescue Actor identification and the hot-patch payload.
type RescuePatchAPIRequest struct {
	Atespace        string            `json:"atespace,omitempty"`
	RescueActorName string            `json:"rescueActorName"`
	Files           map[string]string `json:"files,omitempty"`
	Diff            string            `json:"diff,omitempty"`
	DeleteFiles     []string          `json:"deleteFiles,omitempty"`
}

func (s *Server) handleRescuePatch(w http.ResponseWriter, r *http.Request) {
	if s.cfg.RescueService == nil {
		writeError(w, http.StatusServiceUnavailable, "Rescue service is not configured")
		return
	}
	var req RescuePatchAPIRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	atespace := strings.TrimSpace(req.Atespace)
	if atespace == "" {
		atespace = s.cfg.Atespace
	}
	resp, err := s.cfg.RescueService.ApplyHotPatch(r.Context(), atespace, req.RescueActorName, supervisor.RescuePatchRequest{
		Files:       req.Files,
		Diff:        req.Diff,
		DeleteFiles: req.DeleteFiles,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// RescueCommitAndResumeRequest commits a fixed-* Snapshot Tag from a patched Rescue Actor
// and launches a mid-DAG resumed PipelineRun.
type RescueCommitAndResumeRequest struct {
	Atespace                string   `json:"atespace,omitempty"`
	RescueActorName         string   `json:"rescueActorName"`
	FixedTagName            string   `json:"fixedTagName,omitempty"`
	OriginalPipelineRunName string   `json:"originalPipelineRunName"`
	ResumedPipelineRunName  string   `json:"resumedPipelineRunName,omitempty"`
	SkipTasks               []string `json:"skipTasks,omitempty"`
	ApprovalDecision        string   `json:"approvalDecision,omitempty"`
	DeleteRescueActor       *bool    `json:"deleteRescueActor,omitempty"`
}

func (s *Server) handleRescueCommitAndResume(w http.ResponseWriter, r *http.Request) {
	if s.cfg.RescueService == nil {
		writeError(w, http.StatusServiceUnavailable, "Rescue service is not configured")
		return
	}
	var req RescueCommitAndResumeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	atespace := strings.TrimSpace(req.Atespace)
	if atespace == "" {
		atespace = s.cfg.Atespace
	}
	delActor := true
	if req.DeleteRescueActor != nil {
		delActor = *req.DeleteRescueActor
	}
	tag, err := s.cfg.RescueService.CommitFixedTag(r.Context(), rescue.CommitFixedTagRequest{
		Atespace:          atespace,
		RescueActorName:   req.RescueActorName,
		FixedTagName:      req.FixedTagName,
		DeleteRescueActor: delActor,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	fixedTagName := tag.GetMetadata().GetName()

	approvalDecision := req.ApprovalDecision
	if approvalDecision == "" {
		approvalDecision = "approved"
	}

	resumedPR, err := s.cfg.RescueService.LaunchResumedPipelineRun(r.Context(), rescue.ResumePipelineRequest{
		Namespace:               s.cfg.Namespace,
		OriginalPipelineRunName: req.OriginalPipelineRunName,
		ResumedPipelineRunName:  req.ResumedPipelineRunName,
		FixedTagName:            fixedTagName,
		SkipTasks:               req.SkipTasks,
		ApprovalDecision:        approvalDecision,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"fixedTag":           fixedTagName,
		"resumedPipelineRun": resumedPR.Name,
		"skipTasks":          resumedPR.Spec.Workspaces[0].Actor.SkipTasks,
	})
}

func (s *Server) handleSuspendActor(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ControlClient == nil {
		writeError(w, http.StatusServiceUnavailable, "ControlClient is not configured")
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "actor name is required")
		return
	}
	resp, err := s.cfg.ControlClient.SuspendActor(r.Context(), &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: s.cfg.Atespace, Name: name},
		Options: &ateapi.SuspendOptions{
			Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
		},
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"actor": name,
		"state": resp.GetActor().GetStatus().GetState().String(),
	})
}

func sanitizeStepUID(base string) string {
	var b strings.Builder
	for i := 0; i < len(base); i++ {
		ch := base[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			b.WriteByte(ch)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"internal marshal error"}`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(code)
	_, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
