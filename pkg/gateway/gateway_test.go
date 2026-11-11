package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gke-demos/tekton-actors/pkg/approvalgate"
	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	"github.com/gke-demos/tekton-actors/pkg/rescue"
	"github.com/gke-demos/tekton-actors/pkg/supervisor"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	faketekton "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakekube "k8s.io/client-go/kubernetes/fake"
)

type fakeControlClient struct {
	ateapi.ControlClient
	mu        sync.Mutex
	templates map[string]*ateapi.ActorTemplate
	actors    map[string]*ateapi.Actor
	tags      map[string]*ateapi.Tag
}

func newFakeControlClient() *fakeControlClient {
	return &fakeControlClient{
		templates: make(map[string]*ateapi.ActorTemplate),
		actors:    make(map[string]*ateapi.Actor),
		tags:      make(map[string]*ateapi.Tag),
	}
}

func (f *fakeControlClient) ListActorTemplates(_ context.Context, req *ateapi.ListActorTemplatesRequest, _ ...grpc.CallOption) (*ateapi.ListActorTemplatesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var list []*ateapi.ActorTemplate
	for _, t := range f.templates {
		if req.GetAtespace() == "" || t.GetMetadata().GetAtespace() == req.GetAtespace() {
			list = append(list, proto.CloneOf(t))
		}
	}
	return &ateapi.ListActorTemplatesResponse{ActorTemplates: list}, nil
}

func (f *fakeControlClient) GetTag(_ context.Context, req *ateapi.GetTagRequest, _ ...grpc.CallOption) (*ateapi.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetTag().GetAtespace() + "/" + req.GetTag().GetName()
	tag, ok := f.tags[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "tag not found")
	}
	return proto.CloneOf(tag), nil
}

func (f *fakeControlClient) CreateTag(_ context.Context, req *ateapi.CreateTagRequest, _ ...grpc.CallOption) (*ateapi.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetTag().GetMetadata().GetAtespace() + "/" + req.GetTag().GetMetadata().GetName()
	if _, exists := f.tags[key]; exists {
		return nil, status.Error(codes.AlreadyExists, "tag exists")
	}
	stored := proto.CloneOf(req.GetTag())
	stored.Status = &ateapi.TagStatus{
		ActorTemplateUid: "uid-tmpl-unit-test",
		Snapshot: &ateapi.Snapshot{
			Uuid: "snap-" + req.GetTag().GetMetadata().GetName(),
		},
	}
	f.tags[key] = stored
	return proto.CloneOf(stored), nil
}

func (f *fakeControlClient) ListTags(_ context.Context, req *ateapi.ListTagsRequest, _ ...grpc.CallOption) (*ateapi.ListTagsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var list []*ateapi.Tag
	for _, t := range f.tags {
		if req.GetAtespace() == "" || t.GetMetadata().GetAtespace() == req.GetAtespace() {
			list = append(list, proto.CloneOf(t))
		}
	}
	return &ateapi.ListTagsResponse{Tags: list}, nil
}

func (f *fakeControlClient) GetActor(_ context.Context, req *ateapi.GetActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	a, ok := f.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	return proto.CloneOf(a), nil
}

func (f *fakeControlClient) CreateActor(_ context.Context, req *ateapi.CreateActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetActor().GetMetadata().GetAtespace() + "/" + req.GetActor().GetMetadata().GetName()
	if _, exists := f.actors[key]; exists {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	stored := proto.CloneOf(req.GetActor())
	stored.Status = &ateapi.ActorStatus{
		State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		LastAssignedGeneration: 1,
	}
	f.actors[key] = stored
	return proto.CloneOf(stored), nil
}

func (f *fakeControlClient) ListActors(_ context.Context, req *ateapi.ListActorsRequest, _ ...grpc.CallOption) (*ateapi.ListActorsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var list []*ateapi.Actor
	for _, a := range f.actors {
		if req.GetAtespace() == "" || a.GetMetadata().GetAtespace() == req.GetAtespace() {
			list = append(list, proto.CloneOf(a))
		}
	}
	return &ateapi.ListActorsResponse{Actors: list}, nil
}

func (f *fakeControlClient) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	a, ok := f.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	a.Status = &ateapi.ActorStatus{
		State:                  ateapi.ActorState_ACTOR_STATE_RUNNING,
		WorkerAssignment:       &ateapi.WorkerAssignment{WorkerPod: "ate-worker-0"},
		LastAssignedGeneration: 1,
	}
	return &ateapi.ResumeActorResponse{Actor: proto.CloneOf(a)}, nil
}

func (f *fakeControlClient) SuspendActor(_ context.Context, req *ateapi.SuspendActorRequest, _ ...grpc.CallOption) (*ateapi.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	a, ok := f.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	a.Status = &ateapi.ActorStatus{
		State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		LastAssignedGeneration: 2,
	}
	return &ateapi.SuspendActorResponse{Actor: proto.CloneOf(a)}, nil
}

func (f *fakeControlClient) DeleteActor(_ context.Context, req *ateapi.DeleteActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	a := f.actors[key]
	delete(f.actors, key)
	return proto.CloneOf(a), nil
}

func containsEmoji(s string) bool {
	for _, r := range s {
		switch {
		case r >= 0x1F300 && r <= 0x1FAFF:
			return true
		case r >= 0x2600 && r <= 0x27BF:
			return true
		case r == 0xFE0F:
			return true
		}
	}
	return false
}

func TestGateway_UIRoutesAndProjectorToggleAndZeroEmojis(t *testing.T) {
	kubeClient := fakekube.NewSimpleClientset()
	tektonClient := faketekton.NewSimpleClientset()
	controlClient := newFakeControlClient()

	srv := NewServer(Config{
		Namespace:     "tekton-actors",
		Atespace:      "tekton-actors",
		KubeClient:    kubeClient,
		TektonClient:  tektonClient,
		ControlClient: controlClient,
	})

	routes := []string{"/", "/compare", "/rescue", "/about"}
	for _, route := range routes {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", route, rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, `id="theme-dark-btn"`) || !strings.Contains(body, `id="theme-light-btn"`) {
			t.Fatalf("GET %s missing Projector Light Mode segmented icon toggle buttons", route)
		}
		if !strings.Contains(body, `<svg`) {
			t.Fatalf("GET %s missing inline SVG icons", route)
		}
		if containsEmoji(body) {
			t.Fatalf("GET %s contains disallowed emoji characters", route)
		}
	}
}

func TestGateway_RESTStateAndPipelineRunAndApprovalAndRescue(t *testing.T) {
	ctx := context.Background()

	// Start a real in-memory Step Supervisor coordinator daemon for rescue exec/patch verification.
	wsDir := filepath.Join(t.TempDir(), "workspace")
	tektonDir := filepath.Join(t.TempDir(), "tekton")
	if err := gitoci.WriteSampleApp(filepath.Join(wsDir, "source", "app"), true); err != nil {
		t.Fatalf("WriteSampleApp: %v", err)
	}
	supServer, err := supervisor.NewServer(supervisor.Config{
		Role:         supervisor.RoleCoordinator,
		WorkspaceDir: wsDir,
		TektonDir:    tektonDir,
		ResultsDir:   filepath.Join(tektonDir, "results"),
	})
	if err != nil {
		t.Fatalf("supervisor.NewServer: %v", err)
	}
	supSrv := httptest.NewServer(supServer.Handler())
	defer supSrv.Close()

	// Start in-memory git-oci server so /api/state can query audit summary.
	gitOCISrv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: "test-token",
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	ociHTTP := httptest.NewServer(gitOCISrv.Handler())
	defer ociHTTP.Close()

	workerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ate-worker-0",
			Namespace: "tekton-actors",
			Labels:    map[string]string{"ate.dev/pool": "tekton-actors-pool"},
		},
		Spec: corev1.PodSpec{
			NodeName: "gke-node-0",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	gateCR := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "release-actor-1-release-gate",
			Namespace: "tekton-actors",
			Labels: map[string]string{
				"tekton.dev/pipelineRun":  "release-actor-1",
				"tekton.dev/pipelineTask": "release-gate",
			},
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       v1beta1.TaskKind(approvalgate.Kind),
			},
		},
	}

	kubeClient := fakekube.NewSimpleClientset(workerPod)
	tektonClient := faketekton.NewSimpleClientset(gateCR)
	controlClient := newFakeControlClient()
	controlClient.templates["tekton-actors/tmpl-unit-test"] = &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "tekton-actors",
			Name:     "tmpl-unit-test",
			Uid:      "uid-tmpl-unit-test",
		},
	}
	_, _ = controlClient.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: "tekton-actors",
				Name:     "failure-release-buggy-unit-test",
			},
			SourceActor: &ateapi.ObjectRef{
				Atespace: "tekton-actors",
				Name:     "br-release-buggy-unit-test",
			},
		},
	})

	rescueSvc := rescue.NewService(controlClient, tektonClient, func(_, _ string) (string, *http.Client) {
		return supSrv.URL, supSrv.Client()
	})

	srv := NewServer(Config{
		Namespace:     "tekton-actors",
		Atespace:      "tekton-actors",
		GitOCIURL:     ociHTTP.URL,
		KubeClient:    kubeClient,
		TektonClient:  tektonClient,
		ControlClient: controlClient,
		RescueService: rescueSvc,
	})

	// 1. Trigger side-by-side (both) PipelineRuns
	launchBody := bytes.NewBufferString(`{"mode":"both","injectBug":false,"requireApproval":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/pipelineruns", launchBody)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/pipelineruns status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var launchResp struct {
		Created []string `json:"created"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&launchResp); err != nil {
		t.Fatalf("decode launch response: %v", err)
	}
	if len(launchResp.Created) != 2 {
		t.Fatalf("created pipelineruns = %v, want 2", launchResp.Created)
	}
	actorPRName := launchResp.Created[0]

	// 2. Approve the pending ApprovalGate CustomRun
	approveBody := bytes.NewBufferString(`{"decision":"approved","approver":"operator","note":"Verified zero-slot pause"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/approvals/release-actor-1-release-gate", approveBody)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/approvals status = %d, body = %s", rec.Code, rec.Body.String())
	}

	updatedGate, err := tektonClient.TektonV1beta1().CustomRuns("tekton-actors").Get(ctx, "release-actor-1-release-gate", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated gate: %v", err)
	}
	if updatedGate.Annotations[approvalgate.AnnotationDecision] != approvalgate.DecisionApproved {
		t.Fatalf("gate decision = %q, want approved", updatedGate.Annotations[approvalgate.AnnotationDecision])
	}

	// 3. Rescue fork -> patch -> exec -> commit-and-resume
	forkBody := bytes.NewBufferString(`{"failureTag":"failure-release-buggy-unit-test","rescueActorName":"rescue-unit-test-1"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/rescue/fork", forkBody)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/rescue/fork status = %d, body = %s", rec.Code, rec.Body.String())
	}

	patchBody := bytes.NewBufferString(`{"rescueActorName":"rescue-unit-test-1","files":{"source/internal/pricing/pricing.go":"package pricing\n"}}`)
	req = httptest.NewRequest(http.MethodPost, "/api/rescue/patch", patchBody)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/rescue/patch status = %d, body = %s", rec.Code, rec.Body.String())
	}

	execBody := bytes.NewBufferString(`{"rescueActorName":"rescue-unit-test-1","script":"cat source/internal/pricing/pricing.go"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/rescue/exec", execBody)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/rescue/exec status = %d, body = %s", rec.Code, rec.Body.String())
	}

	commitBody := bytes.NewBufferString(fmt.Sprintf(
		`{"rescueActorName":"rescue-unit-test-1","fixedTagName":"fixed-release-buggy-unit-test","originalPipelineRunName":%q,"skipTasks":["clone-and-build"]}`,
		actorPRName,
	))
	req = httptest.NewRequest(http.MethodPost, "/api/rescue/commit-and-resume", commitBody)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/rescue/commit-and-resume status = %d, body = %s", rec.Code, rec.Body.String())
	}

	// 4. GET /api/state and verify aggregated response
	req = httptest.NewRequest(http.MethodGet, "/api/state", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/state status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var state StateSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&state); err != nil {
		t.Fatalf("decode state response: %v", err)
	}
	if len(state.PipelineRuns) != 3 {
		t.Fatalf("state.PipelineRuns len = %d, want 3 (actor + pod-pvc + resumed)", len(state.PipelineRuns))
	}
	if len(state.Substrate.WorkerPods) != 1 {
		t.Fatalf("state.Substrate.WorkerPods len = %d, want 1", len(state.Substrate.WorkerPods))
	}
	if len(state.Substrate.Tags) < 2 {
		t.Fatalf("state.Substrate.Tags len = %d, want >= 2 (failure-* and fixed-*)", len(state.Substrate.Tags))
	}

	// Verify the resumed PipelineRun has skipTasks=["clone-and-build"] and sourceTag="fixed-release-buggy-unit-test"
	var foundResumed *v1.PipelineRun
	allPRs, _ := tektonClient.TektonV1().PipelineRuns("tekton-actors").List(ctx, metav1.ListOptions{})
	for i := range allPRs.Items {
		pr := &allPRs.Items[i]
		for _, wb := range pr.Spec.Workspaces {
			if wb.Actor != nil && wb.Actor.SourceTag == "fixed-release-buggy-unit-test" {
				foundResumed = pr
			}
		}
	}
	if foundResumed == nil {
		t.Fatalf("expected resumed PipelineRun with sourceTag=fixed-release-buggy-unit-test")
	}
	if fmt.Sprintf("%v", foundResumed.Spec.Workspaces[0].Actor.SkipTasks) != "[clone-and-build]" {
		t.Fatalf("resumed skipTasks = %v, want [clone-and-build]", foundResumed.Spec.Workspaces[0].Actor.SkipTasks)
	}
	_ = runtime.SchemeBuilder{}
}
