package rescue_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	"github.com/gke-demos/tekton-actors/pkg/rescue"
	"github.com/gke-demos/tekton-actors/pkg/supervisor"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	faketekton "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		if t.GetMetadata().GetAtespace() == req.GetAtespace() {
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
	stored.Status = &ateapi.TagStatus{ActorTemplateUid: "uid-tmpl-unit-test"}
	f.tags[key] = stored
	return proto.CloneOf(stored), nil
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
		State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
	}
	f.actors[key] = stored
	return proto.CloneOf(stored), nil
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
		State:            ateapi.ActorState_ACTOR_STATE_RUNNING,
		WorkerAssignment: &ateapi.WorkerAssignment{WorkerPod: "ate-worker-rescue-0"},
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
		State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
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

func TestRescueService_ForkInspectPatchCommitAndResume(t *testing.T) {
	ctx := context.Background()
	fakeCtrl := newFakeControlClient()

	const atespace = "tekton-actors"
	fakeCtrl.templates[atespace+"/tmpl-unit-test"] = &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: atespace,
			Name:     "tmpl-unit-test",
			Uid:      "uid-tmpl-unit-test",
		},
	}
	fakeCtrl.tags[atespace+"/failure-unit-test-1"] = &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: atespace, Name: "failure-unit-test-1"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: atespace, Name: "br-unit-test"},
		Status:      &ateapi.TagStatus{ActorTemplateUid: "uid-tmpl-unit-test"},
	}

	// Stand up a real Step Supervisor server rooted at a scratch /workspace with the buggy sample app.
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

	origPR := &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "release-actor-buggy",
			Namespace: "tekton-actors",
			Labels:    map[string]string{"tekton.dev/pipeline": "release-pipeline"},
		},
		Spec: v1.PipelineRunSpec{
			PipelineRef: &v1.PipelineRef{Name: "release-pipeline"},
			Params: []v1.Param{
				{Name: "inject-bug", Value: *v1.NewStructuredValues("true")},
				{Name: "approval-decision", Value: *v1.NewStructuredValues("")},
			},
			Workspaces: []v1.WorkspaceBinding{{
				Name: "source",
				Actor: &v1.ActorWorkspaceSource{
					Atespace:     atespace,
					EgressSecret: "git-oci-auth/token",
				},
			}},
		},
	}
	tektonClient := faketekton.NewSimpleClientset(origPR)

	svc := rescue.NewService(fakeCtrl, tektonClient, func(_, _ string) (string, *http.Client) {
		return supSrv.URL, supSrv.Client()
	})

	// 2. Fork Rescue Actor from failure-unit-test-1.
	forked, err := svc.ForkRescueActor(ctx, rescue.ForkRequest{
		Atespace:   atespace,
		FailureTag: "failure-unit-test-1",
	})
	if err != nil {
		t.Fatalf("ForkRescueActor: %v", err)
	}
	if forked.RescueActorName != "rescue-unit-test-1" || forked.State != "ACTOR_STATE_RUNNING" {
		t.Fatalf("unexpected ForkResponse: %+v", forked)
	}

	// 3. Inspect workspace before patching.
	inspectRes, err := svc.InspectWorkspace(ctx, atespace, forked.RescueActorName, supervisor.RescueExecRequest{
		Command:    []string{"/bin/sh", "-c", "cat source/app/pricing.go"},
		WorkingDir: wsDir,
	})
	if err != nil {
		t.Fatalf("InspectWorkspace: %v", err)
	}
	if !strings.Contains(inspectRes.Output, "subtotalCents - discountCents - 1") {
		t.Fatalf("expected buggy 'subtotalCents - discountCents - 1' in pricing.go before patch, got:\n%s", inspectRes.Output)
	}

	// 4. Apply default 1-click hot-patch (SampleAppWorkspaceFixDiff) and verify pricing.go is fixed.
	patchRes, err := svc.ApplyHotPatch(ctx, atespace, forked.RescueActorName, supervisor.RescuePatchRequest{})
	if err != nil {
		t.Fatalf("ApplyHotPatch: %v", err)
	}
	if len(patchRes.PatchedFiles) != 1 || patchRes.PatchedFiles[0] != "source/app/pricing.go" {
		t.Fatalf("unexpected PatchedFiles: %+v", patchRes.PatchedFiles)
	}

	fixedBytes, err := os.ReadFile(filepath.Join(wsDir, "source", "app", "pricing.go"))
	if err != nil {
		t.Fatalf("read patched pricing.go: %v", err)
	}
	if strings.Contains(string(fixedBytes), "subtotalCents - discountCents - 1") || !strings.Contains(string(fixedBytes), "return subtotalCents - discountCents") {
		t.Fatalf("expected patched pricing.go to restore exact discount subtraction, got:\n%s", string(fixedBytes))
	}

	// 5. Commit fixed-* Snapshot Tag and delete Rescue Actor.
	fixedTag, err := svc.CommitFixedTag(ctx, rescue.CommitFixedTagRequest{
		Atespace:          atespace,
		RescueActorName:   forked.RescueActorName,
		FixedTagName:      "fixed-unit-test-1",
		DeleteRescueActor: true,
	})
	if err != nil {
		t.Fatalf("CommitFixedTag: %v", err)
	}
	if fixedTag.GetMetadata().GetName() != "fixed-unit-test-1" {
		t.Fatalf("unexpected fixed tag name: %q", fixedTag.GetMetadata().GetName())
	}

	// 6. Launch resumed PipelineRun seeded from fixed-unit-test-1 and skipping clone-and-build.
	resumedPR, err := svc.LaunchResumedPipelineRun(ctx, rescue.ResumePipelineRequest{
		Namespace:               "tekton-actors",
		OriginalPipelineRunName: "release-actor-buggy",
		ResumedPipelineRunName:  "release-actor-resumed",
		FixedTagName:            "fixed-unit-test-1",
		ApprovalDecision:        "approved",
	})
	if err != nil {
		t.Fatalf("LaunchResumedPipelineRun: %v", err)
	}
	actorSrc := resumedPR.Spec.Workspaces[0].Actor
	if actorSrc == nil || actorSrc.SourceTag != "fixed-unit-test-1" {
		t.Fatalf("expected resumed PipelineRun Actor SourceTag=fixed-unit-test-1, got %+v", actorSrc)
	}
	if len(actorSrc.SkipTasks) != 1 || actorSrc.SkipTasks[0] != "clone-and-build" {
		t.Fatalf("expected SkipTasks=[clone-and-build], got %+v", actorSrc.SkipTasks)
	}
	for _, p := range resumedPR.Spec.Params {
		if p.Name == "inject-bug" && p.Value.StringVal != "false" {
			t.Fatalf("expected resumed PipelineRun inject-bug=false, got %q", p.Value.StringVal)
		}
		if p.Name == "approval-decision" && p.Value.StringVal != "approved" {
			t.Fatalf("expected resumed PipelineRun approval-decision=approved, got %q", p.Value.StringVal)
		}
	}
}
