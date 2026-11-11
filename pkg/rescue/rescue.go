package rescue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	"github.com/gke-demos/tekton-actors/pkg/supervisor"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonclient "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	AnnotationFailureTag          = "tekton.ate.dev/failure-tag"
	AnnotationFailedActor         = "tekton.ate.dev/failed-actor"
	AnnotationActorTemplate       = "tekton.ate.dev/actor-template"
	AnnotationSourceTag           = "tekton.ate.dev/source-tag"
	AnnotationResumedFromPipeline = "tekton.ate.dev/resumed-from-pipelinerun"
	AnnotationFailureOriginTask   = "tekton.ate.dev/failure-origin-taskrun"
)

// EndpointResolver returns the base URL and HTTP client for talking to an
// Actor's Step Supervisor coordinator (:8080).
type EndpointResolver func(atespace, actorName string) (baseURL string, client *http.Client)

// Service orchestrates Rescue Actor forking from failure Snapshot Tags,
// workspace inspection, hot-patching, fixed Snapshot Tag minting, and mid-DAG
// PipelineRun resumption.
type Service struct {
	control      ateapi.ControlClient
	tektonClient tektonclient.Interface
	endpoints    EndpointResolver
}

// NewService constructs a Rescue Service.
func NewService(control ateapi.ControlClient, tektonClient tektonclient.Interface, endpoints EndpointResolver) *Service {
	return &Service{
		control:      control,
		tektonClient: tektonClient,
		endpoints:    endpoints,
	}
}

// ForkRequest describes the parameters for ForkRescueActor.
type ForkRequest struct {
	Atespace        string `json:"atespace"`
	FailureTag      string `json:"failureTag"`
	RescueActorName string `json:"rescueActorName,omitempty"`
	TemplateName    string `json:"templateName,omitempty"`
}

// ForkResponse describes the running Rescue Actor forked from a failure Snapshot Tag.
type ForkResponse struct {
	Atespace        string `json:"atespace"`
	RescueActorName string `json:"rescueActorName"`
	FailureTag      string `json:"failureTag"`
	TemplateName    string `json:"templateName"`
	State           string `json:"state"`
	WorkerPod       string `json:"workerPod,omitempty"`
}

// ForkRescueActor creates (or reuses) a live Rescue Actor seeded from FailureTag
// and resumes it to ACTOR_STATE_RUNNING so operators can inspect or patch /workspace.
func (s *Service) ForkRescueActor(ctx context.Context, req ForkRequest) (*ForkResponse, error) {
	if s.control == nil {
		return nil, fmt.Errorf("Substrate ControlClient is not configured")
	}
	atespace := strings.TrimSpace(req.Atespace)
	failureTag := strings.TrimSpace(req.FailureTag)
	if atespace == "" || failureTag == "" {
		return nil, fmt.Errorf("atespace and failureTag are required")
	}
	rescueName := strings.TrimSpace(req.RescueActorName)
	if rescueName == "" {
		rescueName = sanitizeDNSName("rescue", strings.TrimPrefix(failureTag, "failure-"))
	}

	tag, err := s.control.GetTag(ctx, &ateapi.GetTagRequest{
		Tag: &ateapi.ObjectRef{Atespace: atespace, Name: failureTag},
	})
	if err != nil {
		return nil, fmt.Errorf("get failure tag %s/%s: %w", atespace, failureTag, err)
	}

	tmplName := strings.TrimSpace(req.TemplateName)
	if tmplName == "" {
		var findErr error
		tmplName, findErr = s.findTemplateNameByUID(ctx, atespace, tag.GetStatus().GetActorTemplateUid())
		if findErr != nil {
			return nil, findErr
		}
	}

	actor, err := s.control.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: rescueName},
	})
	if status.Code(err) == codes.NotFound {
		actor, err = s.control.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{
					Atespace: atespace,
					Name:     rescueName,
				},
				ActorTemplate: &ateapi.ObjectRef{
					Atespace: atespace,
					Name:     tmplName,
				},
				SourceTag: &ateapi.ObjectRef{
					Atespace: atespace,
					Name:     failureTag,
				},
			},
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("create rescue actor %s/%s: %w", atespace, rescueName, err)
		}
		if status.Code(err) == codes.AlreadyExists {
			actor, err = s.control.GetActor(ctx, &ateapi.GetActorRequest{
				Actor: &ateapi.ObjectRef{Atespace: atespace, Name: rescueName},
			})
		}
	}
	if err != nil {
		return nil, fmt.Errorf("get rescue actor %s/%s: %w", atespace, rescueName, err)
	}

	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		resResp, resErr := s.control.ResumeActor(ctx, &ateapi.ResumeActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: rescueName},
		})
		if resErr != nil {
			return nil, fmt.Errorf("resume rescue actor %s/%s: %w", atespace, rescueName, resErr)
		}
		actor = resResp.GetActor()
	}

	return &ForkResponse{
		Atespace:        atespace,
		RescueActorName: rescueName,
		FailureTag:      failureTag,
		TemplateName:    actor.GetActorTemplate().GetName(),
		State:           actor.GetStatus().GetState().String(),
		WorkerPod:       actor.GetStatus().GetWorkerAssignment().GetWorkerPod(),
	}, nil
}

// InspectWorkspace runs a command inside a live Rescue Actor's /workspace via
// Step Supervisor POST /v1/rescue/exec.
func (s *Service) InspectWorkspace(
	ctx context.Context,
	atespace, rescueActorName string,
	req supervisor.RescueExecRequest,
) (*supervisor.RescueExecResponse, error) {
	var out supervisor.RescueExecResponse
	if err := s.doActorJSON(ctx, atespace, rescueActorName, http.MethodPost, "/v1/rescue/exec", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ApplyHotPatch applies file replacements, deletions, or a unified diff to a
// live Rescue Actor's /workspace via Step Supervisor POST /v1/rescue/patch.
// When req specifies no files, diff, or deletions, it defaults to
// gitoci.SampleAppWorkspaceFixDiff (the fulfillment-api pricing discount fix).
func (s *Service) ApplyHotPatch(
	ctx context.Context,
	atespace, rescueActorName string,
	req supervisor.RescuePatchRequest,
) (*supervisor.RescuePatchResponse, error) {
	if len(req.Files) == 0 && strings.TrimSpace(req.Diff) == "" && len(req.DeleteFiles) == 0 {
		req.Diff = gitoci.SampleAppWorkspaceFixDiff
	}
	var out supervisor.RescuePatchResponse
	if err := s.doActorJSON(ctx, atespace, rescueActorName, http.MethodPost, "/v1/rescue/patch", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FetchStepLogs fetches step logs from an Actor's Step Supervisor GET /v1/steps/{id}/logs.
func (s *Service) FetchStepLogs(ctx context.Context, atespace, actorName, stepID string, offset int64) (string, error) {
	if s.endpoints == nil {
		return "", fmt.Errorf("actor endpoint resolver is not configured")
	}
	baseURL, client := s.endpoints(atespace, actorName)
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	u := fmt.Sprintf("%s/v1/steps/%s/logs?offset=%s", strings.TrimRight(baseURL, "/"), url.PathEscape(stepID), strconv.FormatInt(offset, 10))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch step logs %s/%s/%s status %d: %s", atespace, actorName, stepID, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// CommitFixedTagRequest describes the parameters for CommitFixedTag.
type CommitFixedTagRequest struct {
	Atespace          string `json:"atespace"`
	RescueActorName   string `json:"rescueActorName"`
	FixedTagName      string `json:"fixedTagName,omitempty"`
	DeleteRescueActor bool   `json:"deleteRescueActor"`
}

// CommitFixedTag synchronously suspends the patched Rescue Actor, mints an
// immutable fixed Snapshot Tag (`fixed-*`), and optionally deletes the Rescue Actor.
func (s *Service) CommitFixedTag(ctx context.Context, req CommitFixedTagRequest) (*ateapi.Tag, error) {
	if s.control == nil {
		return nil, fmt.Errorf("Substrate ControlClient is not configured")
	}
	atespace := strings.TrimSpace(req.Atespace)
	rescueName := strings.TrimSpace(req.RescueActorName)
	if atespace == "" || rescueName == "" {
		return nil, fmt.Errorf("atespace and rescueActorName are required")
	}
	fixedTag := strings.TrimSpace(req.FixedTagName)
	if fixedTag == "" {
		fixedTag = sanitizeDNSName("fixed", strings.TrimPrefix(rescueName, "rescue-"))
	}

	_, err := s.control.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: atespace, Name: rescueName},
		Options: &ateapi.SuspendOptions{
			Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("suspend rescue actor %s/%s: %w", atespace, rescueName, err)
	}

	tag, err := s.control.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: atespace,
				Name:     fixedTag,
			},
			Scope: ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{
				Atespace: atespace,
				Name:     rescueName,
			},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		tag, err = s.control.GetTag(ctx, &ateapi.GetTagRequest{
			Tag: &ateapi.ObjectRef{Atespace: atespace, Name: fixedTag},
		})
	}
	if err != nil {
		return nil, fmt.Errorf("create fixed tag %s/%s: %w", atespace, fixedTag, err)
	}

	if req.DeleteRescueActor {
		_, delErr := s.control.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: rescueName},
		})
		if delErr != nil && status.Code(delErr) != codes.NotFound {
			return nil, fmt.Errorf("delete rescue actor %s/%s: %w", atespace, rescueName, delErr)
		}
	}

	return tag, nil
}

// ResumePipelineRequest describes the parameters for LaunchResumedPipelineRun.
type ResumePipelineRequest struct {
	Namespace               string   `json:"namespace"`
	OriginalPipelineRunName string   `json:"originalPipelineRunName"`
	ResumedPipelineRunName  string   `json:"resumedPipelineRunName,omitempty"`
	FixedTagName            string   `json:"fixedTagName"`
	SkipTasks               []string `json:"skipTasks,omitempty"`
	ApprovalDecision        string   `json:"approvalDecision,omitempty"`
}

// LaunchResumedPipelineRun creates a new PipelineRun cloned from OriginalPipelineRunName
// whose Actor Workspace Binding points to `sourceTag: FixedTagName` and `skipTasks: SkipTasks`
// (defaulting to `["clone-and-build"]`) so execution resumes mid-DAG with the patched workspace.
func (s *Service) LaunchResumedPipelineRun(ctx context.Context, req ResumePipelineRequest) (*v1.PipelineRun, error) {
	if s.tektonClient == nil {
		return nil, fmt.Errorf("Tekton client is not configured")
	}
	ns := strings.TrimSpace(req.Namespace)
	origName := strings.TrimSpace(req.OriginalPipelineRunName)
	fixedTag := strings.TrimSpace(req.FixedTagName)
	if ns == "" || origName == "" || fixedTag == "" {
		return nil, fmt.Errorf("namespace, originalPipelineRunName, and fixedTagName are required")
	}

	origPR, err := s.tektonClient.TektonV1().PipelineRuns(ns).Get(ctx, origName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get original PipelineRun %s/%s: %w", ns, origName, err)
	}

	skipTasks := req.SkipTasks
	if len(skipTasks) == 0 {
		skipTasks = []string{"clone-and-build"}
	}

	newSpec := origPR.Spec.DeepCopy()
	newSpec.Status = ""
	foundActorWS := false
	for i := range newSpec.Workspaces {
		if newSpec.Workspaces[i].Actor != nil {
			newSpec.Workspaces[i].Actor.SourceTag = fixedTag
			newSpec.Workspaces[i].Actor.SkipTasks = append([]string(nil), skipTasks...)
			foundActorWS = true
		}
	}
	if !foundActorWS {
		return nil, fmt.Errorf("original PipelineRun %s/%s has no Actor workspace binding", ns, origName)
	}

	for i := range newSpec.Params {
		switch newSpec.Params[i].Name {
		case "inject-bug":
			newSpec.Params[i].Value = *v1.NewStructuredValues("false")
		case "approval-decision":
			if req.ApprovalDecision != "" {
				newSpec.Params[i].Value = *v1.NewStructuredValues(req.ApprovalDecision)
			}
		}
	}

	resumedName := strings.TrimSpace(req.ResumedPipelineRunName)
	if resumedName == "" {
		resumedName = sanitizeDNSName(origName, fmt.Sprintf("resumed-%d", time.Now().Unix()%100000))
	}

	labels := make(map[string]string, len(origPR.Labels)+1)
	for k, v := range origPR.Labels {
		labels[k] = v
	}
	labels["tekton.ate.dev/resumed"] = "true"

	annotations := map[string]string{
		AnnotationResumedFromPipeline: origPR.Name,
		AnnotationSourceTag:           fixedTag,
	}
	if origPR.Annotations["tekton.ate.dev/retain-workspace-actor"] == "true" {
		annotations["tekton.ate.dev/retain-workspace-actor"] = "true"
	}

	resumedPR := &v1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        resumedName,
			Namespace:   ns,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *newSpec,
	}

	created, err := s.tektonClient.TektonV1().PipelineRuns(ns).Create(ctx, resumedPR, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create resumed PipelineRun %s/%s: %w", ns, resumedName, err)
	}
	return created, nil
}

func (s *Service) findTemplateNameByUID(ctx context.Context, atespace, uid string) (string, error) {
	pageToken := ""
	var firstName string
	for {
		resp, err := s.control.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{
			Atespace: atespace,
			Options:  &ateapi.ListOptions{PageSize: 200, PageToken: pageToken},
		})
		if err != nil {
			return "", fmt.Errorf("list actor templates in %q: %w", atespace, err)
		}
		for _, t := range resp.GetActorTemplates() {
			if firstName == "" {
				firstName = t.GetMetadata().GetName()
			}
			if uid != "" && t.GetMetadata().GetUid() == uid {
				return t.GetMetadata().GetName(), nil
			}
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	if uid == "" && firstName != "" {
		return firstName, nil
	}
	return "", fmt.Errorf("ActorTemplate UID %q not found in atespace %q", uid, atespace)
}

func (s *Service) doActorJSON(
	ctx context.Context,
	atespace, actorName, method, path string,
	reqBody any,
	respDest any,
) error {
	if s.endpoints == nil {
		return fmt.Errorf("actor endpoint resolver is not configured")
	}
	baseURL, client := s.endpoints(atespace, actorName)
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	targetURL := strings.TrimRight(baseURL, "/") + path
	httpReq, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s %s on Actor %s/%s: %w", method, path, atespace, actorName, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s %s on Actor %s/%s returned %d: %s", method, path, atespace, actorName, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if respDest != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respDest); err != nil {
			return fmt.Errorf("decode %s response: %w", path, err)
		}
	}
	return nil
}

func sanitizeDNSName(prefix, raw string) string {
	var b strings.Builder
	s := strings.ToLower(strings.TrimSpace(prefix + "-" + raw))
	lastHyphen := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
			lastHyphen = false
		} else if !lastHyphen && b.Len() > 0 {
			b.WriteByte('-')
			lastHyphen = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	if out == "" {
		return "actor"
	}
	return out
}
