package gitoci_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/approvalgate"
	"github.com/gke-demos/tekton-actors/pkg/gitoci"
	tektoncfg "github.com/tektoncd/pipeline/pkg/apis/config"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func tektonValidationContext(t *testing.T) context.Context {
	t.Helper()
	flags, err := tektoncfg.NewFeatureFlagsFromMap(map[string]string{
		"enable-custom-tasks": "true",
		"enable-api-fields":   "beta",
	})
	if err != nil {
		t.Fatalf("NewFeatureFlagsFromMap: %v", err)
	}
	defaults, err := tektoncfg.NewDefaultsFromMap(map[string]string{})
	if err != nil {
		t.Fatalf("NewDefaultsFromMap: %v", err)
	}
	cfg := &tektoncfg.Config{
		Defaults:     defaults,
		FeatureFlags: flags,
	}
	return tektoncfg.ToContext(context.Background(), cfg)
}

func loadTasksAndPipeline(t *testing.T) (map[string]*tektonv1.Task, *tektonv1.Pipeline) {
	t.Helper()
	ctx := tektonValidationContext(t)
	pipelineDir := filepath.Join("..", "..", "manifests", "pipeline")
	entries, err := os.ReadDir(pipelineDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", pipelineDir, err)
	}

	tasks := make(map[string]*tektonv1.Task)
	var pipeline *tektonv1.Pipeline

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		fullPath := filepath.Join(pipelineDir, entry.Name())
		raw, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", fullPath, err)
		}
		var meta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		if err := yaml.Unmarshal(raw, &meta); err != nil {
			t.Fatalf("parse header %s: %v", fullPath, err)
		}
		if meta.APIVersion != "tekton.dev/v1" {
			t.Fatalf("%s apiVersion = %q, want tekton.dev/v1", fullPath, meta.APIVersion)
		}

		switch meta.Kind {
		case "Task":
			var task tektonv1.Task
			if err := yaml.UnmarshalStrict(raw, &task); err != nil {
				t.Fatalf("UnmarshalStrict Task %s: %v", fullPath, err)
			}
			task.SetDefaults(ctx)
			if errs := task.Validate(ctx); errs != nil {
				t.Fatalf("Task %s (%s) failed Tekton v1 Validate: %v", task.Name, fullPath, errs)
			}
			tasks[task.Name] = &task

		case "Pipeline":
			var p tektonv1.Pipeline
			if err := yaml.UnmarshalStrict(raw, &p); err != nil {
				t.Fatalf("UnmarshalStrict Pipeline %s: %v", fullPath, err)
			}
			p.SetDefaults(ctx)
			if errs := p.Validate(ctx); errs != nil {
				t.Fatalf("Pipeline %s (%s) failed Tekton v1 Validate: %v", p.Name, fullPath, errs)
			}
			pipeline = &p

		default:
			t.Fatalf("unexpected kind %q in %s", meta.Kind, fullPath)
		}
	}

	return tasks, pipeline
}

func TestManifestsValidateTektonV1AndKubernetesSchemas(t *testing.T) {
	ctx := tektonValidationContext(t)
	tasks, pipeline := loadTasksAndPipeline(t)

	wantTasks := []string{
		"clone-and-build",
		"unit-test",
		"vuln-scan",
		"integration-smoke",
		"package-and-publish",
	}
	for _, name := range wantTasks {
		if _, ok := tasks[name]; !ok {
			t.Fatalf("missing required Task %q in manifests/pipeline/", name)
		}
	}
	if pipeline == nil || pipeline.Name != "release-pipeline" {
		t.Fatalf("missing release-pipeline in manifests/pipeline/ (got %v)", pipeline)
	}

	// Verify Pipeline workspaces: required "source" + optional "creds".
	wsMap := map[string]tektonv1.PipelineWorkspaceDeclaration{}
	for _, ws := range pipeline.Spec.Workspaces {
		wsMap[ws.Name] = ws
	}
	if ws, ok := wsMap["source"]; !ok || ws.Optional {
		t.Fatalf("expected required workspace 'source', got %+v", ws)
	}
	if ws, ok := wsMap["creds"]; !ok || !ws.Optional {
		t.Fatalf("expected optional workspace 'creds', got %+v", ws)
	}

	// Verify DAG nodes, ApprovalGate CustomTask, and absence of $(tasks.clone-and-build.results.*)
	// so mid-DAG resume with skipTasks: ["clone-and-build"] never auto-skips downstream tasks.
	rawPipelineBytes, err := os.ReadFile(filepath.Join("..", "..", "manifests", "pipeline", "pipeline-release.yaml"))
	if err != nil {
		t.Fatalf("read pipeline-release.yaml: %v", err)
	}
	if strings.Contains(string(rawPipelineBytes), "tasks.clone-and-build.results") {
		t.Fatalf("pipeline-release.yaml must not reference tasks.clone-and-build.results so mid-DAG skipTasks: [clone-and-build] executes downstream stages cleanly")
	}

	ptByName := map[string]tektonv1.PipelineTask{}
	for _, pt := range pipeline.Spec.Tasks {
		ptByName[pt.Name] = pt
	}
	gateTask, ok := ptByName["release-approval"]
	if !ok {
		t.Fatal("missing release-approval task in release-pipeline")
	}
	if gateTask.TaskSpec == nil || gateTask.TaskSpec.APIVersion != approvalgate.APIVersion || gateTask.TaskSpec.Kind != approvalgate.Kind {
		t.Fatalf("unexpected release-approval taskSpec: %+v", gateTask.TaskSpec)
	}

	// Validate Pod+PVC example PipelineRun against stock Tekton v1.PipelineRun.Validate(ctx).
	pvcPRBytes, err := os.ReadFile(filepath.Join("..", "..", "manifests", "examples", "pipelinerun-pod-pvc.yaml"))
	if err != nil {
		t.Fatalf("read pipelinerun-pod-pvc.yaml: %v", err)
	}
	var pvcPR tektonv1.PipelineRun
	if err := yaml.UnmarshalStrict(pvcPRBytes, &pvcPR); err != nil {
		t.Fatalf("UnmarshalStrict pipelinerun-pod-pvc.yaml: %v", err)
	}
	pvcPR.SetDefaults(ctx)
	if errs := pvcPR.Validate(ctx); errs != nil {
		t.Fatalf("pipelinerun-pod-pvc.yaml failed Validate: %v", errs)
	}

	// Validate Actor example PipelineRun structure (actor workspace binding in forked Tekton).
	actorPRBytes, err := os.ReadFile(filepath.Join("..", "..", "manifests", "examples", "pipelinerun-actor.yaml"))
	if err != nil {
		t.Fatalf("read pipelinerun-actor.yaml: %v", err)
	}
	var actorPR struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       struct {
			PipelineRef struct {
				Name string `json:"name"`
			} `json:"pipelineRef"`
			Workspaces []struct {
				Name  string `json:"name"`
				Actor *struct {
					Atespace     string `json:"atespace"`
					EgressSecret string `json:"egressSecret"`
				} `json:"actor"`
			} `json:"workspaces"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(actorPRBytes, &actorPR); err != nil {
		t.Fatalf("Unmarshal pipelinerun-actor.yaml: %v", err)
	}
	if actorPR.APIVersion != "tekton.dev/v1" || actorPR.Kind != "PipelineRun" || actorPR.Spec.PipelineRef.Name != "release-pipeline" {
		t.Fatalf("unexpected pipelinerun-actor.yaml header/ref: %+v", actorPR)
	}
	if len(actorPR.Spec.Workspaces) != 1 ||
		actorPR.Spec.Workspaces[0].Name != "source" ||
		actorPR.Spec.Workspaces[0].Actor == nil ||
		actorPR.Spec.Workspaces[0].Actor.Atespace != "tekton-actors" ||
		actorPR.Spec.Workspaces[0].Actor.EgressSecret != "git-oci-auth" {
		t.Fatalf("pipelinerun-actor.yaml must bind only 'source' with actor{atespace: tekton-actors, egressSecret: git-oci-auth}: %+v", actorPR.Spec.Workspaces)
	}

	// Validate manifests/git-oci-server.yaml and manifests/demo-repo-git-oci.yaml Kubernetes objects.
	for _, tc := range []struct {
		file string
		ns   string
	}{
		{file: "git-oci-server.yaml", ns: "tekton-actors"},
		{file: "demo-repo-git-oci.yaml", ns: "demo-repo"},
	} {
		deployManifestBytes, err := os.ReadFile(filepath.Join("..", "..", "manifests", tc.file))
		if err != nil {
			t.Fatalf("read manifests/%s: %v", tc.file, err)
		}
		docs := bytes.Split(deployManifestBytes, []byte("\n---"))
		sawNS, sawDeploy, sawSvc := false, false, false
		for _, doc := range docs {
			trimmed := bytes.TrimSpace(doc)
			if len(trimmed) == 0 {
				continue
			}
			var kindMeta struct {
				Kind string `json:"kind"`
			}
			if err := yaml.Unmarshal(trimmed, &kindMeta); err != nil {
				t.Fatalf("unmarshal kind in %s: %v", tc.file, err)
			}
			switch kindMeta.Kind {
			case "Namespace":
				var ns corev1.Namespace
				if err := yaml.UnmarshalStrict(trimmed, &ns); err != nil {
					t.Fatalf("UnmarshalStrict Namespace in %s: %v", tc.file, err)
				}
				if ns.Name == tc.ns {
					sawNS = true
				}
			case "Deployment":
				var dep appsv1.Deployment
				if err := yaml.UnmarshalStrict(trimmed, &dep); err != nil {
					t.Fatalf("UnmarshalStrict Deployment in %s: %v", tc.file, err)
				}
				if dep.Name == "git-oci" && dep.Namespace == tc.ns {
					sawDeploy = true
				}
			case "Service":
				var svc corev1.Service
				if err := yaml.UnmarshalStrict(trimmed, &svc); err != nil {
					t.Fatalf("UnmarshalStrict Service in %s: %v", tc.file, err)
				}
				if svc.Name == "git-oci" && svc.Namespace == tc.ns {
					sawSvc = true
				}
			}
		}
		if !sawNS || !sawDeploy || !sawSvc {
			t.Fatalf("missing expected resources in %s: ns=%v deploy=%v svc=%v",
				tc.file, sawNS, sawDeploy, sawSvc)
		}
	}
}

func TestPipelineTaskScriptsDualModeExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tasks, _ := loadTasksAndPipeline(t)

	realToken := randomTestToken(t)
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: realToken,
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	originTS := httptest.NewServer(srv.Handler())
	defer originTS.Close()

	// Simulated atenet-egress HTTP proxy: replaces "Bearer ate-placeholder" with
	// "Bearer <realToken>" on the wire while keeping zero secrets inside the Actor workspace!
	originURL, err := url.Parse(originTS.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	egressProxy := httputil.NewSingleHostReverseProxy(originURL)
	origDirector := egressProxy.Director
	egressProxy.Director = func(req *http.Request) {
		origDirector(req)
		if strings.TrimSpace(req.Header.Get("Authorization")) == "Bearer ate-placeholder" {
			req.Header.Set("Authorization", "Bearer "+realToken)
		}
	}
	egressTS := httptest.NewServer(egressProxy)
	defer egressTS.Close()

	// --- Mode A: Actor Workspace Binding Mode (Zero Ambient Secrets + EgressPolicy) ---
	actorRoot := t.TempDir()
	actorSourceDir := filepath.Join(actorRoot, "source")
	actorCredsDir := filepath.Join(actorRoot, "creds") // intentionally NOT created (unbound)
	if err := os.MkdirAll(actorSourceDir, 0o755); err != nil {
		t.Fatalf("mkdir actorSourceDir: %v", err)
	}

	cloneRes := executeTaskSteps(t, ctx, tasks["clone-and-build"], map[string]string{
		"$(params.git-url)":         egressTS.URL + "/repo/fulfillment-api.git",
		"$(params.inject-bug)":      "false",
		"$(workspaces.source.path)": actorSourceDir,
		"$(workspaces.creds.path)":  actorCredsDir,
		"$(workspaces.creds.bound)": "false",
	})
	if cloneRes["commit-sha"] != srv.MainCommitSHA() {
		t.Fatalf("Actor clone-and-build commit-sha = %q, want %q", cloneRes["commit-sha"], srv.MainCommitSHA())
	}
	if len(cloneRes["binary-sha256"]) != 64 {
		t.Fatalf("Actor clone-and-build binary-sha256 = %q", cloneRes["binary-sha256"])
	}

	unitRes := executeTaskSteps(t, ctx, tasks["unit-test"], map[string]string{
		"$(workspaces.source.path)": actorSourceDir,
	})
	if !strings.Contains(unitRes["test-summary"], "PASS") {
		t.Fatalf("Actor unit-test summary = %q, expected PASS", unitRes["test-summary"])
	}

	vulnRes := executeTaskSteps(t, ctx, tasks["vuln-scan"], map[string]string{
		"$(workspaces.source.path)": actorSourceDir,
		"$(workspaces.creds.path)":  actorCredsDir,
		"$(workspaces.creds.bound)": "false",
	})
	if vulnRes["secret-posture"] != "ZERO_AMBIENT_SECRETS" {
		t.Fatalf("Actor vuln-scan secret-posture = %q, want ZERO_AMBIENT_SECRETS (report=%s)",
			vulnRes["secret-posture"], vulnRes["scan-report"])
	}

	smokeRes := executeTaskSteps(t, ctx, tasks["integration-smoke"], map[string]string{
		"$(workspaces.source.path)": actorSourceDir,
	})
	if !strings.Contains(smokeRes["smoke-status"], "ok") {
		t.Fatalf("Actor integration-smoke status = %q", smokeRes["smoke-status"])
	}

	pubRes := executeTaskSteps(t, ctx, tasks["package-and-publish"], map[string]string{
		"$(params.registry-url)":      egressTS.URL,
		"$(params.image-repo)":        "fulfillment-api",
		"$(params.image-tag)":         "v1.0.0-actor",
		"$(params.test-summary)":      unitRes["test-summary"],
		"$(params.secret-posture)":    vulnRes["secret-posture"],
		"$(params.smoke-status)":      smokeRes["smoke-status"],
		"$(params.approval-decision)": "approved",
		"$(workspaces.source.path)":   actorSourceDir,
		"$(workspaces.creds.path)":    actorCredsDir,
		"$(workspaces.creds.bound)":   "false",
	})
	if !strings.HasPrefix(pubRes["image-digest"], "sha256:") {
		t.Fatalf("Actor package-and-publish image-digest = %q", pubRes["image-digest"])
	}

	// --- Mode B: Pod + PVC Mode (Mounted Secret Workspace) ---
	pvcRoot := t.TempDir()
	pvcSourceDir := filepath.Join(pvcRoot, "source")
	pvcCredsDir := filepath.Join(pvcRoot, "creds")
	if err := os.MkdirAll(pvcSourceDir, 0o755); err != nil {
		t.Fatalf("mkdir pvcSourceDir: %v", err)
	}
	if err := os.MkdirAll(pvcCredsDir, 0o755); err != nil {
		t.Fatalf("mkdir pvcCredsDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pvcCredsDir, "token"), []byte(realToken+"\n"), 0o600); err != nil {
		t.Fatalf("write pvc token: %v", err)
	}

	executeTaskSteps(t, ctx, tasks["clone-and-build"], map[string]string{
		"$(params.git-url)":         originTS.URL + "/repo/fulfillment-api.git",
		"$(params.inject-bug)":      "false",
		"$(workspaces.source.path)": pvcSourceDir,
		"$(workspaces.creds.path)":  pvcCredsDir,
		"$(workspaces.creds.bound)": "true",
	})

	pvcVulnRes := executeTaskSteps(t, ctx, tasks["vuln-scan"], map[string]string{
		"$(workspaces.source.path)": pvcSourceDir,
		"$(workspaces.creds.path)":  pvcCredsDir,
		"$(workspaces.creds.bound)": "true",
	})
	if pvcVulnRes["secret-posture"] != "AMBIENT_SECRET_MOUNTED" {
		t.Fatalf("Pod+PVC vuln-scan secret-posture = %q, want AMBIENT_SECRET_MOUNTED", pvcVulnRes["secret-posture"])
	}
	if strings.Contains(pvcVulnRes["scan-report"], realToken) {
		t.Fatalf("vuln-scan scan-report leaked raw secret token!")
	}

	pvcPubRes := executeTaskSteps(t, ctx, tasks["package-and-publish"], map[string]string{
		"$(params.registry-url)":      originTS.URL,
		"$(params.image-repo)":        "fulfillment-api",
		"$(params.image-tag)":         "v1.0.0-pvc",
		"$(params.test-summary)":      "PASS",
		"$(params.secret-posture)":    pvcVulnRes["secret-posture"],
		"$(params.smoke-status)":      "ok",
		"$(params.approval-decision)": "approved",
		"$(workspaces.source.path)":   pvcSourceDir,
		"$(workspaces.creds.path)":    pvcCredsDir,
		"$(workspaces.creds.bound)":   "true",
	})
	if !strings.HasPrefix(pvcPubRes["image-digest"], "sha256:") {
		t.Fatalf("Pod+PVC package-and-publish image-digest = %q", pvcPubRes["image-digest"])
	}

	// Verify git-oci-server audit recorded BOTH ate-egress and mounted-secret OCI pushes!
	auditResp, err := http.Get(originTS.URL + "/api/audit")
	if err != nil {
		t.Fatalf("GET /api/audit: %v", err)
	}
	rawAudit, _ := io.ReadAll(auditResp.Body)
	auditResp.Body.Close()
	var audit gitoci.AuditResponse
	if err := json.Unmarshal(rawAudit, &audit); err != nil {
		t.Fatalf("unmarshal audit: %v", err)
	}
	if audit.Summary.OCIManifestPushes < 2 {
		t.Fatalf("OCIManifestPushes = %d, want >= 2", audit.Summary.OCIManifestPushes)
	}
}

func TestPipelineTaskScriptsBuggyRescueResumeLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tasks, _ := loadTasksAndPipeline(t)

	realToken := randomTestToken(t)
	srv, err := gitoci.NewServer(gitoci.Config{
		DataDir:   t.TempDir(),
		AuthToken: realToken,
	})
	if err != nil {
		t.Fatalf("gitoci.NewServer: %v", err)
	}
	originTS := httptest.NewServer(srv.Handler())
	defer originTS.Close()

	// 1. Run clone-and-build with inject-bug=true.
	wsRoot := t.TempDir()
	sourceDir := filepath.Join(wsRoot, "source")
	credsDir := filepath.Join(wsRoot, "creds")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("mkdir sourceDir: %v", err)
	}
	if err := os.MkdirAll(credsDir, 0o755); err != nil {
		t.Fatalf("mkdir credsDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(credsDir, "token"), []byte(realToken), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	cloneRes := executeTaskSteps(t, ctx, tasks["clone-and-build"], map[string]string{
		"$(params.git-url)":         originTS.URL + "/repo/fulfillment-api.git",
		"$(params.inject-bug)":      "true",
		"$(workspaces.source.path)": sourceDir,
		"$(workspaces.creds.path)":  credsDir,
		"$(workspaces.creds.bound)": "true",
	})
	if cloneRes["commit-sha"] != srv.BuggyCommitSHA() {
		t.Fatalf("inject-bug=true commit-sha = %q, want buggy %q", cloneRes["commit-sha"], srv.BuggyCommitSHA())
	}

	// 2. unit-test MUST fail on TestCalculateTotal while integration-smoke still succeeds!
	smokeRes := executeTaskSteps(t, ctx, tasks["integration-smoke"], map[string]string{
		"$(workspaces.source.path)": sourceDir,
	})
	if !strings.Contains(smokeRes["smoke-status"], "ok") {
		t.Fatalf("expected integration-smoke to succeed on standard tier, got %q", smokeRes["smoke-status"])
	}

	unitErrOut, unitErr := executeTaskStepExpectError(t, ctx, tasks["unit-test"], map[string]string{
		"$(workspaces.source.path)": sourceDir,
	})
	if unitErr == nil {
		t.Fatalf("expected unit-test to fail when inject-bug=true, but succeeded:\n%s", unitErrOut)
	}
	if !strings.Contains(unitErrOut, "TestCalculateTotal") {
		t.Fatalf("expected TestCalculateTotal in unit-test failure log, got:\n%s", unitErrOut)
	}

	// 3. Hot-patch /workspace using SampleAppWorkspaceFixDiff via git apply and resume unit-test + package-and-publish WITHOUT re-running clone-and-build!
	applyCmd := exec.CommandContext(ctx, "git", "apply")
	applyCmd.Dir = wsRoot
	applyCmd.Stdin = strings.NewReader(gitoci.SampleAppWorkspaceFixDiff)
	if out, err := applyCmd.CombinedOutput(); err != nil {
		t.Fatalf("git apply SampleAppWorkspaceFixDiff failed: %v\n%s", err, string(out))
	}

	resumedUnitRes := executeTaskSteps(t, ctx, tasks["unit-test"], map[string]string{
		"$(workspaces.source.path)": sourceDir,
	})
	if !strings.Contains(resumedUnitRes["test-summary"], "PASS") {
		t.Fatalf("resumed unit-test summary = %q, expected PASS", resumedUnitRes["test-summary"])
	}

	pubRes := executeTaskSteps(t, ctx, tasks["package-and-publish"], map[string]string{
		"$(params.registry-url)":      originTS.URL,
		"$(params.image-repo)":        "fulfillment-api",
		"$(params.image-tag)":         "v1.0.1-rescued",
		"$(params.test-summary)":      resumedUnitRes["test-summary"],
		"$(params.secret-posture)":    "ZERO_AMBIENT_SECRETS",
		"$(params.smoke-status)":      smokeRes["smoke-status"],
		"$(params.approval-decision)": "approved",
		"$(workspaces.source.path)":   sourceDir,
		"$(workspaces.creds.path)":    credsDir,
		"$(workspaces.creds.bound)":   "true",
	})
	if !strings.HasPrefix(pubRes["image-digest"], "sha256:") {
		t.Fatalf("resumed package-and-publish image-digest = %q", pubRes["image-digest"])
	}
}

func executeTaskSteps(t *testing.T, ctx context.Context, task *tektonv1.Task, replacements map[string]string) map[string]string {
	t.Helper()
	resultsDir := t.TempDir()
	fullMap := make(map[string]string, len(replacements)+len(task.Spec.Results)+len(task.Spec.Params))
	for _, p := range task.Spec.Params {
		if p.Default != nil {
			fullMap["$(params."+p.Name+")"] = p.Default.StringVal
		} else {
			fullMap["$(params."+p.Name+")"] = ""
		}
	}
	for k, v := range replacements {
		fullMap[k] = v
	}
	for _, r := range task.Spec.Results {
		resFile := filepath.Join(resultsDir, r.Name)
		fullMap["$(results."+r.Name+".path)"] = resFile
	}

	for _, step := range task.Spec.Steps {
		script := step.Script
		for placeholder, val := range fullMap {
			script = strings.ReplaceAll(script, placeholder, val)
		}
		scriptFile := filepath.Join(t.TempDir(), "step-"+step.Name+".sh")
		if err := os.WriteFile(scriptFile, []byte(script), 0o755); err != nil {
			t.Fatalf("write step script %s/%s: %v", task.Name, step.Name, err)
		}

		// Use an isolated HOME/TMPDIR while inheriting the host's warm GOCACHE so stdlib isn't recompiled 3x.
		hostGoCacheOut, _ := exec.CommandContext(ctx, "go", "env", "GOCACHE").Output()
		hostGoCache := strings.TrimSpace(string(hostGoCacheOut))

		cmd := exec.CommandContext(ctx, "/bin/sh", scriptFile)
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + t.TempDir(),
			"TMPDIR=" + t.TempDir(),
			"GOPROXY=off",
			"GOSUMDB=off",
			"GOTOOLCHAIN=local",
			"TEKTON_GOCACHE_OVERRIDE=" + hostGoCache,
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("task %s step %s failed: %v\nOutput:\n%s", task.Name, step.Name, err, string(out))
		}
	}

	collected := make(map[string]string, len(task.Spec.Results))
	for _, r := range task.Spec.Results {
		b, err := os.ReadFile(filepath.Join(resultsDir, r.Name))
		if err != nil {
			t.Fatalf("task %s did not write result %q: %v", task.Name, r.Name, err)
		}
		collected[r.Name] = strings.TrimSpace(string(b))
	}
	return collected
}

func executeTaskStepExpectError(t *testing.T, ctx context.Context, task *tektonv1.Task, replacements map[string]string) (string, error) {
	t.Helper()
	resultsDir := t.TempDir()
	fullMap := make(map[string]string, len(replacements)+len(task.Spec.Results)+len(task.Spec.Params))
	for _, p := range task.Spec.Params {
		if p.Default != nil {
			fullMap["$(params."+p.Name+")"] = p.Default.StringVal
		} else {
			fullMap["$(params."+p.Name+")"] = ""
		}
	}
	for k, v := range replacements {
		fullMap[k] = v
	}
	for _, r := range task.Spec.Results {
		fullMap["$(results."+r.Name+".path)"] = filepath.Join(resultsDir, r.Name)
	}

	hostGoCacheOut, _ := exec.CommandContext(ctx, "go", "env", "GOCACHE").Output()
	hostGoCache := strings.TrimSpace(string(hostGoCacheOut))

	var allOut strings.Builder
	for _, step := range task.Spec.Steps {
		script := step.Script
		for placeholder, val := range fullMap {
			script = strings.ReplaceAll(script, placeholder, val)
		}
		scriptFile := filepath.Join(t.TempDir(), "step-"+step.Name+".sh")
		if err := os.WriteFile(scriptFile, []byte(script), 0o755); err != nil {
			t.Fatalf("write step script: %v", err)
		}
		cmd := exec.CommandContext(ctx, "/bin/sh", scriptFile)
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + t.TempDir(),
			"TMPDIR=" + t.TempDir(),
			"GOPROXY=off",
			"GOSUMDB=off",
			"GOTOOLCHAIN=local",
			"TEKTON_GOCACHE_OVERRIDE=" + hostGoCache,
		}
		out, err := cmd.CombinedOutput()
		allOut.Write(out)
		if err != nil {
			return allOut.String(), err
		}
	}
	return allOut.String(), nil
}
