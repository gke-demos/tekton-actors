package approvalgate_test

import (
	"context"
	"testing"
	"time"

	"github.com/gke-demos/tekton-actors/pkg/approvalgate"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
)

func TestReconcile_IgnoresNonApprovalGateCustomRuns(t *testing.T) {
	ctx := context.Background()

	otherRef := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "other-custom-ref",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: "example.dev/v1alpha1",
				Kind:       "WaitTimer",
			},
		},
	}
	otherSpec := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "other-custom-spec",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomSpec: &v1beta1.EmbeddedCustomRunSpec{
				TypeMeta: runtime.TypeMeta{
					APIVersion: approvalgate.APIVersion,
					Kind:       "SomethingElse",
				},
			},
		},
	}

	client := fake.NewSimpleClientset(otherRef, otherSpec)
	reconciler := approvalgate.NewReconciler(client)

	for _, name := range []string{"other-custom-ref", "other-custom-spec", "missing-run"} {
		if err := reconciler.Reconcile(ctx, "ci", name); err != nil {
			t.Fatalf("Reconcile(%q) unexpected error: %v", name, err)
		}
	}

	for _, name := range []string{"other-custom-ref", "other-custom-spec"} {
		got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get(%q) error: %v", name, err)
		}
		if got.Status.StartTime != nil {
			t.Errorf("%q StartTime = %v, want nil", name, got.Status.StartTime)
		}
		if len(got.Status.Conditions) != 0 {
			t.Errorf("%q Conditions = %+v, want empty", name, got.Status.Conditions)
		}
	}
}

func TestReconcile_InitializesUnapprovedCustomRunToPendingApproval(t *testing.T) {
	ctx := context.Background()
	fixedNow := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)

	defaultGate := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "release-gate-default",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
		},
	}
	customSpecGate := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "release-gate-embedded",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomSpec: &v1beta1.EmbeddedCustomRunSpec{
				TypeMeta: runtime.TypeMeta{
					APIVersion: approvalgate.APIVersion,
					Kind:       approvalgate.Kind,
				},
			},
			Params: v1beta1.Params{
				{
					Name:  "description",
					Value: *v1beta1.NewStructuredValues("Approve 100% production rollout"),
				},
			},
		},
	}

	client := fake.NewSimpleClientset(defaultGate, customSpecGate)
	reconciler := approvalgate.NewReconciler(client, approvalgate.WithClock(func() time.Time { return fixedNow }))

	if err := reconciler.Reconcile(ctx, "ci", "release-gate-default"); err != nil {
		t.Fatalf("Reconcile(release-gate-default) error: %v", err)
	}
	if err := reconciler.Reconcile(ctx, "ci", "release-gate-embedded"); err != nil {
		t.Fatalf("Reconcile(release-gate-embedded) error: %v", err)
	}

	gotDefault, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "release-gate-default", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(release-gate-default) error: %v", err)
	}
	if gotDefault.Status.StartTime == nil || !gotDefault.Status.StartTime.Time.Equal(fixedNow) {
		t.Fatalf("StartTime = %v, want %v", gotDefault.Status.StartTime, fixedNow)
	}
	cond := gotDefault.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil {
		t.Fatal("expected Succeeded condition to be set")
	}
	if cond.Status != corev1.ConditionUnknown {
		t.Errorf("Succeeded status = %q, want %q", cond.Status, corev1.ConditionUnknown)
	}
	if cond.Reason != approvalgate.ReasonPendingApproval {
		t.Errorf("Succeeded reason = %q, want %q", cond.Reason, approvalgate.ReasonPendingApproval)
	}
	if cond.Message != approvalgate.DefaultPendingMessage {
		t.Errorf("Succeeded message = %q, want %q", cond.Message, approvalgate.DefaultPendingMessage)
	}

	gotEmbedded, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "release-gate-embedded", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(release-gate-embedded) error: %v", err)
	}
	condEmbedded := gotEmbedded.Status.GetCondition(apis.ConditionSucceeded)
	if condEmbedded == nil || condEmbedded.Status != corev1.ConditionUnknown || condEmbedded.Reason != approvalgate.ReasonPendingApproval {
		t.Fatalf("embedded Succeeded condition = %+v, want Unknown/PendingApproval", condEmbedded)
	}
	if condEmbedded.Message != "Approve 100% production rollout" {
		t.Errorf("embedded Succeeded message = %q, want %q", condEmbedded.Message, "Approve 100% production rollout")
	}

	// Re-reconciling an already-pending gate with unchanged state must not issue redundant status writes.
	actionsBefore := len(client.Actions())
	if err := reconciler.Reconcile(ctx, "ci", "release-gate-default"); err != nil {
		t.Fatalf("second Reconcile(release-gate-default) error: %v", err)
	}
	for _, act := range client.Actions()[actionsBefore:] {
		if act.GetVerb() == "update" {
			t.Errorf("unexpected redundant update action on idempotent pending reconcile: %+v", act)
		}
	}
}

func TestReconcile_ApprovedViaSetDecisionAndSpecParams(t *testing.T) {
	ctx := context.Background()
	startAt := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)
	approveAt := time.Date(2026, 11, 11, 1, 5, 30, 0, time.UTC)
	currentNow := startAt

	gateViaHelper := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "gate-approve-helper",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
			Params: v1beta1.Params{
				{
					Name:  "message",
					Value: *v1beta1.NewStructuredValues("Verify staging smoke metrics"),
				},
			},
		},
	}

	gateViaParams := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "gate-approve-params",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
			Params: v1beta1.Params{
				{
					Name:  "decision",
					Value: *v1beta1.NewStructuredValues("approved"),
				},
			},
		},
	}

	client := fake.NewSimpleClientset(gateViaHelper, gateViaParams)
	reconciler := approvalgate.NewReconciler(client, approvalgate.WithClock(func() time.Time { return currentNow }))

	// Step 1: Initialize gateViaHelper to PendingApproval.
	if err := reconciler.Reconcile(ctx, "ci", "gate-approve-helper"); err != nil {
		t.Fatalf("initial Reconcile(gate-approve-helper) error: %v", err)
	}

	// Step 2: Approve via SetDecision and advance clock.
	currentNow = approveAt
	if err := approvalgate.SetDecision(ctx, client, "ci", "gate-approve-helper", approvalgate.DecisionApproved, "release-bot", "canary error budget healthy"); err != nil {
		t.Fatalf("SetDecision(approved) error: %v", err)
	}
	if err := reconciler.Reconcile(ctx, "ci", "gate-approve-helper"); err != nil {
		t.Fatalf("post-approval Reconcile(gate-approve-helper) error: %v", err)
	}

	got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "gate-approve-helper", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(gate-approve-helper) error: %v", err)
	}
	cond := got.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil || cond.Status != corev1.ConditionTrue || cond.Reason != approvalgate.ReasonApproved {
		t.Fatalf("Succeeded condition = %+v, want True/Approved", cond)
	}
	if got.Status.StartTime == nil || !got.Status.StartTime.Time.Equal(startAt) {
		t.Errorf("StartTime = %v, want %v", got.Status.StartTime, startAt)
	}
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Time.Equal(approveAt) {
		t.Errorf("CompletionTime = %v, want %v", got.Status.CompletionTime, approveAt)
	}

	results := resultsMap(got.Status.Results)
	if results["decision"] != "approved" {
		t.Errorf("result[decision] = %q, want %q", results["decision"], "approved")
	}
	if results["approvedBy"] != "release-bot" {
		t.Errorf("result[approvedBy] = %q, want %q", results["approvedBy"], "release-bot")
	}
	if results["approvedAt"] != "2026-11-11T01:05:30Z" {
		t.Errorf("result[approvedAt] = %q, want %q", results["approvedAt"], "2026-11-11T01:05:30Z")
	}
	if results["note"] != "canary error budget healthy" {
		t.Errorf("result[note] = %q, want %q", results["note"], "canary error budget healthy")
	}

	// Step 3: Reconciling an already-terminal run must be a no-op.
	actionsBefore := len(client.Actions())
	currentNow = approveAt.Add(10 * time.Minute)
	if err := reconciler.Reconcile(ctx, "ci", "gate-approve-helper"); err != nil {
		t.Fatalf("terminal Reconcile(gate-approve-helper) error: %v", err)
	}
	for _, act := range client.Actions()[actionsBefore:] {
		if act.GetVerb() == "update" {
			t.Errorf("unexpected update on terminal CustomRun: %+v", act)
		}
	}

	// Step 4: Spec-param approval with default "operator" actor.
	if err := reconciler.Reconcile(ctx, "ci", "gate-approve-params"); err != nil {
		t.Fatalf("Reconcile(gate-approve-params) error: %v", err)
	}
	gotParams, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "gate-approve-params", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(gate-approve-params) error: %v", err)
	}
	condParams := gotParams.Status.GetCondition(apis.ConditionSucceeded)
	if condParams == nil || condParams.Status != corev1.ConditionTrue || condParams.Reason != approvalgate.ReasonApproved {
		t.Fatalf("params Succeeded condition = %+v, want True/Approved", condParams)
	}
	paramResults := resultsMap(gotParams.Status.Results)
	if paramResults["approvedBy"] != "operator" {
		t.Errorf("default result[approvedBy] = %q, want %q", paramResults["approvedBy"], "operator")
	}
}

func TestReconcile_RejectedAndSetDecisionValidation(t *testing.T) {
	ctx := context.Background()
	startAt := time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC)
	rejectAt := time.Date(2026, 11, 11, 2, 3, 15, 0, time.UTC)
	currentNow := startAt

	gate := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "gate-reject",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
		},
	}
	nonGate := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "not-a-gate",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: "example.dev/v1alpha1",
				Kind:       "Noop",
			},
		},
	}

	client := fake.NewSimpleClientset(gate, nonGate)
	reconciler := approvalgate.NewReconciler(client, approvalgate.WithClock(func() time.Time { return currentNow }))

	if err := reconciler.Reconcile(ctx, "ci", "gate-reject"); err != nil {
		t.Fatalf("initial Reconcile(gate-reject) error: %v", err)
	}

	if err := approvalgate.SetDecision(ctx, client, "ci", "gate-reject", "maybe", "sec-reviewer", ""); err == nil {
		t.Fatal("expected SetDecision with invalid decision to return error")
	}
	if err := approvalgate.SetDecision(ctx, client, "ci", "not-a-gate", approvalgate.DecisionRejected, "sec-reviewer", ""); err == nil {
		t.Fatal("expected SetDecision on non-ApprovalGate CustomRun to return error")
	}

	currentNow = rejectAt
	if err := approvalgate.SetDecision(ctx, client, "ci", "gate-reject", approvalgate.DecisionRejected, "sec-reviewer", "CVE-2026-9999 detected in base image"); err != nil {
		t.Fatalf("SetDecision(rejected) error: %v", err)
	}
	if err := reconciler.Reconcile(ctx, "ci", "gate-reject"); err != nil {
		t.Fatalf("post-rejection Reconcile(gate-reject) error: %v", err)
	}

	got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "gate-reject", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(gate-reject) error: %v", err)
	}
	cond := got.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != approvalgate.ReasonRejected {
		t.Fatalf("Succeeded condition = %+v, want False/Rejected", cond)
	}
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Time.Equal(rejectAt) {
		t.Errorf("CompletionTime = %v, want %v", got.Status.CompletionTime, rejectAt)
	}

	results := resultsMap(got.Status.Results)
	if results["decision"] != "rejected" {
		t.Errorf("result[decision] = %q, want %q", results["decision"], "rejected")
	}
	if results["approvedBy"] != "sec-reviewer" {
		t.Errorf("result[approvedBy] = %q, want %q", results["approvedBy"], "sec-reviewer")
	}
	if results["note"] != "CVE-2026-9999 detected in base image" {
		t.Errorf("result[note] = %q, want %q", results["note"], "CVE-2026-9999 detected in base image")
	}

	if err := approvalgate.SetDecision(ctx, client, "ci", "gate-reject", approvalgate.DecisionApproved, "operator", "override"); err == nil {
		t.Fatal("expected SetDecision on already-terminal CustomRun to return error")
	}
}

func TestReconcile_RunCancelledTransitionsToCancelled(t *testing.T) {
	ctx := context.Background()
	startAt := time.Date(2026, 11, 11, 3, 0, 0, 0, time.UTC)
	cancelAt := time.Date(2026, 11, 11, 3, 1, 45, 0, time.UTC)
	currentNow := startAt

	gate := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "gate-cancel",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
		},
	}

	client := fake.NewSimpleClientset(gate)
	reconciler := approvalgate.NewReconciler(client, approvalgate.WithClock(func() time.Time { return currentNow }))

	if err := reconciler.Reconcile(ctx, "ci", "gate-cancel"); err != nil {
		t.Fatalf("initial Reconcile(gate-cancel) error: %v", err)
	}

	pending, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "gate-cancel", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(gate-cancel) error: %v", err)
	}
	cancelledCopy := pending.DeepCopy()
	cancelledCopy.Spec.Status = v1beta1.CustomRunSpecStatusCancelled
	cancelledCopy.Spec.StatusMessage = v1beta1.CustomRunCancelledByPipelineMsg
	if _, err := client.TektonV1beta1().CustomRuns("ci").Update(ctx, cancelledCopy, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update(RunCancelled) error: %v", err)
	}

	currentNow = cancelAt
	if err := reconciler.Reconcile(ctx, "ci", "gate-cancel"); err != nil {
		t.Fatalf("cancelled Reconcile(gate-cancel) error: %v", err)
	}

	got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "gate-cancel", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(gate-cancel) after cancel error: %v", err)
	}
	cond := got.Status.GetCondition(apis.ConditionSucceeded)
	if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != approvalgate.ReasonCancelled {
		t.Fatalf("Succeeded condition = %+v, want False/Cancelled", cond)
	}
	if cond.Message != string(v1beta1.CustomRunCancelledByPipelineMsg) {
		t.Errorf("Succeeded message = %q, want %q", cond.Message, v1beta1.CustomRunCancelledByPipelineMsg)
	}
	if got.Status.StartTime == nil || !got.Status.StartTime.Time.Equal(startAt) {
		t.Errorf("StartTime = %v, want %v", got.Status.StartTime, startAt)
	}
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Time.Equal(cancelAt) {
		t.Errorf("CompletionTime = %v, want %v", got.Status.CompletionTime, cancelAt)
	}
}

func TestController_WatchesAndReconcilesEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := fake.NewSimpleClientset()
	reconciler := approvalgate.NewReconciler(client)
	ctrl := approvalgate.NewController(client, reconciler, approvalgate.ControllerConfig{
		Namespace:    "ci",
		ResyncPeriod: 0,
		Workers:      2,
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.Run(ctx)
	}()

	waitFor(t, 3*time.Second, func() bool {
		return ctrl.Ready()
	}, "controller cache sync")

	cr := &v1beta1.CustomRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ci",
			Name:      "live-watch-gate",
		},
		Spec: v1beta1.CustomRunSpec{
			CustomRef: &v1beta1.TaskRef{
				APIVersion: approvalgate.APIVersion,
				Kind:       approvalgate.Kind,
			},
		},
	}
	if _, err := client.TektonV1beta1().CustomRuns("ci").Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create(live-watch-gate) error: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "live-watch-gate", metav1.GetOptions{})
		if err != nil {
			return false
		}
		cond := got.Status.GetCondition(apis.ConditionSucceeded)
		return cond != nil && cond.Status == corev1.ConditionUnknown && cond.Reason == approvalgate.ReasonPendingApproval
	}, "PendingApproval status from informer loop")

	if err := approvalgate.SetDecision(ctx, client, "ci", "live-watch-gate", approvalgate.DecisionApproved, "sre-lead", "ship it"); err != nil {
		t.Fatalf("SetDecision(approved) error: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		got, err := client.TektonV1beta1().CustomRuns("ci").Get(ctx, "live-watch-gate", metav1.GetOptions{})
		if err != nil {
			return false
		}
		cond := got.Status.GetCondition(apis.ConditionSucceeded)
		return cond != nil && cond.Status == corev1.ConditionTrue && cond.Reason == approvalgate.ReasonApproved
	}, "Approved status from informer loop")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ctrl.Run returned error on shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for controller shutdown")
	}
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func resultsMap(results []v1beta1.CustomRunResult) map[string]string {
	out := make(map[string]string, len(results))
	for _, r := range results {
		out[r.Name] = r.Value
	}
	return out
}
