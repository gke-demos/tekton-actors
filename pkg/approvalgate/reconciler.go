package approvalgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
)

const (
	// APIVersion is the CustomRun target API version for ApprovalGate tasks.
	APIVersion = "tekton.ate.dev/v1alpha1"
	// Kind is the CustomRun target kind for ApprovalGate tasks.
	Kind = "ApprovalGate"

	// AnnotationDecision records the manual approval decision ("approved" | "rejected").
	AnnotationDecision = "tekton.ate.dev/approval-decision"
	// AnnotationApprovedBy records the actor/user who supplied the decision.
	AnnotationApprovedBy = "tekton.ate.dev/approved-by"
	// AnnotationNote records an optional approval or rejection note.
	AnnotationNote = "tekton.ate.dev/approval-note"

	// DecisionApproved approves a pending ApprovalGate.
	DecisionApproved = "approved"
	// DecisionRejected rejects a pending ApprovalGate.
	DecisionRejected = "rejected"

	// DefaultApprovedBy is used when no actor is explicitly supplied.
	DefaultApprovedBy = "operator"
	// DefaultPendingMessage is reported while an ApprovalGate awaits a decision.
	DefaultPendingMessage = "Waiting for manual release approval"

	// ReasonPendingApproval indicates the gate is waiting on manual approval.
	ReasonPendingApproval = "PendingApproval"
	// ReasonApproved indicates the gate was approved.
	ReasonApproved = "Approved"
	// ReasonRejected indicates the gate was rejected.
	ReasonRejected = "Rejected"
	// ReasonCancelled indicates the gate was cancelled via spec.status == RunCancelled.
	ReasonCancelled = "Cancelled"
)

// ReconcilerOption customizes a Reconciler.
type ReconcilerOption func(*Reconciler)

// WithClock overrides the wall-clock source used for timestamps.
func WithClock(now func() time.Time) ReconcilerOption {
	return func(r *Reconciler) {
		if now != nil {
			r.now = now
		}
	}
}

// Reconciler reconciles tekton.ate.dev/v1alpha1 ApprovalGate CustomRuns.
type Reconciler struct {
	client versioned.Interface
	now    func() time.Time
}

// NewReconciler creates a new ApprovalGate Reconciler.
func NewReconciler(client versioned.Interface, opts ...ReconcilerOption) *Reconciler {
	r := &Reconciler{
		client: client,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// IsApprovalGate reports whether cr targets tekton.ate.dev/v1alpha1 ApprovalGate
// via either spec.customRef or spec.customSpec.
func IsApprovalGate(cr *v1beta1.CustomRun) bool {
	if cr == nil {
		return false
	}
	if cr.Spec.CustomRef != nil &&
		cr.Spec.CustomRef.APIVersion == APIVersion &&
		string(cr.Spec.CustomRef.Kind) == Kind {
		return true
	}
	if cr.Spec.CustomSpec != nil &&
		cr.Spec.CustomSpec.APIVersion == APIVersion &&
		cr.Spec.CustomSpec.Kind == Kind {
		return true
	}
	return false
}

// SetDecision records a manual approval decision on a pending ApprovalGate CustomRun.
func SetDecision(ctx context.Context, tektonClient versioned.Interface, namespace, customRunName, decision, actor, note string) error {
	if tektonClient == nil {
		return errors.New("tekton client is required")
	}
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(customRunName) == "" {
		return errors.New("namespace and customRunName are required")
	}
	normalizedDecision := strings.ToLower(strings.TrimSpace(decision))
	if normalizedDecision != DecisionApproved && normalizedDecision != DecisionRejected {
		return fmt.Errorf("invalid approval decision %q: must be %q or %q", decision, DecisionApproved, DecisionRejected)
	}
	resolvedActor := strings.TrimSpace(actor)
	if resolvedActor == "" {
		resolvedActor = DefaultApprovedBy
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cr, err := tektonClient.TektonV1beta1().CustomRuns(namespace).Get(ctx, customRunName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !IsApprovalGate(cr) {
			return fmt.Errorf("customrun %s/%s is not an ApprovalGate", namespace, customRunName)
		}
		if cr.IsDone() || cr.IsCancelled() {
			return fmt.Errorf("approval gate %s/%s is already terminal or cancelled", namespace, customRunName)
		}

		updated := cr.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string, 3)
		}
		updated.Annotations[AnnotationDecision] = normalizedDecision
		updated.Annotations[AnnotationApprovedBy] = resolvedActor
		if note != "" {
			updated.Annotations[AnnotationNote] = note
		} else {
			delete(updated.Annotations, AnnotationNote)
		}

		_, err = tektonClient.TektonV1beta1().CustomRuns(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

// Reconcile reconciles a single CustomRun identified by namespace and name.
func (r *Reconciler) Reconcile(ctx context.Context, namespace, name string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cr, err := r.client.TektonV1beta1().CustomRuns(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !IsApprovalGate(cr) || cr.IsDone() {
			return nil
		}

		now := metav1.NewTime(r.now())

		if cr.IsCancelled() {
			updated := cr.DeepCopy()
			if !updated.HasStarted() {
				updated.Status.StartTime = &now
			}
			cancelMsg := strings.TrimSpace(string(cr.Spec.StatusMessage))
			if cancelMsg == "" {
				cancelMsg = "ApprovalGate cancelled"
			}
			updated.Status.MarkCustomRunFailed(ReasonCancelled, "%s", cancelMsg)
			updated.Status.CompletionTime = &now
			_, err = r.client.TektonV1beta1().CustomRuns(namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
			return err
		}

		decision, approvedBy, note := resolveDecision(cr)

		switch decision {
		case DecisionApproved:
			updated := cr.DeepCopy()
			if !updated.HasStarted() {
				updated.Status.StartTime = &now
			}
			setResult(&updated.Status.Results, "decision", DecisionApproved)
			setResult(&updated.Status.Results, "approvedBy", approvedBy)
			setResult(&updated.Status.Results, "approvedAt", now.Time.UTC().Format(time.RFC3339))
			setResult(&updated.Status.Results, "note", note)

			msg := fmt.Sprintf("Approved by %s", approvedBy)
			if note != "" {
				msg = fmt.Sprintf("Approved by %s: %s", approvedBy, note)
			}
			updated.Status.MarkCustomRunSucceeded(ReasonApproved, "%s", msg)
			updated.Status.CompletionTime = &now

			_, err = r.client.TektonV1beta1().CustomRuns(namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
			return err

		case DecisionRejected:
			updated := cr.DeepCopy()
			if !updated.HasStarted() {
				updated.Status.StartTime = &now
			}
			setResult(&updated.Status.Results, "decision", DecisionRejected)
			setResult(&updated.Status.Results, "approvedBy", approvedBy)
			setResult(&updated.Status.Results, "note", note)

			msg := fmt.Sprintf("Rejected by %s", approvedBy)
			if note != "" {
				msg = fmt.Sprintf("Rejected by %s: %s", approvedBy, note)
			}
			updated.Status.MarkCustomRunFailed(ReasonRejected, "%s", msg)
			updated.Status.CompletionTime = &now

			_, err = r.client.TektonV1beta1().CustomRuns(namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
			return err
		}

		pendingMsg := resolvePendingMessage(cr)
		cond := cr.Status.GetCondition(apis.ConditionSucceeded)
		if cr.HasStarted() &&
			cond != nil &&
			cond.Status == corev1.ConditionUnknown &&
			cond.Reason == ReasonPendingApproval &&
			cond.Message == pendingMsg {
			return nil
		}

		updated := cr.DeepCopy()
		if !updated.HasStarted() {
			updated.Status.StartTime = &now
		}
		updated.Status.MarkCustomRunRunning(ReasonPendingApproval, "%s", pendingMsg)
		_, err = r.client.TektonV1beta1().CustomRuns(namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

func resolveDecision(cr *v1beta1.CustomRun) (decision, approvedBy, note string) {
	if cr.Annotations != nil {
		decision = strings.ToLower(strings.TrimSpace(cr.Annotations[AnnotationDecision]))
		approvedBy = strings.TrimSpace(cr.Annotations[AnnotationApprovedBy])
		note = cr.Annotations[AnnotationNote]
	}
	if decision == "" {
		decision = strings.ToLower(strings.TrimSpace(getStringParam(cr, "decision")))
	}
	if approvedBy == "" {
		approvedBy = strings.TrimSpace(getStringParam(cr, "approvedBy"))
	}
	if approvedBy == "" {
		approvedBy = DefaultApprovedBy
	}
	if note == "" {
		note = getStringParam(cr, "note")
	}
	return decision, approvedBy, note
}

func setResult(results *[]v1beta1.CustomRunResult, name, value string) {
	for i := range *results {
		if (*results)[i].Name == name {
			(*results)[i].Value = value
			return
		}
	}
	*results = append(*results, v1beta1.CustomRunResult{
		Name:  name,
		Value: value,
	})
}

func resolvePendingMessage(cr *v1beta1.CustomRun) string {
	for _, key := range []string{"description", "message"} {
		if val := strings.TrimSpace(getStringParam(cr, key)); val != "" {
			return val
		}
	}
	if cr.Spec.CustomSpec != nil && len(cr.Spec.CustomSpec.Spec.Raw) > 0 {
		var specFields struct {
			Description string `json:"description"`
			Message     string `json:"message"`
		}
		if err := json.Unmarshal(cr.Spec.CustomSpec.Spec.Raw, &specFields); err == nil {
			if val := strings.TrimSpace(specFields.Description); val != "" {
				return val
			}
			if val := strings.TrimSpace(specFields.Message); val != "" {
				return val
			}
		}
	}
	return DefaultPendingMessage
}

func getStringParam(cr *v1beta1.CustomRun, name string) string {
	p := cr.Spec.GetParam(name)
	if p == nil {
		return ""
	}
	return p.Value.StringVal
}
