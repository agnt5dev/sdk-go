package agnt5

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// SleepOption configures one workflow sleep.
type SleepOption func(*sleepOptions)

type sleepOptions struct {
	key string
}

// WithSleepKey assigns a stable key to a durable sleep. Use explicit keys in
// branches, loops, and fan-out where source-order ordinals are not stable.
func WithSleepKey(key string) SleepOption {
	return func(options *sleepOptions) {
		options.key = strings.TrimSpace(key)
	}
}

type durableSleepSuspensionError struct {
	suspension *pb.WorkerSuspension
}

func (e *durableSleepSuspensionError) Error() string {
	if e == nil || e.suspension == nil {
		return "agnt5: durable sleep suspended"
	}
	return "agnt5: durable sleep suspended: " + e.suspension.GetTimerKey()
}

// Sleep waits locally outside a negotiated runtime context and yields a typed
// durable suspension when durable_suspension_v1 is available.
func (c *Context) Sleep(duration time.Duration, opts ...SleepOption) error {
	if c == nil {
		return context.Canceled
	}
	if duration < 0 {
		return fmt.Errorf("agnt5: sleep duration cannot be negative")
	}
	if duration == 0 {
		return nil
	}
	options := sleepOptions{}
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	timerKey := ""
	if options.key != "" {
		timerKey = "sleep:" + options.key
	} else {
		timerKey = strings.TrimPrefix(c.nextStepKey("sleep"), "step:")
	}

	if c.Metadata(durableSuspensionV1Capability) != "true" {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-c.Done():
			return c.Err()
		}
	}
	if c.activationWriter == nil {
		return newActivationError(
			ActivationErrorDurabilityUnavailable,
			"runtime negotiated durable_suspension_v1 but no activation writer is configured",
			"",
			0,
			nil,
		)
	}
	if _, completed := c.completedStepPayload(timerKey); completed {
		return nil
	}

	delayMS := duration.Milliseconds()
	if delayMS == 0 {
		delayMS = 1
	}
	canonicalInput, err := canonicalActivationValue(map[string]any{
		"delay_ms":  delayMS,
		"timer_key": timerKey,
	})
	if err != nil {
		return newActivationError(ActivationErrorInvalidArgument, err.Error(), "", 0, err)
	}
	inputDigest := sha256.Sum256(canonicalInput)
	definitionDigest, err := activationDefinitionDigestFromContext(c)
	if err != nil {
		return err
	}
	workerSessionID := c.Metadata("worker_session_id")
	if workerSessionID == "" {
		workerSessionID = c.Metadata("worker_id")
	}
	runAuthority := c.Metadata("run_authority")
	if runAuthority == "" {
		runAuthority = c.InvocationID()
	}
	leaseAuthority := c.Metadata("lease_authority")
	if leaseAuthority == "" {
		leaseAuthority = c.LeaseID()
	}
	if c.projectID == "" || c.RunID() == "" || workerSessionID == "" || runAuthority == "" || leaseAuthority == "" {
		return newActivationError(
			ActivationErrorDurabilityUnavailable,
			"durable sleep requires project, run, worker-session, run, and lease authority",
			"",
			0,
			nil,
		)
	}

	parentActivationID := parentActivationID(c)
	expectedID := activationID(
		c.projectID,
		c.RunID(),
		parentActivationID,
		pb.ActivationKind_ACTIVATION_KIND_TIMER,
		timerKey,
	)
	if c.Metadata("timer_key") == timerKey {
		if c.Metadata("activation_id") != expectedID {
			return newActivationError(
				ActivationErrorNondeterministicReplay,
				"timer resume authority does not match the deterministic sleep activation",
				c.Metadata("activation_id"),
				0,
				nil,
			)
		}
		c.recordCompletedStep(timerKey, []byte("null"))
		return nil
	}

	begin, err := c.activationWriter.BeginActivation(c, &pb.BeginActivationRequest{
		ProjectId:          c.projectID,
		RunId:              c.RunID(),
		ParentActivationId: parentActivationID,
		Kind:               pb.ActivationKind_ACTIVATION_KIND_TIMER,
		StableKey:          timerKey,
		InputDigest:        inputDigest[:],
		DefinitionDigest:   cloneBytes(definitionDigest),
		RecoveryPolicy:     pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_DURABLE_STEPS,
		WorkerSessionId:    workerSessionID,
		RunAuthority:       []byte(runAuthority),
		LeaseAuthority:     []byte(leaseAuthority),
	})
	if err != nil {
		return err
	}
	if begin.GetActivationId() != expectedID {
		return newActivationError(
			ActivationErrorUnknownOutcome,
			fmt.Sprintf("runtime returned activation ID %q, want %q", begin.GetActivationId(), expectedID),
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	if begin.GetOutcome() != pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE {
		return activationBeginDecisionError(begin, "sleep")
	}
	if begin.GetAttempt() == 0 || len(begin.GetFenceToken()) == 0 {
		return newActivationError(
			ActivationErrorUnknownOutcome,
			"EXECUTE receipt is missing fenced authority",
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	continuation, err := c.sleepContinuation()
	if err != nil {
		return err
	}
	return &durableSleepSuspensionError{suspension: &pb.WorkerSuspension{
		ActivationId:     begin.GetActivationId(),
		Attempt:          begin.GetAttempt(),
		FenceToken:       cloneBytes(begin.GetFenceToken()),
		TimerKey:         timerKey,
		InputDigest:      inputDigest[:],
		DefinitionDigest: cloneBytes(definitionDigest),
		Continuation:     continuation,
		DelayMs:          delayMS,
	}}
}

func activationBeginDecisionError(begin *pb.BeginActivationResponse, kind string) error {
	switch begin.GetOutcome() {
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_WAIT:
		return newActivationError(ActivationErrorContended, kind+" activation is already executing", begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CONFLICT:
		return newActivationError(ActivationErrorNondeterministicReplay, "stable "+kind+" key was reused with different durable semantics", begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CANCELLED:
		return newActivationError(ActivationErrorCancelled, kind+" activation was cancelled", begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_UNKNOWN_OUTCOME:
		return newActivationError(ActivationErrorUnknownOutcome, kind+" activation has an unknown outcome", begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY:
		return newActivationError(ActivationErrorUnknownOutcome, kind+" activation unexpectedly returned a terminal replay", begin.GetActivationId(), begin.GetAttempt(), nil)
	default:
		return newActivationError(ActivationErrorUnknownOutcome, "runtime returned an unspecified "+kind+" activation decision", begin.GetActivationId(), begin.GetAttempt(), nil)
	}
}

func (c *Context) sleepContinuation() ([]byte, error) {
	continuation := map[string]any{
		"workflow_correlation_id": c.componentCorrelationID(),
	}
	if completed := c.completedStepsJSON(); completed != "" {
		continuation["completed_steps"] = json.RawMessage(completed)
	}
	payload, err := json.Marshal(continuation)
	if err != nil {
		return nil, fmt.Errorf("agnt5: encode durable sleep continuation: %w", err)
	}
	return payload, nil
}

func mergeDurableContinuation(metadata map[string]string) map[string]string {
	out := cloneStringMap(metadata)
	encoded := out["continuation_b64"]
	if encoded == "" {
		return out
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return out
	}
	var continuation struct {
		CompletedSteps        json.RawMessage `json:"completed_steps"`
		WorkflowCorrelationID string          `json:"workflow_correlation_id"`
	}
	if err := json.Unmarshal(decoded, &continuation); err != nil {
		return out
	}
	if out["completed_steps"] == "" && len(continuation.CompletedSteps) > 0 {
		out["completed_steps"] = string(continuation.CompletedSteps)
	}
	if out["workflow_correlation_id"] == "" {
		out["workflow_correlation_id"] = continuation.WorkflowCorrelationID
	}
	return out
}
