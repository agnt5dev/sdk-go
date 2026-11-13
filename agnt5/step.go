package agnt5

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// Step runs a named unit of work and memoizes successful output when the worker
// is connected to an AGNT5 engine. Without an engine checkpoint writer it keeps
// local behavior: execute the function and emit step lifecycle events.
func Step[T any](ctx *Context, name string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if fn == nil {
		return zero, ErrNilHandler
	}
	return runStep(ctx, name, "", nil, func(stepContext context.Context, _ string) (T, error) {
		return fn(stepContext)
	})
}

// StepWithKey runs a named durable step under an explicit stable key. Explicit
// keys are required for fan-out, parallel branches, reordered collections, and
// repeated same-name work where a sequential ordinal is not deterministic.
func StepWithKey[T any](ctx *Context, name, key string, fn func(*Context) (T, error)) (T, error) {
	var zero T
	if fn == nil {
		return zero, ErrNilHandler
	}
	if strings.TrimSpace(key) == "" {
		return zero, newActivationError(ActivationErrorInvalidArgument, "explicit step key is required", "", 0, nil)
	}
	return runStep(ctx, name, key, nil, func(stepContext context.Context, stepCorrelationID string) (T, error) {
		return fn(stepScope(ctx, stepContext).withParentCorrelationID(stepCorrelationID))
	})
}

// Task invokes a registered-style handler as a durable workflow step. In
// addition to the step boundary (a journaled activation record when the
// runtime negotiated durable activations, workflow.step events otherwise), it
// emits a function lifecycle child so Studio can show each nested component
// consistently across SDKs.
//
// When the named function was registered with WithRetry, Task retries it with
// that policy and hands every attempt a fresh JSON copy of the input, as a
// declared step does. Typed fields decode as usual; a number held in an
// interface-typed field arrives as json.Number so a large integer keeps its
// exact value. A retried attempt runs the whole handler again, including nested steps that
// are numbered by call order; give nested work an explicit key (StepWithKey,
// TaskWithKey) when it must not run twice.
func Task[TInput any, TOutput any](
	ctx *Context,
	name string,
	input TInput,
	fn func(*Context, TInput) (TOutput, error),
) (TOutput, error) {
	return runTask(ctx, name, "", input, fn)
}

// TaskWithKey invokes a registered-style handler as a durable workflow step
// under an explicit stable key. Use it for parallel branches, reordered
// collections, and repeated same-name work where call-order ordinals are not
// deterministic.
func TaskWithKey[TInput any, TOutput any](
	ctx *Context,
	name string,
	key string,
	input TInput,
	fn func(*Context, TInput) (TOutput, error),
) (TOutput, error) {
	var zero TOutput
	if strings.TrimSpace(key) == "" {
		return zero, newActivationError(ActivationErrorInvalidArgument, "explicit task key is required", "", 0, nil)
	}
	return runTask(ctx, name, key, input, fn)
}

func runTask[TInput any, TOutput any](
	ctx *Context,
	name string,
	key string,
	input TInput,
	fn func(*Context, TInput) (TOutput, error),
) (TOutput, error) {
	var zero TOutput
	if fn == nil {
		return zero, ErrNilHandler
	}
	policy := taskRetryPolicy(ctx, name)
	// A retried attempt must see the input the first one was given, even if an
	// earlier attempt changed a map or slice inside it before failing.
	var snapshot []byte
	if policy != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			// Without a copy, a retry would see whatever an earlier attempt
			// did to the input, so refuse to start instead.
			return zero, fmt.Errorf("agnt5: snapshot task %q input: %w", name, err)
		}
		snapshot = encoded
	}
	return runStep(ctx, name, key, input, func(stepContext context.Context, stepCorrelationID string) (TOutput, error) {
		ctx := stepScope(ctx, stepContext)
		input := input
		if snapshot != nil {
			var fresh TInput
			// UseNumber keeps a number held in an interface-typed field exact;
			// the default decoder would round a large integer through float64.
			decoder := json.NewDecoder(bytes.NewReader(snapshot))
			decoder.UseNumber()
			if err := decoder.Decode(&fresh); err != nil {
				return zero, fmt.Errorf("agnt5: snapshot task %q input: %w", name, err)
			}
			input = fresh
		}
		functionCorrelationID := newCorrelationID("function")
		startedAt := time.Now()
		_ = ctx.Emit(Event{
			Type:                "function.started",
			CorrelationID:       functionCorrelationID,
			ParentCorrelationID: stepCorrelationID,
			SourceTimestampNS:   startedAt.UnixNano(),
			Data: map[string]any{
				"name":                  name,
				"component_type":        "function",
				"correlation_id":        functionCorrelationID,
				"parent_correlation_id": stepCorrelationID,
				"input_data":            input,
				"attempt":               ctx.Attempt(),
				"timestamp_ns":          startedAt.UnixNano(),
			},
		})
		out, err := fn(ctx.withParentCorrelationID(functionCorrelationID), input)
		durationMS := time.Since(startedAt).Milliseconds()
		timestampNS := time.Now().UnixNano()
		if err != nil {
			_ = ctx.Emit(Event{
				Type:                "function.failed",
				CorrelationID:       functionCorrelationID,
				ParentCorrelationID: stepCorrelationID,
				SourceTimestampNS:   timestampNS,
				Data: map[string]any{
					"name":                  name,
					"component_type":        "function",
					"correlation_id":        functionCorrelationID,
					"parent_correlation_id": stepCorrelationID,
					"error_message":         err.Error(),
					"duration_ms":           durationMS,
					"timestamp_ns":          timestampNS,
				},
			})
			return zero, err
		}
		_ = ctx.Emit(Event{
			Type:                "function.completed",
			CorrelationID:       functionCorrelationID,
			ParentCorrelationID: stepCorrelationID,
			SourceTimestampNS:   timestampNS,
			Data: map[string]any{
				"name":                  name,
				"component_type":        "function",
				"correlation_id":        functionCorrelationID,
				"parent_correlation_id": stepCorrelationID,
				"output_data":           out,
				"duration_ms":           durationMS,
				"timestamp_ns":          timestampNS,
			},
		})
		return out, nil
	}, policy)
}

// stepScope returns the step-scoped Context handed to the step body when the
// runner produced one (durable activations scope nested work to the
// activation), falling back to the caller's Context otherwise.
func stepScope(ctx *Context, stepContext context.Context) *Context {
	if scoped, ok := stepContext.(*Context); ok && scoped != nil {
		return scoped
	}
	return ctx
}

func runStep[T any](ctx *Context, name, explicitKey string, input any, fn func(context.Context, string) (T, error), policies ...*StepConfig) (out T, err error) {
	var zero T
	var policy *StepConfig
	if len(policies) > 0 {
		policy = policies[0]
	}
	if ctx == nil {
		return zero, context.Canceled
	}
	if fn == nil {
		return zero, ErrNilHandler
	}
	if strings.TrimSpace(name) == "" {
		return zero, ErrInvalidStepName
	}
	ctx, finishTelemetry := ctx.startTelemetrySpan("workflow.step." + name)
	defer finishTelemetryScope(finishTelemetry, &err)
	stepKey := ""
	if explicitKey != "" {
		stepKey = "step:" + name + ":" + explicitKey
	} else {
		stepKey = ctx.nextStepKey(name)
	}
	plan, activationEnabled, err := activationPlanForStep(ctx, stepKey, input)
	if err != nil {
		return zero, err
	}
	if policy != nil {
		return runStepWithRetries(ctx, name, stepKey, plan, activationEnabled, fn, policy)
	}
	if activationEnabled {
		return runActivatedStep(ctx, name, stepKey, plan, fn)
	}
	return runLegacyStep(ctx, name, stepKey, fn)
}

// taskRetryPolicy returns the retry policy the named function was registered
// with (WithRetry, WithBackoff), or nil when it has none. The runtime applies
// that policy to a function run on its own; a workflow calls the handler in
// process, so Task has to apply it here or the first error fails the step.
func taskRetryPolicy(ctx *Context, name string) *StepConfig {
	if ctx == nil || ctx.shared == nil || ctx.shared.registry == nil {
		return nil
	}
	component, ok := ctx.shared.registry.Get(name)
	if !ok || component.Type != ComponentTypeFunction {
		return nil
	}
	attempts, ok := int32Config(component.Config, "max_attempts")
	if !ok || attempts < 2 {
		return nil
	}
	policy := &StepConfig{MaxAttempts: int(attempts), MaxInterval: time.Minute, Backoff: "exponential", Multiplier: 2}
	if value, ok := int32Config(component.Config, "initial_interval_ms"); ok && value > 0 {
		policy.InitialInterval = time.Duration(value) * time.Millisecond
	}
	if value, ok := int32Config(component.Config, "max_interval_ms"); ok && value > 0 {
		policy.MaxInterval = time.Duration(value) * time.Millisecond
	}
	if policy.MaxInterval < policy.InitialInterval {
		policy.MaxInterval = policy.InitialInterval
	}
	switch component.Config["backoff_type"] {
	case "constant", "linear", "exponential":
		policy.Backoff = component.Config["backoff_type"]
	}
	if value, ok := float64Config(component.Config, "backoff_multiplier"); ok && value > 0 {
		policy.Multiplier = value
	}
	return policy
}

// runStepWithRetries runs one step until it succeeds or its attempts are
// spent. With durable activations every attempt is admitted and recorded by
// the runtime under the same step identity; without them the attempts are
// counted here.
func runStepWithRetries[T any](ctx *Context, name, stepKey string, plan activationPlan, activationEnabled bool, fn func(context.Context, string) (T, error), policy *StepConfig) (T, error) {
	var zero T
	if activationEnabled {
		// Keep the step in flight across the backoff, so a sibling durable wait
		// does not release the run's lease between two attempts.
		reservation := newCorrelationID("step-inflight")
		ctx.shared.activity.beginStarted()
		ctx.shared.activity.beginFinished(reservation, true)
		defer ctx.shared.activity.settled(reservation)
		ctx = ctx.withParentCorrelationID(ctx.parentCID)
		ctx.enclosingActivations = append(slices.Clip(ctx.enclosingActivations), reservation)
	}
	for attempt := uint32(1); ; attempt++ {
		var result T
		var err error
		if activationEnabled {
			result, err = runActivatedStep(ctx, name, stepKey, plan, fn, policy)
			var retry *stepRetryReady
			if !errors.As(err, &retry) {
				return result, err
			}
			attempt = retry.attempt
		} else {
			// Give the handler its own zero-based attempt number, as the
			// activated path does, so ctx.Attempt() advances across retries.
			scope := ctx.withParentCorrelationID(ctx.parentCID)
			stepAttempt := int(attempt) - 1
			scope.stepAttempt = &stepAttempt
			// Only the handler's own error is retried. A checkpoint that fails
			// after the handler succeeded must reach the workflow: the output
			// is already cached locally, so another pass would return it as a
			// success that was never persisted.
			handlerFailed := false
			result, err = runLegacyStep(scope, name, stepKey, func(stepContext context.Context, correlationID string) (T, error) {
				out, handlerErr := fn(stepContext, correlationID)
				handlerFailed = handlerErr != nil
				return out, handlerErr
			})
			var activationErr *ActivationError
			var suspension *durableSleepSuspensionError
			if err == nil || !handlerFailed || int(attempt) >= policy.MaxAttempts || ctx.Err() != nil ||
				errors.As(err, &activationErr) || errors.As(err, &suspension) || IsWaitingForUserInput(err) {
				return result, err
			}
		}
		timer := time.NewTimer(policy.delay(attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		}
	}
}

func runLegacyStep[T any](ctx *Context, name, stepKey string, fn func(context.Context, string) (T, error)) (T, error) {
	var zero T
	stepType := string(ctx.ComponentType())
	if stepType == "" {
		stepType = "function"
	}
	if payload, ok := ctx.completedStepPayload(stepKey); ok && ctx.isHITLReplay() {
		var cached T
		if err := json.Unmarshal(payload, &cached); err != nil {
			return zero, fmt.Errorf("agnt5: decode replayed step %q: %w", name, err)
		}
		return cached, nil
	}
	stepCorrelationID := newCorrelationID("step")
	parentCorrelationID := ctx.parentCorrelationID()
	startedTimestampNS := time.Now().UnixNano()
	_ = ctx.Emit(Event{
		Type:                "workflow.step.started",
		CorrelationID:       stepCorrelationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   startedTimestampNS,
		Data: map[string]any{
			"name":                  name,
			"step_key":              stepKey,
			"component_type":        "step",
			"correlation_id":        stepCorrelationID,
			"parent_correlation_id": parentCorrelationID,
			"timestamp_ns":          startedTimestampNS,
		},
	})

	if payload, ok := ctx.completedStepPayload(stepKey); ok {
		var cached T
		if err := json.Unmarshal(payload, &cached); err != nil {
			return zero, fmt.Errorf("agnt5: decode replayed step %q: %w", name, err)
		}
		_ = ctx.Emit(Event{
			Type:                "workflow.step.completed",
			CorrelationID:       stepCorrelationID,
			ParentCorrelationID: parentCorrelationID,
			SourceTimestampNS:   time.Now().UnixNano(),
			Data: map[string]any{
				"name":                  name,
				"step_key":              stepKey,
				"cache_hit":             true,
				"component_type":        "step",
				"correlation_id":        stepCorrelationID,
				"parent_correlation_id": parentCorrelationID,
			},
		})
		return cached, nil
	}

	if ctx.checkpointWriter != nil && ctx.projectID != "" {
		started, err := ctx.checkpointWriter.Checkpoint(ctx, &pb.CheckpointRequest{
			ProjectId: ctx.projectID,
			Checkpoint: &pb.DurableStepCheckpoint{
				RunId:    ctx.RunID(),
				StepKey:  stepKey,
				StepName: name,
				StepType: stepType,
				Type:     pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED,
			},
		})
		if err != nil {
			return zero, err
		}
		if started.GetMemoized() {
			var cached T
			if err := json.Unmarshal(started.GetCachedOutput(), &cached); err != nil {
				return zero, fmt.Errorf("agnt5: decode memoized step %q: %w", name, err)
			}
			_ = ctx.Emit(Event{
				Type:                "workflow.step.completed",
				CorrelationID:       stepCorrelationID,
				ParentCorrelationID: parentCorrelationID,
				SourceTimestampNS:   time.Now().UnixNano(),
				Data: map[string]any{
					"name":                  name,
					"step_key":              stepKey,
					"cache_hit":             true,
					"component_type":        "step",
					"correlation_id":        stepCorrelationID,
					"parent_correlation_id": parentCorrelationID,
				},
			})
			ctx.recordCompletedStep(stepKey, started.GetCachedOutput())
			return cached, nil
		}
	}

	startedAt := time.Now()
	out, err := fn(ctx, stepCorrelationID)
	if err != nil {
		if ctx.checkpointWriter != nil && ctx.projectID != "" {
			_, _ = ctx.checkpointWriter.Checkpoint(ctx, &pb.CheckpointRequest{
				ProjectId: ctx.projectID,
				Checkpoint: &pb.DurableStepCheckpoint{
					RunId:        ctx.RunID(),
					StepKey:      stepKey,
					StepName:     name,
					StepType:     stepType,
					Type:         pb.CheckpointType_CHECKPOINT_TYPE_STEP_FAILED,
					ErrorMessage: err.Error(),
					ErrorType:    fmt.Sprintf("%T", err),
					LatencyMs:    time.Since(startedAt).Milliseconds(),
				},
			})
		}
		_ = ctx.Emit(Event{
			Type:                "workflow.step.failed",
			CorrelationID:       stepCorrelationID,
			ParentCorrelationID: parentCorrelationID,
			SourceTimestampNS:   time.Now().UnixNano(),
			Data: map[string]any{
				"name":                  name,
				"step_key":              stepKey,
				"error":                 err.Error(),
				"component_type":        "step",
				"correlation_id":        stepCorrelationID,
				"parent_correlation_id": parentCorrelationID,
			},
		})
		return zero, err
	}

	payload, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		return zero, fmt.Errorf("agnt5: encode step %q output: %w", name, marshalErr)
	}
	ctx.recordCompletedStep(stepKey, payload)
	if ctx.checkpointWriter != nil && ctx.projectID != "" {
		if _, err := ctx.checkpointWriter.Checkpoint(ctx, &pb.CheckpointRequest{
			ProjectId: ctx.projectID,
			Checkpoint: &pb.DurableStepCheckpoint{
				RunId:     ctx.RunID(),
				StepKey:   stepKey,
				StepName:  name,
				StepType:  stepType,
				Type:      pb.CheckpointType_CHECKPOINT_TYPE_STEP_COMPLETED,
				Payload:   payload,
				LatencyMs: time.Since(startedAt).Milliseconds(),
			},
		}); err != nil {
			return zero, err
		}
	}
	_ = ctx.Emit(Event{
		Type:                "workflow.step.completed",
		CorrelationID:       stepCorrelationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   time.Now().UnixNano(),
		Data: map[string]any{
			"name":                  name,
			"step_key":              stepKey,
			"cache_hit":             false,
			"component_type":        "step",
			"correlation_id":        stepCorrelationID,
			"parent_correlation_id": parentCorrelationID,
		},
	})
	return out, nil
}

func runActivatedStep[T any](ctx *Context, name, stepKey string, plan activationPlan, fn func(context.Context, string) (T, error), policies ...*StepConfig) (T, error) {
	var policy *StepConfig
	if len(policies) > 0 {
		policy = policies[0]
	}
	if policy != nil {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
	}
	var zero T
	workerSessionID := ctx.Metadata("worker_session_id")
	if workerSessionID == "" {
		workerSessionID = ctx.Metadata("worker_id")
	}
	runAuthority := ctx.Metadata("run_authority")
	if runAuthority == "" {
		runAuthority = ctx.InvocationID()
	}
	leaseAuthority := ctx.Metadata("lease_authority")
	if leaseAuthority == "" {
		leaseAuthority = ctx.LeaseID()
	}
	if ctx.projectID == "" || ctx.RunID() == "" || workerSessionID == "" || runAuthority == "" || leaseAuthority == "" {
		return zero, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"durable activation requires project, run, worker-session, run, and lease authority",
			"",
			0,
			nil,
		)
	}

	parentActivationID := parentActivationID(ctx)
	expectedActivationID := activationID(ctx.projectID, ctx.RunID(), parentActivationID, pb.ActivationKind_ACTIVATION_KIND_STEP, plan.stableKey)
	begin, err := ctx.activationWriter.BeginActivation(ctx, &pb.BeginActivationRequest{
		ProjectId:                  ctx.projectID,
		RunId:                      ctx.RunID(),
		ParentActivationId:         parentActivationID,
		Kind:                       pb.ActivationKind_ACTIVATION_KIND_STEP,
		StableKey:                  plan.stableKey,
		InputDigest:                cloneBytes(plan.inputDigest),
		DefinitionDigest:           cloneBytes(plan.definitionDigest),
		RecoveryPolicy:             pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_DURABLE_STEPS,
		WorkerSessionId:            workerSessionID,
		RunAuthority:               []byte(runAuthority),
		LeaseAuthority:             []byte(leaseAuthority),
		DisplayName:                name,
		DisplayParentCorrelationId: ctx.displayParentCorrelationID(),
		InputData: activationInputData(map[string]any{
			"step_name": name,
			"step_key":  stepKey,
			"input":     plan.input,
		}),
	})
	if err != nil {
		return zero, err
	}
	// Every way out ends this activation's count, including an output that can't be encoded.
	defer ctx.shared.activity.settledIfAdmitted(begin)
	if policy != nil {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	if begin.GetActivationId() != expectedActivationID {
		return zero, newActivationError(
			ActivationErrorUnknownOutcome,
			fmt.Sprintf("runtime returned activation ID %q, want %q", begin.GetActivationId(), expectedActivationID),
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}

	switch begin.GetOutcome() {
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY:
		payload, payloadErr := inlineActivationBytes(begin.GetReplayResult())
		if payloadErr != nil {
			return zero, payloadErr
		}
		var cached T
		if err := json.Unmarshal(payload, &cached); err != nil {
			return zero, fmt.Errorf("agnt5: decode replayed activation step %q: %w", name, err)
		}
		// A replayed begin appends nothing to the journal; the accepted
		// record already carries the step boundary.
		ctx.recordCompletedStep(stepKey, payload)
		return cached, nil
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_WAIT:
		message := "activation is already executing"
		if wait := begin.GetWait(); wait != nil {
			message = fmt.Sprintf("activation is already executing in attempt %d", wait.GetAttempt())
		}
		return zero, newActivationError(ActivationErrorContended, message, begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CONFLICT:
		message := "stable step key was reused with different input or definition"
		if conflict := begin.GetConflict(); conflict != nil && conflict.GetMessage() != "" {
			message = conflict.GetMessage()
		}
		return zero, newActivationError(ActivationErrorNondeterministicReplay, message, begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CANCELLED:
		return zero, newActivationError(ActivationErrorCancelled, "activation was cancelled", begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_UNKNOWN_OUTCOME:
		message := "activation has an unknown external outcome"
		if unknown := begin.GetUnknownOutcome(); unknown != nil && unknown.GetErrorCode() != "" {
			message += ": " + unknown.GetErrorCode()
		}
		return zero, newActivationError(ActivationErrorUnknownOutcome, message, begin.GetActivationId(), begin.GetAttempt(), nil)
	case pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE:
		if begin.GetAttempt() == 0 || len(begin.GetFenceToken()) == 0 {
			return zero, newActivationError(ActivationErrorUnknownOutcome, "EXECUTE receipt is missing fenced authority", begin.GetActivationId(), begin.GetAttempt(), nil)
		}
	default:
		return zero, newActivationError(ActivationErrorUnknownOutcome, "runtime returned an unspecified activation decision", begin.GetActivationId(), begin.GetAttempt(), nil)
	}

	// The activation record is the step boundary: its ID is the correlation
	// ID nested events parent to, and the runtime journals started/completed/
	// failed from the activation RPCs themselves.
	execution := ActivationExecution{
		ActivationID:   begin.GetActivationId(),
		Attempt:        begin.GetAttempt(),
		IdempotencyKey: "agnt5:" + begin.GetActivationId(),
	}
	startedAt := time.Now()
	out, userErr := func() (result T, err error) {
		timer := startCoreTimer(ctx, "business")
		defer func() { timer.finish(ctx, err) }()
		scope := ctx.withActivationExecution(execution)
		if policy != nil {
			attempt := int(begin.GetAttempt()) - 1
			scope.stepAttempt = &attempt
		}
		result, err = fn(scope, begin.GetActivationId())
		if err == nil {
			timer.outcome = "success"
		}
		return result, err
	}()
	if policy != nil {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	if userErr != nil {
		if policy != nil {
			var activationErr *ActivationError
			var suspension *durableSleepSuspensionError
			if errors.As(userErr, &activationErr) || errors.As(userErr, &suspension) || IsWaitingForUserInput(userErr) {
				return zero, userErr
			}
		}
		retryable := policy != nil && int(begin.GetAttempt()) < policy.MaxAttempts
		errorData, _ := json.Marshal(map[string]string{"message": userErr.Error(), "type": fmt.Sprintf("%T", userErr)})
		failed, failErr := ctx.activationWriter.FailActivation(ctx, &pb.FailActivationRequest{
			ProjectId:                ctx.projectID,
			RunId:                    ctx.RunID(),
			ActivationId:             begin.GetActivationId(),
			Attempt:                  begin.GetAttempt(),
			FenceToken:               cloneBytes(begin.GetFenceToken()),
			ErrorCode:                "STEP_FAILED",
			ErrorData:                inlineActivationPayload(errorData),
			Retryable:                retryable,
			ExternalOutcomeCertainty: pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN,
			LatencyMs:                time.Since(startedAt).Milliseconds(),
		})
		if failErr != nil {
			return zero, fmt.Errorf("agnt5: record failure for step %q: %w (user error: %v)", name, failErr, userErr)
		}
		if !failed.GetAccepted() || (policy != nil && (failed.GetActivationId() != begin.GetActivationId() || failed.GetAttempt() != begin.GetAttempt())) {
			return zero, newActivationError(ActivationErrorUnknownOutcome, "runtime did not accept the step failure", begin.GetActivationId(), begin.GetAttempt(), userErr)
		}
		if retryable && failed.GetStatus() == pb.ActivationStatus_ACTIVATION_STATUS_RETRY_READY {
			return zero, &stepRetryReady{cause: userErr, attempt: begin.GetAttempt()}
		}
		return zero, userErr
	}

	payload, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		return zero, fmt.Errorf("agnt5: encode step %q output: %w", name, marshalErr)
	}
	outputDigest := sha256.Sum256(payload)
	completed, err := ctx.activationWriter.CompleteActivation(ctx, &pb.CompleteActivationRequest{
		ProjectId:    ctx.projectID,
		RunId:        ctx.RunID(),
		ActivationId: begin.GetActivationId(),
		Attempt:      begin.GetAttempt(),
		FenceToken:   cloneBytes(begin.GetFenceToken()),
		Output:       inlineActivationPayload(payload),
		OutputDigest: outputDigest[:],
		Usage:        &pb.ActivationUsage{LatencyMs: time.Since(startedAt).Milliseconds()},
	})
	if err != nil {
		return zero, err
	}
	if policy != nil {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	if !completed.GetAccepted() || completed.GetActivationId() != begin.GetActivationId() || completed.GetAttempt() != begin.GetAttempt() {
		return zero, newActivationError(ActivationErrorUnknownOutcome, "runtime returned an invalid completion receipt", begin.GetActivationId(), begin.GetAttempt(), nil)
	}
	ctx.recordCompletedStep(stepKey, payload)
	return out, nil
}
