package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
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
	return runStep(ctx, name, func(stepContext context.Context, _ string) (T, error) {
		return fn(stepContext)
	})
}

// Task invokes a registered-style handler as a durable workflow step. In
// addition to workflow.step lifecycle events, it emits a function lifecycle
// child so Studio can show each nested component consistently across SDKs.
func Task[TInput any, TOutput any](
	ctx *Context,
	name string,
	input TInput,
	fn func(*Context, TInput) (TOutput, error),
) (TOutput, error) {
	var zero TOutput
	if fn == nil {
		return zero, ErrNilHandler
	}
	return runStep(ctx, name, func(_ context.Context, stepCorrelationID string) (TOutput, error) {
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
	})
}

func runStep[T any](ctx *Context, name string, fn func(context.Context, string) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		return zero, context.Canceled
	}
	if fn == nil {
		return zero, ErrNilHandler
	}
	if strings.TrimSpace(name) == "" {
		return zero, ErrInvalidStepName
	}
	stepKey := ctx.nextStepKey(name)
	stepType := string(ctx.ComponentType())
	if stepType == "" {
		stepType = "function"
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
