package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
)

// Step runs a named unit of work and memoizes successful output when the worker
// is connected to an AGNT5 engine. Without an engine checkpoint writer it keeps
// local behavior: execute the function and emit step lifecycle events.
func Step[T any](ctx *Context, name string, fn func(context.Context) (T, error)) (T, error) {
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
	_ = ctx.Emit(Event{
		Type: "workflow.step.started",
		Data: map[string]any{"name": name, "step_key": stepKey},
	})

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
				Type: "workflow.step.completed",
				Data: map[string]any{"name": name, "step_key": stepKey, "cache_hit": true},
			})
			return cached, nil
		}
	}

	startedAt := time.Now()
	out, err := fn(ctx)
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
			Type: "workflow.step.failed",
			Data: map[string]any{"name": name, "step_key": stepKey, "error": err.Error()},
		})
		return zero, err
	}

	payload, marshalErr := json.Marshal(out)
	if marshalErr != nil {
		return zero, fmt.Errorf("agnt5: encode step %q output: %w", name, marshalErr)
	}
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
		Type: "workflow.step.completed",
		Data: map[string]any{"name": name, "step_key": stepKey, "cache_hit": false},
	})
	return out, nil
}
