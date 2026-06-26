package agnt5

import (
	"context"
	"errors"
	"testing"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
)

type recordingCheckpointWriter struct {
	responses []*pb.CheckpointResponse
	requests  []*pb.CheckpointRequest
	err       error
}

func (w *recordingCheckpointWriter) Checkpoint(_ context.Context, req *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	w.requests = append(w.requests, req)
	if w.err != nil {
		return nil, w.err
	}
	if len(w.responses) > 0 {
		resp := w.responses[0]
		w.responses = w.responses[1:]
		return resp, nil
	}
	return &pb.CheckpointResponse{Success: true}, nil
}

func TestStepWritesRuntimeCheckpoints(t *testing.T) {
	writer := &recordingCheckpointWriter{}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-step",
		RunID:         "run-step",
		ComponentName: "wf",
		ComponentType: ComponentTypeWorkflow,
	}, writer, "proj-1")

	got, err := Step(ctx, "load", func(context.Context) (string, error) {
		return "value", nil
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if got != "value" {
		t.Fatalf("step output: %q", got)
	}
	assertCheckpointTypes(t, writer.requests,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_COMPLETED,
	)
	completed := writer.requests[1].GetCheckpoint()
	if writer.requests[0].GetProjectId() != "proj-1" ||
		completed.GetRunId() != "run-step" ||
		completed.GetStepKey() != "step:load:0" ||
		completed.GetStepName() != "load" ||
		completed.GetStepType() != "workflow" ||
		string(completed.GetPayload()) != `"value"` {
		t.Fatalf("completed checkpoint: %#v", writer.requests[1])
	}
}

func TestStepReturnsMemoizedOutput(t *testing.T) {
	writer := &recordingCheckpointWriter{
		responses: []*pb.CheckpointResponse{{
			Success:      true,
			Memoized:     true,
			CachedOutput: []byte(`{"message":"cached"}`),
		}},
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-step",
		RunID:         "run-step",
		ComponentName: "wf",
		ComponentType: ComponentTypeWorkflow,
	}, writer, "proj-1")
	called := false

	got, err := Step(ctx, "load", func(context.Context) (greetOutput, error) {
		called = true
		return greetOutput{}, nil
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if called {
		t.Fatal("memoized step executed function")
	}
	if got.Message != "cached" {
		t.Fatalf("memoized output: %#v", got)
	}
	assertCheckpointTypes(t, writer.requests, pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED)
	if gotEvents := eventTypes(ctx.Events()); len(gotEvents) != 2 ||
		gotEvents[0] != "workflow.step.started" ||
		gotEvents[1] != "workflow.step.completed" {
		t.Fatalf("events: %#v", gotEvents)
	}
}

func TestStepReplaySkipsCompletedStepAndRunsMissingStep(t *testing.T) {
	writer := &recordingCheckpointWriter{
		responses: []*pb.CheckpointResponse{{
			Success:      true,
			Memoized:     true,
			CachedOutput: []byte(`"cached-first"`),
		}, {
			Success: true,
		}, {
			Success: true,
		}},
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-step",
		RunID:         "run-step",
		ComponentName: "wf",
		ComponentType: ComponentTypeWorkflow,
	}, writer, "proj-1")
	firstCalled := false
	secondCalled := false

	first, err := Step(ctx, "first", func(context.Context) (string, error) {
		firstCalled = true
		return "should-not-run", nil
	})
	if err != nil {
		t.Fatalf("first step: %v", err)
	}
	second, err := Step(ctx, "second", func(context.Context) (string, error) {
		secondCalled = true
		return "fresh-second", nil
	})
	if err != nil {
		t.Fatalf("second step: %v", err)
	}
	if firstCalled || !secondCalled {
		t.Fatalf("step calls: first=%t second=%t", firstCalled, secondCalled)
	}
	if first != "cached-first" || second != "fresh-second" {
		t.Fatalf("step outputs: first=%q second=%q", first, second)
	}
	assertCheckpointTypes(t, writer.requests,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_COMPLETED,
	)
	if writer.requests[0].GetCheckpoint().GetStepKey() != "step:first:0" ||
		writer.requests[1].GetCheckpoint().GetStepKey() != "step:second:0" {
		t.Fatalf("step keys: %#v %#v", writer.requests[0].GetCheckpoint(), writer.requests[1].GetCheckpoint())
	}
	events := ctx.Events()
	if len(events) < 2 {
		t.Fatalf("events: %#v", events)
	}
	completed, ok := events[1].Data.(map[string]any)
	if !ok || completed["cache_hit"] != true {
		t.Fatalf("cache-hit event: %#v", events[1])
	}
}

func TestStepWritesFailedCheckpoint(t *testing.T) {
	writer := &recordingCheckpointWriter{}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-step",
		RunID:         "run-step",
		ComponentName: "wf",
		ComponentType: ComponentTypeWorkflow,
	}, writer, "proj-1")
	stepErr := errors.New("boom")

	_, err := Step(ctx, "load", func(context.Context) (string, error) {
		return "", stepErr
	})
	if !errors.Is(err, stepErr) {
		t.Fatalf("step error: %v", err)
	}
	assertCheckpointTypes(t, writer.requests,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_STARTED,
		pb.CheckpointType_CHECKPOINT_TYPE_STEP_FAILED,
	)
	failed := writer.requests[1].GetCheckpoint()
	if failed.GetErrorMessage() != "boom" || failed.GetStepKey() != "step:load:0" {
		t.Fatalf("failed checkpoint: %#v", failed)
	}
}

func assertCheckpointTypes(t *testing.T, requests []*pb.CheckpointRequest, types ...pb.CheckpointType) {
	t.Helper()
	if len(requests) != len(types) {
		t.Fatalf("checkpoint count = %d, want %d: %#v", len(requests), len(types), requests)
	}
	for i, want := range types {
		if got := requests[i].GetCheckpoint().GetType(); got != want {
			t.Fatalf("checkpoint[%d] = %s, want %s", i, got, want)
		}
	}
}
