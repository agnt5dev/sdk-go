package agnt5

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
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

func TestTaskEmitsNestedFunctionLifecycle(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-task",
		RunID:         "run-task",
		ComponentName: "workflow",
		ComponentType: ComponentTypeWorkflow,
	}, nil, "")
	ctx.setParentCorrelationID("workflow-cid")

	got, err := Task(ctx, "ks_analyze_text", "one", func(_ *Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})
	if err != nil {
		t.Fatalf("task: %v", err)
	}
	if got != "ONE" {
		t.Fatalf("task output = %q", got)
	}
	events := ctx.Events()
	if gotTypes := eventTypes(events); !slices.Equal(gotTypes, []string{
		"workflow.step.started",
		"function.started",
		"function.completed",
		"workflow.step.completed",
	}) {
		t.Fatalf("events = %#v", gotTypes)
	}
	stepCID := events[0].CorrelationID
	functionCID := events[1].CorrelationID
	if stepCID == "" || functionCID == "" ||
		events[0].ParentCorrelationID != "workflow-cid" ||
		events[1].ParentCorrelationID != stepCID ||
		events[2].CorrelationID != functionCID ||
		events[2].ParentCorrelationID != stepCID ||
		events[3].CorrelationID != stepCID ||
		events[3].ParentCorrelationID != "workflow-cid" {
		t.Fatalf("lifecycle correlation: %#v", events)
	}
	for _, event := range events[1:3] {
		data, ok := event.Data.(map[string]any)
		if !ok || data["name"] != "ks_analyze_text" {
			t.Fatalf("function data: %#v", event.Data)
		}
	}
}

func TestTaskCorrelationIsExplicitUnderConcurrency(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-task",
		RunID:         "run-task",
		ComponentName: "workflow",
		ComponentType: ComponentTypeWorkflow,
	}, nil, "")
	ctx.setParentCorrelationID("workflow-cid")

	var wg sync.WaitGroup
	for index := 0; index < 3; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := Task(ctx, "ks_analyze_text", index, func(_ *Context, input int) (int, error) {
				return input, nil
			}); err != nil {
				t.Errorf("task %d: %v", index, err)
			}
		}()
	}
	wg.Wait()

	steps := make(map[string]struct{})
	for _, event := range ctx.Events() {
		if event.Type == "workflow.step.started" {
			if event.ParentCorrelationID != "workflow-cid" {
				t.Fatalf("step parent = %q", event.ParentCorrelationID)
			}
			steps[event.CorrelationID] = struct{}{}
		}
	}
	if len(steps) != 3 {
		t.Fatalf("step correlations = %#v", steps)
	}
	for _, event := range ctx.Events() {
		if !strings.HasPrefix(event.Type, "function.") {
			continue
		}
		if _, ok := steps[event.ParentCorrelationID]; !ok {
			t.Fatalf("%s has unknown parent %q", event.Type, event.ParentCorrelationID)
		}
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
