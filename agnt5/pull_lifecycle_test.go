package agnt5

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

type orderedLifecycleWriter struct {
	mu       sync.Mutex
	sequence []string
}

func (w *orderedLifecycleWriter) append(eventTypes ...string) {
	w.mu.Lock()
	w.sequence = append(w.sequence, eventTypes...)
	w.mu.Unlock()
}

func (w *orderedLifecycleWriter) Sequence() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.sequence...)
}

func (w *orderedLifecycleWriter) WriteEvent(_ context.Context, event journalEvent) error {
	w.append(event.EventType)
	return nil
}

func (w *orderedLifecycleWriter) WriteEvents(_ context.Context, events []journalEvent) error {
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, event.EventType)
	}
	w.append(types...)
	return nil
}

func (w *orderedLifecycleWriter) Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	return nil, errors.New("legacy checkpoint must not be used after durable activation negotiation")
}

func (w *orderedLifecycleWriter) BeginActivation(_ context.Context, request *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	w.append("workflow.step.started")
	return &pb.BeginActivationResponse{
		Outcome:      pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE,
		ActivationId: activationID(request.GetProjectId(), request.GetRunId(), request.GetParentActivationId(), request.GetKind(), request.GetStableKey()),
		Attempt:      1,
		FenceToken:   []byte("fence-1"),
	}, nil
}

func (w *orderedLifecycleWriter) CompleteActivation(_ context.Context, request *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	w.append("workflow.step.completed")
	return &pb.CompleteActivationResponse{
		Accepted:     true,
		ActivationId: request.GetActivationId(),
		Attempt:      request.GetAttempt(),
	}, nil
}

func (w *orderedLifecycleWriter) FailActivation(_ context.Context, request *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	w.append("workflow.step.failed")
	return &pb.FailActivationResponse{
		Accepted:     true,
		ActivationId: request.GetActivationId(),
		Attempt:      request.GetAttempt(),
	}, nil
}

func TestFoldedDurableTaskPreservesNestedLifecycleOrder(t *testing.T) {
	writer := &orderedLifecycleWriter{}
	worker := NewWorker(
		"svc",
		WithWorkerID("worker-1"),
		WithProjectID("project-1"),
		WithDurableActivationArtifact("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="),
		withEventWriter(writer),
	)
	worker.checkpointWriter = writer
	if err := worker.applyProtocolNegotiation([]string{durableActivationV1Capability}, nil); err != nil {
		t.Fatalf("negotiate durable activation: %v", err)
	}
	if err := RegisterWorkflow(worker, "workflow", func(ctx *Context, _ map[string]any) (string, error) {
		return Task(ctx, "child", "input", func(*Context, string) (string, error) {
			return "output", nil
		})
	}); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	const runID = "run-folded-task"
	worker.beginLifecycleFold(runID)
	responses := worker.dispatchServiceMessages(context.Background(), &pb.DispatchComponentRequest{
		InvocationId:  runID,
		ComponentType: pb.ComponentType_COMPONENT_TYPE_WORKFLOW,
		ComponentName: "workflow",
		InputData:     []byte(`{}`),
		Metadata: map[string]string{
			"dispatch_mode":     "pull",
			"worker_session_id": "session-1",
		},
		LeaseId: "lease-1",
	})
	response := requireDispatchResponse(t, responses)
	if !response.GetSuccess() {
		t.Fatalf("dispatch failed: %s", response.GetErrorMessage())
	}
	if held := worker.endLifecycleFold(runID); len(held) > 0 {
		if err := worker.writeEventsDirect(context.Background(), held); err != nil {
			t.Fatalf("append held lifecycle: %v", err)
		}
	}

	want := []string{
		"run.started",
		"workflow.started",
		"workflow.step.started",
		"function.started",
		"function.completed",
		"workflow.step.completed",
		"workflow.completed",
	}
	if got := writer.Sequence(); !reflect.DeepEqual(got, want) {
		t.Fatalf("journal sequence = %#v, want %#v", got, want)
	}
}

type blockingLifecycleWriter struct {
	flushStarted chan struct{}
	releaseFlush chan struct{}
	once         sync.Once
}

func (w *blockingLifecycleWriter) WriteEvent(ctx context.Context, event journalEvent) error {
	return w.WriteEvents(ctx, []journalEvent{event})
}

func (w *blockingLifecycleWriter) WriteEvents(ctx context.Context, _ []journalEvent) error {
	w.once.Do(func() { close(w.flushStarted) })
	select {
	case <-w.releaseFlush:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type notifyingActivationWriter struct {
	beginCalled chan struct{}
}

func (w *notifyingActivationWriter) Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	return &pb.CheckpointResponse{Success: true}, nil
}

func (w *notifyingActivationWriter) BeginActivation(_ context.Context, request *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	w.beginCalled <- struct{}{}
	return &pb.BeginActivationResponse{
		ActivationId: activationID(request.GetProjectId(), request.GetRunId(), request.GetParentActivationId(), request.GetKind(), request.GetStableKey()),
	}, nil
}

func (w *notifyingActivationWriter) CompleteActivation(_ context.Context, request *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	return &pb.CompleteActivationResponse{Accepted: true, ActivationId: request.GetActivationId()}, nil
}

func (w *notifyingActivationWriter) FailActivation(_ context.Context, request *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	return &pb.FailActivationResponse{Accepted: true, ActivationId: request.GetActivationId()}, nil
}

func TestConcurrentActivationCannotOvertakeLifecycleFlush(t *testing.T) {
	events := &blockingLifecycleWriter{
		flushStarted: make(chan struct{}),
		releaseFlush: make(chan struct{}),
	}
	activations := &notifyingActivationWriter{beginCalled: make(chan struct{}, 2)}
	worker := NewWorker("svc", withEventWriter(events))
	worker.checkpointWriter = activations
	const runID = "run-concurrent-flush"
	worker.beginLifecycleFold(runID)
	if err := worker.writeEvent(context.Background(), journalEvent{RunID: runID, EventType: "run.started"}); err != nil {
		t.Fatalf("hold root lifecycle: %v", err)
	}
	writer, ok := worker.foldingCheckpointWriterFor(runID).(stepActivationWriter)
	if !ok {
		t.Fatal("folding writer does not support activations")
	}

	errs := make(chan error, 2)
	begin := func() {
		_, err := writer.BeginActivation(context.Background(), &pb.BeginActivationRequest{RunId: runID})
		errs <- err
	}
	go begin()
	select {
	case <-events.flushStarted:
	case <-time.After(time.Second):
		t.Fatal("root lifecycle flush did not start")
	}
	go begin()

	premature := false
	select {
	case <-activations.beginCalled:
		premature = true
	case <-time.After(200 * time.Millisecond):
	}
	close(events.releaseFlush)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("begin activation: %v", err)
		}
	}
	if premature {
		t.Fatal("activation began before the root lifecycle flush was acknowledged")
	}
}
