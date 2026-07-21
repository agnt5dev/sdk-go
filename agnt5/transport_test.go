package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testCoordinator struct {
	pb.UnimplementedWorkerCoordinatorServiceServer

	ack           bool
	reject        string
	dispatch      *pb.DispatchComponentRequest
	cancel        *pb.CancelExecutionRequest
	received      chan *pb.ServiceMessage
	response      chan *pb.DispatchComponentResponse
	responseCount int
}

func (s *testCoordinator) WorkerStream(stream grpc.BidiStreamingServer[pb.ServiceMessage, pb.RuntimeMessage]) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	s.received <- message
	err = stream.Send(&pb.RuntimeMessage{
		WorkerId:    message.GetWorkerId(),
		MessageType: pb.RuntimeMessageType_REGISTER_SERVICE_RESPONSE,
		MessageData: &pb.RuntimeMessage_RegisterServiceResponse{
			RegisterServiceResponse: &pb.RegisterServiceResponse{
				Ack:   s.ack,
				Error: s.reject,
			},
		},
	})
	if err != nil || s.dispatch == nil {
		return err
	}
	if err := stream.Send(&pb.RuntimeMessage{
		WorkerId:    message.GetWorkerId(),
		MessageType: pb.RuntimeMessageType_INVOKE_FUNCTION,
		MessageData: &pb.RuntimeMessage_DispatchComponent{
			DispatchComponent: s.dispatch,
		},
	}); err != nil {
		return err
	}
	if s.cancel != nil {
		if err := stream.Send(&pb.RuntimeMessage{
			WorkerId:    message.GetWorkerId(),
			MessageType: pb.RuntimeMessageType_CANCEL_EXECUTION,
			MessageData: &pb.RuntimeMessage_CancelExecution{
				CancelExecution: s.cancel,
			},
		}); err != nil {
			return err
		}
	}
	responseCount := s.responseCount
	if responseCount == 0 {
		responseCount = 1
	}
	for range responseCount {
		responseMessage, err := stream.Recv()
		if err != nil {
			return err
		}
		s.response <- responseMessage.GetFunctionResponse()
	}
	return nil
}

func TestRegisterOnceSendsServiceRegistration(t *testing.T) {
	server := &testCoordinator{
		ack:      true,
		received: make(chan *pb.ServiceMessage, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithServiceVersion("1.2.3"),
		WithServiceType("go-app"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithMaxConcurrency(7),
		WithMetadata(map[string]string{"owner": "sdk"}),
	)
	if err := RegisterWorkflow(worker, "alpha", func(*Context, map[string]string) (map[string]string, error) {
		return map[string]string{"ok": "true"}, nil
	},
		WithCron("*/5 * * * *"),
		WithTriggers(EventTrigger("user.created")),
	); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunction(worker, "zeta", func(*Context, map[string]string) (map[string]string, error) {
		return map[string]string{"ok": "true"}, nil
	},
		WithRetry(3, 100, 5000),
		WithBackoff("exponential", 2),
		WithComponentMetadata(map[string]string{"kind": "fn"}),
	); err != nil {
		t.Fatal(err)
	}

	if err := worker.registerOnce(context.Background(), client); err != nil {
		t.Fatalf("register once: %v", err)
	}

	message := <-server.received
	if message.GetWorkerId() != "worker-1" {
		t.Fatalf("worker id: %q", message.GetWorkerId())
	}
	if message.GetMetadata()["project_id"] != "proj-1" || message.GetMetadata()["deployment_id"] != "dep-1" {
		t.Fatalf("message metadata: %#v", message.GetMetadata())
	}
	registration := message.GetRegisterService()
	if registration == nil {
		t.Fatal("missing register service message")
	}
	if registration.GetServiceName() != "svc" ||
		registration.GetServiceVersion() != "1.2.3" ||
		registration.GetServiceType() != "go-app" {
		t.Fatalf("service identity: %#v", registration)
	}
	if registration.GetMode() != pb.WorkerMode_WORKER_MODE_PULL {
		t.Fatalf("worker mode: %s", registration.GetMode())
	}
	if registration.GetDeploymentId() != "dep-1" {
		t.Fatalf("deployment id: %q", registration.GetDeploymentId())
	}
	if registration.GetMaxConcurrency() != 7 {
		t.Fatalf("max concurrency: %d", registration.GetMaxConcurrency())
	}
	if registration.GetMetadata()["owner"] != "sdk" ||
		registration.GetMetadata()["project_id"] != "proj-1" ||
		registration.GetMetadata()["deployment_id"] != "dep-1" {
		t.Fatalf("registration metadata: %#v", registration.GetMetadata())
	}

	components := registration.GetComponents()
	if len(components) != 2 {
		t.Fatalf("components: %d", len(components))
	}
	if components[0].GetName() != "alpha" ||
		components[0].GetComponentType() != pb.ComponentType_COMPONENT_TYPE_WORKFLOW {
		t.Fatalf("first component: %#v", components[0])
	}
	if components[0].GetMetadata()["cron"] != "*/5 * * * *" {
		t.Fatalf("workflow cron metadata: %#v", components[0].GetMetadata())
	}
	triggers := components[0].GetTriggers()
	if len(triggers) != 1 ||
		triggers[0].GetTriggerType() != "event" ||
		triggers[0].GetEventName() != "user.created" {
		t.Fatalf("workflow triggers: %#v", triggers)
	}
	if components[1].GetName() != "zeta" ||
		components[1].GetComponentType() != pb.ComponentType_COMPONENT_TYPE_FUNCTION {
		t.Fatalf("second component: %#v", components[1])
	}
	if components[1].GetMaxAttempts() != 3 ||
		components[1].GetInitialIntervalMs() != 100 ||
		components[1].GetMaxIntervalMs() != 5000 ||
		components[1].GetBackoffType() != "exponential" ||
		components[1].GetBackoffMultiplier() != 2 {
		t.Fatalf("retry fields were not promoted: %#v", components[1])
	}
	if components[1].GetConfig()["max_attempts"] != "3" ||
		components[1].GetMetadata()["kind"] != "fn" {
		t.Fatalf("component metadata/config lost: %#v %#v", components[1].GetConfig(), components[1].GetMetadata())
	}

	capabilities := registration.GetCapabilities()
	if len(capabilities) != 2 {
		t.Fatalf("capabilities: %d", len(capabilities))
	}
	if capabilities[0].GetComponentName() != "alpha" ||
		capabilities[1].GetComponentName() != "zeta" {
		t.Fatalf("capabilities not sorted with components: %#v", capabilities)
	}
}

func TestRegisterOnceReturnsRejectedRegistration(t *testing.T) {
	server := &testCoordinator{
		ack:      false,
		reject:   "service name denied",
		received: make(chan *pb.ServiceMessage, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-1"))

	err := worker.registerOnce(context.Background(), client)
	if !errors.Is(err, ErrRegistrationRejected) {
		t.Fatalf("expected registration rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "service name denied") {
		t.Fatalf("missing rejection detail: %v", err)
	}
}

func TestRunWorkerStreamHandlesSingleDispatch(t *testing.T) {
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-1:invoke",
			ServiceName:   "svc",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			Metadata:      map[string]string{"request_id": "req-1"},
			Attempt:       2,
			IsStreaming:   true,
			LeaseId:       "lease-1",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		if ctx.InvocationID() != "run-1:invoke" {
			t.Fatalf("invocation id: %q", ctx.InvocationID())
		}
		if ctx.RunID() != "run-1" {
			t.Fatalf("run id: %q", ctx.RunID())
		}
		if ctx.ComponentName() != "greet" || ctx.ComponentType() != ComponentTypeFunction {
			t.Fatalf("component: %s %s", ctx.ComponentName(), ctx.ComponentType())
		}
		if ctx.Metadata("request_id") != "req-1" {
			t.Fatalf("metadata: %#v", ctx.MetadataMap())
		}
		if ctx.Attempt() != 2 || !ctx.IsStreaming() || ctx.LeaseID() != "lease-1" {
			t.Fatalf("dispatch context: attempt=%d streaming=%t lease=%q", ctx.Attempt(), ctx.IsStreaming(), ctx.LeaseID())
		}
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	response := <-server.response
	if response == nil {
		t.Fatal("missing function response")
	}
	if !response.GetSuccess() {
		t.Fatalf("expected success response: %s", response.GetErrorMessage())
	}
	if response.GetInvocationId() != "run-1:invoke" ||
		response.GetEventType() != "run.completed" ||
		response.GetAttempt() != 2 ||
		response.GetLeaseId() != "lease-1" {
		t.Fatalf("response identity: %#v", response)
	}
	if response.GetMetadata()["request_id"] != "req-1" {
		t.Fatalf("response metadata: %#v", response.GetMetadata())
	}
	var output dispatchGreetOutput
	if err := json.Unmarshal(response.GetOutputData(), &output); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if output.Message != "hello Ada" {
		t.Fatalf("output: %#v", output)
	}
}

func TestRunWorkerStreamWritesLifecycleEvents(t *testing.T) {
	writer := &recordingEventWriter{}
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-events:invoke",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			Metadata:      map[string]string{"request_id": "req-events"},
			LeaseId:       "lease-events",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		withEventWriter(writer),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	writer.assertTypes(t, "run.started", "function.started", "function.completed")
	events := writer.Events()
	if events[0].RunID != "run-events" ||
		events[0].Metadata["project_id"] != "proj-1" ||
		events[0].Metadata["tenant_id"] != "proj-1" ||
		events[0].Metadata["deployment_id"] != "dep-1" ||
		events[0].Metadata["worker_id"] != "worker-1" ||
		events[0].Metadata["component_name"] != "greet" {
		t.Fatalf("event metadata: %#v", events[0])
	}
	var completed map[string]any
	if err := json.Unmarshal(events[2].Data, &completed); err != nil {
		t.Fatalf("decode function.completed: %v", err)
	}
	output, ok := completed["output_data"].(map[string]any)
	if !ok || output["message"] != "hello Ada" {
		t.Fatalf("function.completed output: %#v", completed)
	}

	response := <-server.response
	if !response.GetSuccess() || response.GetEventType() != "run.completed" {
		t.Fatalf("terminal response: %#v", response)
	}
}

func TestRunWorkerStreamBatchesBufferedBoundaryEvents(t *testing.T) {
	writer := &recordingEventWriter{}
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-boundary:invoke",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-boundary",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithProjectID("proj-1"),
		withEventWriter(writer),
	)
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		_ = ctx.Emit(Event{Type: "workflow.state.changed", Data: map[string]any{"name": in.Name}})
		_ = ctx.Emit(Event{Type: "agent.iteration.completed", Data: map[string]any{"ok": true}})
		return dispatchGreetOutput{Message: "done"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	writer.assertTypes(t,
		"run.started",
		"function.started",
		"workflow.state.changed",
		"agent.iteration.completed",
		"function.completed",
	)
	batches := writer.Batches()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("boundary batches: %#v", batches)
	}
	events := writer.Events()
	componentCorrelationID := events[1].CorrelationID
	if componentCorrelationID == "" {
		t.Fatalf("component correlation id was empty: %#v", events[1])
	}
	for _, event := range events[2:4] {
		if event.CorrelationID == "" || event.ParentCorrelationID != componentCorrelationID {
			t.Fatalf("handler event was not parented under component: %#v", event)
		}
	}
}

func TestRunWorkerStreamStreamsSSEEventsBeforeTerminal(t *testing.T) {
	writer := &recordingEventSink{}
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-stream:invoke",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			IsStreaming:   true,
			LeaseId:       "lease-stream",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithProjectID("proj-1"),
		withEventWriter(writer),
	)
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		ctx.Output("hello " + in.Name)
		return dispatchGreetOutput{Message: "done"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	streamEvents := writer.RecordedStreamEvents()
	if len(streamEvents) != 1 || streamEvents[0].EventType != EventTypeOutputDelta {
		t.Fatalf("stream events: %#v", streamEvents)
	}
	events := writer.Events()
	if streamEvents[0].TraceID == "" || streamEvents[0].SpanID != events[1].CorrelationID {
		t.Fatalf("stream event correlation: stream=%#v component=%#v", streamEvents[0], events[1])
	}
	var delta map[string]string
	if err := json.Unmarshal(streamEvents[0].Data, &delta); err != nil {
		t.Fatalf("decode delta: %v", err)
	}
	if delta["content"] != "hello Ada" {
		t.Fatalf("delta: %#v", delta)
	}
	response := <-server.response
	if response.GetEventType() != "run.completed" || !response.GetSuccess() {
		t.Fatalf("terminal response: %#v", response)
	}
}

func TestRunWorkerStreamFallsBackToDispatchForSSEEvents(t *testing.T) {
	writer := &recordingEventWriter{}
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-stream:invoke",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			IsStreaming:   true,
			LeaseId:       "lease-stream",
		},
		received:      make(chan *pb.ServiceMessage, 1),
		response:      make(chan *pb.DispatchComponentResponse, 2),
		responseCount: 2,
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithProjectID("proj-1"),
		withEventWriter(writer),
	)
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		ctx.Output("hello " + in.Name)
		return dispatchGreetOutput{Message: "done"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	first := <-server.response
	if first.GetEventType() != EventTypeOutputDelta ||
		!first.GetSuccess() ||
		first.GetLeaseId() != "lease-stream" {
		t.Fatalf("sse response: %#v", first)
	}
	var delta map[string]string
	if err := json.Unmarshal(first.GetOutputData(), &delta); err != nil {
		t.Fatalf("decode delta: %v", err)
	}
	if delta["content"] != "hello Ada" {
		t.Fatalf("delta: %#v", delta)
	}
	second := <-server.response
	if second.GetEventType() != "run.completed" || !second.GetSuccess() {
		t.Fatalf("terminal response: %#v", second)
	}
	writer.assertTypes(t, "run.started", "function.started", "function.completed")
}

func TestRunWorkerStreamDropsSSEEventsForNonStreamingRun(t *testing.T) {
	writer := &recordingEventSink{}
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-drop:invoke",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-drop",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithProjectID("proj-1"),
		withEventWriter(writer),
	)
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		ctx.Output("hello " + in.Name)
		ctx.Logger().Info("this log is transient")
		return dispatchGreetOutput{Message: "done"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	if got := writer.RecordedStreamEvents(); len(got) != 0 {
		t.Fatalf("non-streaming events leaked to EventStream: %#v", got)
	}
	response := <-server.response
	if response.GetEventType() != "run.completed" || !response.GetSuccess() {
		t.Fatalf("terminal response: %#v", response)
	}
	writer.assertTypes(t, "run.started", "function.started", "function.completed")
}

func TestRunWorkerStreamReturnsDispatchErrorResponse(t *testing.T) {
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-2",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "missing",
			LeaseId:       "lease-2",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-1"))

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	response := <-server.response
	if response.GetSuccess() {
		t.Fatalf("expected failed response: %#v", response)
	}
	if response.GetInvocationId() != "run-2" ||
		response.GetEventType() != "run.failed" ||
		response.GetLeaseId() != "lease-2" {
		t.Fatalf("failed response identity: %#v", response)
	}
	if !strings.Contains(response.GetErrorMessage(), ErrComponentNotFound.Error()) {
		t.Fatalf("error message: %q", response.GetErrorMessage())
	}
}

func TestRunWorkerStreamReturnsPanicFailureResponse(t *testing.T) {
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-panic",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "panic",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-panic",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	if err := RegisterFunction(worker, "panic", func(*Context, dispatchGreetInput) (dispatchGreetOutput, error) {
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	response := <-server.response
	if response.GetSuccess() ||
		response.GetInvocationId() != "run-panic" ||
		response.GetEventType() != "run.failed" ||
		response.GetLeaseId() != "lease-panic" {
		t.Fatalf("panic response: %#v", response)
	}
	if !strings.Contains(response.GetErrorMessage(), "handler panic: boom") {
		t.Fatalf("error message: %q", response.GetErrorMessage())
	}
}

func TestRunWorkerStreamCancelsInFlightDispatch(t *testing.T) {
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-cancel",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "block",
			LeaseId:       "lease-cancel",
		},
		cancel: &pb.CancelExecutionRequest{
			InvocationId: "run-cancel",
			Reason:       "manual",
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	client := newTestCoordinatorClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	if err := RegisterFunction(worker, "block", func(ctx *Context, _ map[string]string) (map[string]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.runWorkerStream(context.Background(), client); err != nil {
		t.Fatalf("run worker stream: %v", err)
	}

	response := <-server.response
	if response.GetSuccess() {
		t.Fatalf("expected canceled response: %#v", response)
	}
	if response.GetInvocationId() != "run-cancel" ||
		response.GetEventType() != "run.failed" ||
		response.GetLeaseId() != "lease-cancel" {
		t.Fatalf("canceled response identity: %#v", response)
	}
	if !strings.Contains(response.GetErrorMessage(), context.Canceled.Error()) {
		t.Fatalf("error message: %q", response.GetErrorMessage())
	}
}

func TestCancelInFlightReleasesBlockedDispatchResult(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-1"), WithMaxConcurrency(1))
	started := make(chan struct{})
	if err := RegisterFunction(worker, "block", func(ctx *Context, _ map[string]string) (map[string]string, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	inFlight := make(map[string]context.CancelFunc)
	doneCh := make(chan dispatchResult)
	slots := worker.dispatchSlots()
	streamCtx, stopStream := context.WithCancel(context.Background())

	err := worker.handleRuntimeMessage(streamCtx, &pb.RuntimeMessage{
		WorkerId:    "worker-1",
		MessageType: pb.RuntimeMessageType_INVOKE_FUNCTION,
		MessageData: &pb.RuntimeMessage_DispatchComponent{
			DispatchComponent: &pb.DispatchComponentRequest{
				InvocationId:  "run-leak",
				ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
				ComponentName: "block",
				InputData:     []byte(`{}`),
			},
		},
	}, inFlight, doneCh, slots)
	if err != nil {
		t.Fatalf("handle runtime message: %v", err)
	}
	<-started
	stopStream()
	cancelInFlight(inFlight)
	deadline := time.After(time.Second)
	for len(slots) != 0 {
		select {
		case <-deadline:
			t.Fatal("dispatch goroutine stayed blocked after in-flight cancellation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestHandleRuntimeMessageReturnsWorkerReplaced(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	inFlight := map[string]context.CancelFunc{
		"run-1": func() {},
	}

	err := worker.handleRuntimeMessage(context.Background(), &pb.RuntimeMessage{
		WorkerId:    "worker-1",
		MessageType: pb.RuntimeMessageType_WORKER_REPLACED,
	}, inFlight, make(chan dispatchResult), nil)

	if !errors.Is(err, ErrWorkerReplaced) {
		t.Fatalf("handle runtime message: %v", err)
	}
	if len(inFlight) != 0 {
		t.Fatalf("in-flight dispatches were not canceled: %#v", inFlight)
	}
}

func TestHandleRuntimeMessageReturnsCoordinatorDraining(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	inFlight := map[string]context.CancelFunc{
		"run-1": func() {},
	}

	err := worker.handleRuntimeMessage(context.Background(), &pb.RuntimeMessage{
		WorkerId:    "worker-1",
		MessageType: pb.RuntimeMessageType_COORDINATOR_DRAINING,
		MessageData: &pb.RuntimeMessage_CoordinatorDraining{
			CoordinatorDraining: &pb.CoordinatorDraining{},
		},
	}, inFlight, make(chan dispatchResult), nil)

	if !errors.Is(err, ErrCoordinatorDraining) {
		t.Fatalf("handle runtime message: %v", err)
	}
	if len(inFlight) != 0 {
		t.Fatalf("in-flight dispatches were not canceled: %#v", inFlight)
	}
}

func TestWorkerRunDialsConfiguredEndpoint(t *testing.T) {
	server := &testCoordinator{
		ack: true,
		dispatch: &pb.DispatchComponentRequest{
			InvocationId:  "run-public",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Grace"}`),
		},
		received: make(chan *pb.ServiceMessage, 1),
		response: make(chan *pb.DispatchComponentResponse, 1),
	}
	listener := newTestCoordinatorListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithCoordinatorEndpoint("http://bufnet"),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		t.Fatalf("run worker: %v", err)
	}

	response := <-server.response
	if !response.GetSuccess() {
		t.Fatalf("expected success response: %s", response.GetErrorMessage())
	}
	var output dispatchGreetOutput
	if err := json.Unmarshal(response.GetOutputData(), &output); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if output.Message != "hello Grace" {
		t.Fatalf("output: %#v", output)
	}
}

func newTestCoordinatorClient(t *testing.T, server *testCoordinator) pb.WorkerCoordinatorServiceClient {
	t.Helper()

	listener := newTestCoordinatorListener(t, server)
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(testBufconnDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial fake coordinator: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return pb.NewWorkerCoordinatorServiceClient(conn)
}

func newTestCoordinatorListener(t *testing.T, server *testCoordinator) *bufconn.Listener {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerCoordinatorServiceServer(grpcServer, server)
	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- err
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
		select {
		case err := <-serveErr:
			t.Errorf("serve fake coordinator: %v", err)
		default:
		}
	})
	return listener
}

func testBufconnDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

type dispatchGreetInput struct {
	Name string `json:"name"`
}

type dispatchGreetOutput struct {
	Message string `json:"message"`
}

type recordingEventWriter struct {
	mu      sync.Mutex
	events  []journalEvent
	batches [][]journalEvent
	err     error
}

func (w *recordingEventWriter) WriteEvent(_ context.Context, event journalEvent) error {
	if w.err != nil {
		return w.err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	event.Data = cloneBytes(event.Data)
	event.Metadata = cloneStringMap(event.Metadata)
	w.events = append(w.events, event)
	return nil
}

func (w *recordingEventWriter) WriteEvents(ctx context.Context, events []journalEvent) error {
	if w.err != nil {
		return w.err
	}
	copied := make([]journalEvent, 0, len(events))
	for _, event := range events {
		event.Data = cloneBytes(event.Data)
		event.Metadata = cloneStringMap(event.Metadata)
		copied = append(copied, event)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches = append(w.batches, copied)
	w.events = append(w.events, copied...)
	return nil
}

func (w *recordingEventWriter) Events() []journalEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]journalEvent, len(w.events))
	copy(out, w.events)
	return out
}

func (w *recordingEventWriter) Batches() [][]journalEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]journalEvent, len(w.batches))
	for i, batch := range w.batches {
		out[i] = make([]journalEvent, len(batch))
		copy(out[i], batch)
	}
	return out
}

func (w *recordingEventWriter) assertTypes(t *testing.T, eventTypes ...string) {
	t.Helper()
	events := w.Events()
	if len(events) != len(eventTypes) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(eventTypes), events)
	}
	for i, want := range eventTypes {
		if events[i].EventType != want {
			t.Fatalf("event[%d] = %q, want %q: %#v", i, events[i].EventType, want, events)
		}
	}
}

type recordingEventSink struct {
	recordingEventWriter

	streamMu  sync.Mutex
	streams   []streamEvent
	streamErr error
}

func (w *recordingEventSink) StreamEvents(_ context.Context, events []streamEvent) error {
	if w.streamErr != nil {
		return w.streamErr
	}
	copied := make([]streamEvent, 0, len(events))
	for _, event := range events {
		event.Data = cloneBytes(event.Data)
		event.Metadata = cloneStringMap(event.Metadata)
		copied = append(copied, event)
	}
	w.streamMu.Lock()
	defer w.streamMu.Unlock()
	w.streams = append(w.streams, copied...)
	return nil
}

func (w *recordingEventSink) RecordedStreamEvents() []streamEvent {
	w.streamMu.Lock()
	defer w.streamMu.Unlock()
	out := make([]streamEvent, len(w.streams))
	copy(out, w.streams)
	return out
}
