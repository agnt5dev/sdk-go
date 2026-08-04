package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testEngine struct {
	pb.UnimplementedEngineServiceServer

	mu           sync.Mutex
	job          *pb.JobAssignment
	jobPolled    bool
	pollErrs     []error
	renewErrs    []error
	completeErrs []error
	capacityErrs []error
	polled       chan *pb.PollJobRequest
	registered   chan *pb.RegisterWorkerSessionRequest
	completed    chan *pb.CompleteJobRequest
	renewed      chan *pb.RenewJobLeaseRequest
	appends      chan *pb.AppendRequest
	batches      chan *pb.AppendBatchRequest
	streamed     chan *pb.EventStreamMessage
	streamClosed chan int64
	capacity     chan *pb.ReportWorkerCapacityRequest
	statePuts    chan *pb.PutEntityStateRequest
	statePutErrs []error
	completeWait bool
	registerIDs  []string
	registerCall int
}

func (s *testEngine) RegisterWorkerSession(_ context.Context, req *pb.RegisterWorkerSessionRequest) (*pb.RegisterWorkerSessionResponse, error) {
	s.registered <- req
	s.mu.Lock()
	sessionID := "session-1"
	if s.registerCall < len(s.registerIDs) {
		sessionID = s.registerIDs[s.registerCall]
	}
	s.registerCall++
	s.mu.Unlock()
	return &pb.RegisterWorkerSessionResponse{
		WorkerSessionId: sessionID,
		ExpiresAtMs:     999999,
		EffectiveSlotPolicy: &pb.WorkerSlotPolicy{
			MinSlots:       1,
			MaxSlots:       req.GetMaxSlots(),
			RampThrottleMs: 1,
		},
	}, nil
}

func (s *testEngine) PollJob(ctx context.Context, req *pb.PollJobRequest) (*pb.PollJobResponse, error) {
	if s.polled != nil {
		s.polled <- req
	}
	s.mu.Lock()
	if len(s.pollErrs) > 0 {
		err := s.pollErrs[0]
		s.pollErrs = s.pollErrs[1:]
		s.mu.Unlock()
		return nil, err
	}
	if !s.jobPolled {
		s.jobPolled = true
		job := s.job
		s.mu.Unlock()
		return &pb.PollJobResponse{Job: job}, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *testEngine) Append(_ context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	if s.appends != nil {
		s.appends <- req
	}
	return &pb.AppendResponse{Offset: uint64(len(s.appends)), TimestampNs: sourceTimestampNS(0)}, nil
}

func (s *testEngine) AppendBatch(_ context.Context, req *pb.AppendBatchRequest) (*pb.AppendBatchResponse, error) {
	if s.batches != nil {
		s.batches <- req
	}
	offsets := make([]uint64, len(req.GetRecords()))
	for i := range offsets {
		offsets[i] = uint64(i)
	}
	return &pb.AppendBatchResponse{
		Offsets:      offsets,
		WrittenCount: int32(len(req.GetRecords())),
	}, nil
}

func (s *testEngine) EventStream(stream grpc.ClientStreamingServer[pb.EventStreamMessage, pb.EventStreamAck]) error {
	var count int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if s.streamClosed != nil {
				s.streamClosed <- count
			}
			return stream.SendAndClose(&pb.EventStreamAck{Success: true, EventsReceived: count})
		}
		if err != nil {
			return err
		}
		count++
		if s.streamed != nil {
			s.streamed <- msg
		}
	}
}

func (s *testEngine) CompleteJob(ctx context.Context, req *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	s.completed <- req
	s.mu.Lock()
	if len(s.completeErrs) > 0 {
		err := s.completeErrs[0]
		s.completeErrs = s.completeErrs[1:]
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	if s.completeWait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &pb.CompleteJobResponse{Acknowledged: true}, nil
}

func (s *testEngine) RenewJobLease(_ context.Context, req *pb.RenewJobLeaseRequest) (*pb.RenewJobLeaseResponse, error) {
	if s.renewed != nil {
		s.renewed <- req
	}
	s.mu.Lock()
	if len(s.renewErrs) > 0 {
		err := s.renewErrs[0]
		s.renewErrs = s.renewErrs[1:]
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	return &pb.RenewJobLeaseResponse{Renewed: true, LeaseExpiresAtMs: time.Now().Add(time.Minute).UnixMilli()}, nil
}

func (s *testEngine) GetEntityState(_ context.Context, _ *pb.GetEntityStateRequest) (*pb.GetEntityStateResponse, error) {
	return &pb.GetEntityStateResponse{Found: false}, nil
}

func (s *testEngine) PutEntityState(_ context.Context, req *pb.PutEntityStateRequest) (*pb.PutEntityStateResponse, error) {
	if s.statePuts != nil {
		s.statePuts <- req
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.statePutErrs) > 0 {
		err := s.statePutErrs[0]
		s.statePutErrs = s.statePutErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &pb.PutEntityStateResponse{NewVersion: req.GetExpectedVersion() + 1}, nil
}

func (s *testEngine) ReportWorkerCapacity(_ context.Context, req *pb.ReportWorkerCapacityRequest) (*pb.ReportWorkerCapacityResponse, error) {
	s.capacity <- req
	s.mu.Lock()
	if len(s.capacityErrs) > 0 {
		err := s.capacityErrs[0]
		s.capacityErrs = s.capacityErrs[1:]
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	return &pb.ReportWorkerCapacityResponse{Accepted: true, RecordedAtMs: sourceTimestampNS(0) / int64(time.Millisecond)}, nil
}

func TestWorkerRunPullCompletesPolledJob(t *testing.T) {
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-pull",
			RunId:         "run-pull",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			Metadata:      map[string]string{"request_id": "req-pull"},
			Attempt:       1,
			LeaseId:       "lease-pull",
		},
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		appends:    make(chan *pb.AppendRequest, 4),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(2),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	registration := <-server.registered
	if registration.GetWorkerId() != "worker-pull" ||
		registration.GetProjectId() != "proj-1" ||
		registration.GetDeploymentId() != "dep-1" ||
		registration.GetMaxSlots() != 2 {
		t.Fatalf("registration: %#v", registration)
	}
	componentNames := make(map[string]pb.ComponentType)
	for _, component := range registration.GetComponents() {
		componentNames[component.GetName()] = component.GetComponentType()
	}
	if componentNames["greet"] != pb.ComponentType_COMPONENT_TYPE_FUNCTION ||
		componentNames["exact_match"] != pb.ComponentType_COMPONENT_TYPE_SCORER ||
		componentNames["llm_judge"] != pb.ComponentType_COMPONENT_TYPE_SCORER {
		t.Fatalf("components: %#v", componentNames)
	}

	completion := <-server.completed
	cancel()
	if completion.GetJobId() != "run-pull" ||
		completion.GetWorkerId() != "worker-pull" ||
		!completion.GetSuccess() ||
		completion.GetProjectId() != "proj-1" ||
		completion.GetLeaseId() != "lease-pull" ||
		completion.GetWorkerSessionId() != "session-1" ||
		completion.Attempt == nil ||
		completion.GetAttempt() != 1 {
		t.Fatalf("completion: %#v", completion)
	}
	var output dispatchGreetOutput
	if err := json.Unmarshal(completion.GetOutputData(), &output); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if output.Message != "hello Ada" {
		t.Fatalf("output: %#v", output)
	}

	eventTypes := collectAppendTypes(t, server.appends, 3)
	want := []string{"run.started", "function.started", "function.completed"}
	for i := range want {
		if eventTypes[i] != want[i] {
			t.Fatalf("event types = %#v, want %#v", eventTypes, want)
		}
	}

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullRampsPollSlotsAboveMinimum(t *testing.T) {
	server := &testEngine{
		polled:     make(chan *pb.PollJobRequest, 2),
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(2),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	first := <-server.polled
	second := <-server.polled
	cancel()
	if first.GetWorkerSessionId() != "session-1" || second.GetWorkerSessionId() != "session-1" {
		t.Fatalf("poll sessions: %#v %#v", first, second)
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullRetriesTransientPollErrors(t *testing.T) {
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-poll-retry",
			RunId:         "run-poll-retry",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-poll-retry",
		},
		pollErrs:   []error{errors.New("temporary poll failure")},
		polled:     make(chan *pb.PollJobRequest, 2),
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	<-server.polled
	<-server.polled
	completion := <-server.completed
	cancel()
	if !completion.GetSuccess() || completion.GetJobId() != "run-poll-retry" {
		t.Fatalf("completion: %#v", completion)
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullReregistersRejectedSession(t *testing.T) {
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-session-replaced",
			RunId:         "run-session-replaced",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-session-2",
		},
		pollErrs: []error{
			status.Error(codes.PermissionDenied, "worker session is not active"),
			status.Error(codes.PermissionDenied, "worker session is not active"),
			status.Error(codes.PermissionDenied, "worker session is not active"),
		},
		registerIDs: []string{"session-1", "session-2"},
		polled:      make(chan *pb.PollJobRequest, 4),
		registered:  make(chan *pb.RegisterWorkerSessionRequest, 2),
		completed:   make(chan *pb.CompleteJobRequest, 1),
		capacity:    make(chan *pb.ReportWorkerCapacityRequest, 4),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		WithReconnectBackoff(0, 0),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	firstPoll := <-server.polled
	secondRejectedPoll := <-server.polled
	thirdRejectedPoll := <-server.polled
	<-server.registered
	replacementPoll := <-server.polled
	completion := <-server.completed
	cancel()

	if firstPoll.GetWorkerSessionId() != "session-1" {
		t.Fatalf("first poll session = %q, want session-1", firstPoll.GetWorkerSessionId())
	}
	if secondRejectedPoll.GetWorkerSessionId() != "session-1" ||
		thirdRejectedPoll.GetWorkerSessionId() != "session-1" {
		t.Fatalf("rejected polls did not retain session-1: %#v %#v", secondRejectedPoll, thirdRejectedPoll)
	}
	if replacementPoll.GetWorkerSessionId() != "session-2" {
		t.Fatalf("replacement poll session = %q, want session-2", replacementPoll.GetWorkerSessionId())
	}
	if completion.GetWorkerSessionId() != "session-2" {
		t.Fatalf("completion session = %q, want session-2", completion.GetWorkerSessionId())
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullKeepsSessionAfterTransientPermissionDenied(t *testing.T) {
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-transient-session-reject",
			RunId:         "run-transient-session-reject",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-session-1",
		},
		pollErrs: []error{
			status.Error(codes.PermissionDenied, "gossip route still converging"),
		},
		registerIDs: []string{"session-1", "unexpected-session-2"},
		polled:      make(chan *pb.PollJobRequest, 3),
		registered:  make(chan *pb.RegisterWorkerSessionRequest, 2),
		completed:   make(chan *pb.CompleteJobRequest, 1),
		capacity:    make(chan *pb.ReportWorkerCapacityRequest, 4),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		WithReconnectBackoff(0, 0),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	rejectedPoll := <-server.polled
	successfulPoll := <-server.polled
	completion := <-server.completed
	cancel()

	if rejectedPoll.GetWorkerSessionId() != "session-1" ||
		successfulPoll.GetWorkerSessionId() != "session-1" {
		t.Fatalf("poll session changed after one rejection: %#v %#v", rejectedPoll, successfulPoll)
	}
	if completion.GetWorkerSessionId() != "session-1" {
		t.Fatalf("completion session = %q, want session-1", completion.GetWorkerSessionId())
	}
	select {
	case registration := <-server.registered:
		t.Fatalf("unexpected re-registration after one rejection: %#v", registration)
	default:
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullReregistersAfterPermissionDeniedRenewal(t *testing.T) {
	t.Setenv(envClaimTimeoutMS, "6")
	denied := status.Error(codes.PermissionDenied, "worker session is not active")
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-renew-session-replaced",
			RunId:         "run-renew-session-replaced",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "block",
			InputData:     []byte(`{}`),
			Metadata:      map[string]string{"lease_timeout_ms": "999999999"},
			LeaseId:       "lease-session-1",
		},
		renewErrs:   []error{denied, denied, denied},
		registerIDs: []string{"session-1", "session-2"},
		registered:  make(chan *pb.RegisterWorkerSessionRequest, 2),
		completed:   make(chan *pb.CompleteJobRequest, 2),
		renewed:     make(chan *pb.RenewJobLeaseRequest, 3),
		capacity:    make(chan *pb.ReportWorkerCapacityRequest, 4),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		WithReconnectBackoff(0, 0),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "block", func(ctx *Context, _ map[string]string) (map[string]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	for i := 0; i < sessionRejectThreshold; i++ {
		renewal := <-server.renewed
		if renewal.GetWorkerSessionId() != "session-1" {
			t.Fatalf("renewal %d session = %q, want session-1", i, renewal.GetWorkerSessionId())
		}
	}
	<-server.registered
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullReregistersAfterPermissionDeniedCompletion(t *testing.T) {
	denied := status.Error(codes.PermissionDenied, "worker session is not active")
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-complete-session-replaced",
			RunId:         "run-complete-session-replaced",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			LeaseId:       "lease-session-1",
		},
		completeErrs: []error{denied, denied, denied},
		registerIDs:  []string{"session-1", "session-2"},
		registered:   make(chan *pb.RegisterWorkerSessionRequest, 2),
		completed:    make(chan *pb.CompleteJobRequest, 3),
		capacity:     make(chan *pb.ReportWorkerCapacityRequest, 4),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		WithReconnectBackoff(0, 0),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(_ *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		return dispatchGreetOutput{Message: "hello " + in.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	for i := 0; i < defaultCompleteJobAttempts; i++ {
		completion := <-server.completed
		if completion.GetWorkerSessionId() != "session-1" {
			t.Fatalf("completion %d session = %q, want session-1", i, completion.GetWorkerSessionId())
		}
	}
	<-server.registered
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestReportPullCapacitySignalsRepeatedPermissionDenied(t *testing.T) {
	denied := status.Error(codes.PermissionDenied, "worker session is not active")
	server := &testEngine{
		capacityErrs: []error{denied, denied, denied},
		capacity:     make(chan *pb.ReportWorkerCapacityRequest, 3),
	}
	client := newTestEngineClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
	)
	config := pullSlotConfig{minSlots: 1, maxSlots: 1, desiredSlots: 1}
	var openPollSlots atomic.Uint32
	var activeSlots atomic.Uint32
	sessionFailures := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.reportPullCapacity(
		ctx,
		client,
		"session-1",
		config,
		&openPollSlots,
		&activeSlots,
		time.Millisecond,
		sessionFailures,
	)

	select {
	case err := <-sessionFailures:
		if status.Code(errors.Unwrap(err)) != codes.PermissionDenied {
			t.Fatalf("session failure = %v, want PermissionDenied", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for capacity session failure")
	}
	for i := 0; i < sessionRejectThreshold; i++ {
		report := <-server.capacity
		if report.GetWorkerSessionId() != "session-1" {
			t.Fatalf("capacity report %d session = %q", i, report.GetWorkerSessionId())
		}
	}
}

func TestWorkerRunPullRenewsLongRunningLease(t *testing.T) {
	t.Setenv(envClaimTimeoutMS, "20")
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-renew",
			RunId:         "run-renew",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "block",
			InputData:     []byte(`{}`),
			Metadata:      map[string]string{"lease_timeout_ms": "1"},
			LeaseId:       "lease-renew",
		},
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		renewed:    make(chan *pb.RenewJobLeaseRequest, 1),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	renewed := make(chan struct{})
	if err := RegisterFunction(worker, "block", func(_ *Context, _ map[string]string) (map[string]string, error) {
		<-renewed
		return map[string]string{"ok": "true"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	renewal := <-server.renewed
	close(renewed)
	completion := <-server.completed
	cancel()
	if renewal.GetRunId() != "run-renew" ||
		renewal.GetLeaseId() != "lease-renew" ||
		renewal.GetLeaseTimeoutMs() != 20 {
		t.Fatalf("renewal: %#v", renewal)
	}
	if completion.GetLeaseId() != "lease-renew" || !completion.GetSuccess() {
		t.Fatalf("completion: %#v", completion)
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullContinuesLeaseRenewalAfterTransientError(t *testing.T) {
	t.Setenv(envClaimTimeoutMS, "20")
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-renew-retry",
			RunId:         "run-renew-retry",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "block",
			InputData:     []byte(`{}`),
			Metadata:      map[string]string{"lease_timeout_ms": "999999999"},
			LeaseId:       "lease-renew-retry",
		},
		renewErrs:  []error{errors.New("temporary renew failure")},
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		renewed:    make(chan *pb.RenewJobLeaseRequest, 2),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	renewedTwice := make(chan struct{})
	if err := RegisterFunction(worker, "block", func(_ *Context, _ map[string]string) (map[string]string, error) {
		<-renewedTwice
		return map[string]string{"ok": "true"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	first := <-server.renewed
	second := <-server.renewed
	close(renewedTwice)
	completion := <-server.completed
	cancel()
	if first.GetRunId() != "run-renew-retry" || second.GetRunId() != "run-renew-retry" {
		t.Fatalf("renewals: %#v %#v", first, second)
	}
	if completion.GetLeaseId() != "lease-renew-retry" || !completion.GetSuccess() {
		t.Fatalf("completion: %#v", completion)
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestWorkerRunPullStreamsSSEEvents(t *testing.T) {
	server := &testEngine{
		job: &pb.JobAssignment{
			JobId:         "run-pull-stream",
			RunId:         "run-pull-stream",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "greet",
			InputData:     []byte(`{"name":"Ada"}`),
			Metadata:      map[string]string{"stream_mode": "full"},
			LeaseId:       "lease-pull-stream",
		},
		registered:   make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:    make(chan *pb.CompleteJobRequest, 1),
		streamed:     make(chan *pb.EventStreamMessage, 1),
		streamClosed: make(chan int64, 1),
		capacity:     make(chan *pb.ReportWorkerCapacityRequest, 2),
	}
	listener := newTestEngineListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
		WithWorkerMode(WorkerModePull),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	releaseHandler := make(chan struct{})
	if err := RegisterFunction(worker, "greet", func(ctx *Context, in dispatchGreetInput) (dispatchGreetOutput, error) {
		ctx.Output("hello " + in.Name)
		<-releaseHandler
		return dispatchGreetOutput{Message: "done"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	<-server.registered
	streamed := <-server.streamed
	select {
	case completion := <-server.completed:
		t.Fatalf("job completed before streaming handler returned: %#v", completion)
	default:
	}
	close(releaseHandler)
	select {
	case count := <-server.streamClosed:
		if count != 1 {
			t.Fatalf("flushed event count = %d, want 1", count)
		}
	case completion := <-server.completed:
		t.Fatalf("job completed before event stream flush: %#v", completion)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event stream flush")
	}
	completion := <-server.completed
	cancel()
	if streamed.GetRunId() != "run-pull-stream" ||
		streamed.GetEventType() != EventTypeOutputDelta ||
		streamed.GetWorkerId() != "worker-pull" {
		t.Fatalf("streamed event: %#v", streamed)
	}
	if completion.GetLeaseId() != "lease-pull-stream" || !completion.GetSuccess() {
		t.Fatalf("completion: %#v", completion)
	}
	if got := completion.GetMetadata()["completion_event_type"]; got != "run.completed" {
		t.Fatalf("completion_event_type = %q, want run.completed", got)
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}
}

func TestDispatchRequestFromJobStampsPullDispatchMode(t *testing.T) {
	request := dispatchRequestFromJob(&pb.JobAssignment{
		JobId:    "job-1",
		RunId:    "run-1",
		Metadata: map[string]string{"source": "test", "dispatch_mode": "push"},
		LeaseId:  "lease-1",
	}, "svc")

	if got := request.GetMetadata()["dispatch_mode"]; got != "pull" {
		t.Fatalf("dispatch_mode = %q, want pull", got)
	}
	if got := request.GetMetadata()["source"]; got != "test" {
		t.Fatalf("source = %q, want test", got)
	}
}

func TestPollConcurrencyLimitReservesControlStream(t *testing.T) {
	for _, test := range []struct {
		config pullSlotConfig
		want   int
	}{
		{config: pullSlotConfig{minSlots: 1, maxSlots: 10}, want: 1},
		{config: pullSlotConfig{minSlots: 2, maxSlots: 10}, want: 2},
		{config: pullSlotConfig{minSlots: 10, maxSlots: 10}, want: 10},
	} {
		if got := pollConcurrencyLimit(test.config); got != test.want {
			t.Fatalf("pollConcurrencyLimit(%+v) = %d, want %d", test.config, got, test.want)
		}
	}
}

func TestIsTerminalPullResponseRecognizesPausedLifecycle(t *testing.T) {
	for _, eventType := range []string{"run.paused", "workflow.paused"} {
		t.Run(eventType, func(t *testing.T) {
			if !isTerminalPullResponse(&pb.DispatchComponentResponse{EventType: eventType}) {
				t.Fatalf("%s should terminate a parked pull job", eventType)
			}
		})
	}
	if isTerminalPullResponse(&pb.DispatchComponentResponse{
		EventType:    "run.cancelled",
		ErrorMessage: "cancelled",
	}) {
		t.Fatal("run.cancelled is authored by the control plane, not CompleteJob")
	}
}

func TestCompletePolledJobRejectsMismatchedLease(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-pull"), WithProjectID("proj-1"))
	err := worker.completePolledJob(context.Background(), nil, "session-1",
		&pb.JobAssignment{
			JobId:   "job-1",
			RunId:   "job-1",
			LeaseId: "lease-current",
		},
		&pb.DispatchComponentResponse{
			InvocationId: "job-1",
			Success:      true,
			LeaseId:      "lease-stale",
		},
	)
	if err == nil || !strings.Contains(err.Error(), "stale lease") {
		t.Fatalf("expected stale lease error, got %v", err)
	}
}

func TestExecutePolledJobRejectsForgedMetadataLeaseBeforeHandler(t *testing.T) {
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
	)
	var handlerCalls atomic.Uint32
	if err := RegisterFunction(worker, "side_effect", func(_ *Context, _ map[string]string) (map[string]string, error) {
		handlerCalls.Add(1)
		return map[string]string{"ok": "true"}, nil
	}); err != nil {
		t.Fatal(err)
	}

	err := worker.executePolledJob(
		context.Background(),
		nil,
		"session-1",
		defaultClaimTimeoutMS,
		&pb.JobAssignment{
			JobId:         "run-forged-lease",
			RunId:         "run-forged-lease",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
			ComponentName: "side_effect",
			InputData:     []byte(`{}`),
			Metadata:      map[string]string{"lease_id": "forged-lease"},
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "no typed lease_id") {
		t.Fatalf("expected typed lease validation error, got %v", err)
	}
	if handlerCalls.Load() != 0 {
		t.Fatalf("handler executed %d times for invalid assignment", handlerCalls.Load())
	}
}

func TestCompletePolledJobHasBoundedDeadline(t *testing.T) {
	server := &testEngine{
		completed:    make(chan *pb.CompleteJobRequest, 1),
		completeWait: true,
	}
	client := newTestEngineClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-pull"), WithProjectID("proj-1"))

	err := worker.completePolledJobWithin(
		context.Background(),
		client,
		"session-1",
		&pb.JobAssignment{
			JobId:   "job-timeout",
			RunId:   "job-timeout",
			LeaseId: "lease-1",
		},
		&pb.DispatchComponentResponse{
			InvocationId: "job-timeout",
			Success:      true,
			EventType:    "run.completed",
			LeaseId:      "lease-1",
		},
		20*time.Millisecond,
	)
	if err == nil || !strings.Contains(err.Error(), "DeadlineExceeded") {
		t.Fatalf("expected bounded CompleteJob deadline, got %v", err)
	}
}

func TestCompletePolledJobRetriesTransientFailure(t *testing.T) {
	server := &testEngine{
		completed:    make(chan *pb.CompleteJobRequest, 2),
		completeErrs: []error{errors.New("transient completion failure")},
	}
	client := newTestEngineClient(t, server)
	worker := NewWorker("svc", WithWorkerID("worker-pull"), WithProjectID("proj-1"))
	request := &pb.CompleteJobRequest{JobId: "job-retry", LeaseId: "lease-1"}

	err := worker.completePolledJobRequestWithRetry(context.Background(), client, request, 20*time.Millisecond, 2, time.Millisecond)
	if err != nil {
		t.Fatalf("retry CompleteJob: %v", err)
	}
	if got := len(server.completed); got != 2 {
		t.Fatalf("CompleteJob attempts = %d, want 2", got)
	}
}

func TestEngineEventWriterBatchesAndStreamsEvents(t *testing.T) {
	server := &testEngine{
		batches:  make(chan *pb.AppendBatchRequest, 1),
		streamed: make(chan *pb.EventStreamMessage, 1),
	}
	writer := newEngineEventWriter(newTestEngineClient(t, server))

	err := writer.WriteEvents(context.Background(), []journalEvent{{
		RunID:     "run-1",
		EventType: "workflow.state.changed",
		Data:      []byte(`{"ok":true}`),
		Metadata:  map[string]string{"project_id": "proj-1"},
	}})
	if err != nil {
		t.Fatalf("write events: %v", err)
	}
	batch := <-server.batches
	if len(batch.GetRecords()) != 1 ||
		batch.GetRecords()[0].GetProjectId() != "proj-1" ||
		batch.GetRecords()[0].GetEventType() != "workflow.state.changed" {
		t.Fatalf("append batch: %#v", batch.GetRecords())
	}

	err = writer.StreamEvents(context.Background(), []streamEvent{{
		RunID:     "run-1",
		EventType: EventTypeOutputDelta,
		Data:      []byte(`{"delta":"hi"}`),
		Metadata:  map[string]string{"project_id": "proj-1", "worker_id": "worker-1"},
	}})
	if err != nil {
		t.Fatalf("stream events: %v", err)
	}
	streamed := <-server.streamed
	if err := writer.FlushEvents(context.Background()); err != nil {
		t.Fatalf("flush events: %v", err)
	}
	if streamed.GetRunId() != "run-1" ||
		streamed.GetEventType() != EventTypeOutputDelta ||
		streamed.GetProjectId() != "proj-1" ||
		streamed.GetWorkerId() != "worker-1" {
		t.Fatalf("streamed event: %#v", streamed)
	}
}

func TestEngineEventWriterAppendBatchRejectsMultiRunBeforeRPC(t *testing.T) {
	server := &testEngine{batches: make(chan *pb.AppendBatchRequest, 1)}
	writer := newEngineEventWriter(newTestEngineClient(t, server))

	err := writer.WriteEvents(context.Background(), []journalEvent{
		{
			RunID:     "run-a",
			EventType: "step.started",
			Metadata:  map[string]string{"project_id": "proj-1"},
		},
		{
			RunID:     "run-b",
			EventType: "step.completed",
			Metadata:  map[string]string{"project_id": "proj-1"},
		},
	})

	if err == nil || !strings.Contains(err.Error(), "must share one run_id") {
		t.Fatalf("expected same-run preflight error, got %v", err)
	}
	select {
	case request := <-server.batches:
		t.Fatalf("AppendBatch called after preflight rejection: %#v", request)
	default:
	}
}

func TestValidateAppendBatchResponse(t *testing.T) {
	tests := []struct {
		name string
		resp *pb.AppendBatchResponse
		ok   bool
	}{
		{
			name: "all appended",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5}, WrittenCount: 2},
			ok:   true,
		},
		{
			name: "all idempotent duplicates",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5}, WrittenCount: 0},
			ok:   true,
		},
		{
			name: "mixed idempotent outcome",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5}, WrittenCount: 1},
			ok:   true,
		},
		{
			name: "short offsets",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4}, WrittenCount: 1},
		},
		{
			name: "long offsets",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5, 6}, WrittenCount: 2},
		},
		{
			name: "negative written count",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5}, WrittenCount: -1},
		},
		{
			name: "excess written count",
			resp: &pb.AppendBatchResponse{Offsets: []uint64{4, 5}, WrittenCount: 3},
		},
		{name: "nil response"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAppendBatchResponse(test.resp, 2)
			if test.ok && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if !test.ok && err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func newTestEngineListener(t *testing.T, server *testEngine) *bufconn.Listener {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterEngineServiceServer(grpcServer, server)
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
			t.Errorf("serve fake engine: %v", err)
		default:
		}
	})
	return listener
}

func collectAppendTypes(t *testing.T, ch <-chan *pb.AppendRequest, count int) []string {
	t.Helper()

	out := make([]string, 0, count)
	for len(out) < count {
		select {
		case req := <-ch:
			out = append(out, req.GetRecord().GetEventType())
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for append %d", len(out)+1)
		}
	}
	return out
}

func newTestEngineClient(t *testing.T, server *testEngine) pb.EngineServiceClient {
	t.Helper()

	listener := newTestEngineListener(t, server)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(testBufconnDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial fake engine: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return pb.NewEngineServiceClient(conn)
}
