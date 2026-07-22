package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testV2RuntimeState struct {
	mu sync.Mutex

	execute        *protocolv2.ExecuteRunRequest
	pollRequests   []*protocolv2.PollRunRequest
	commitRequests []*protocolv2.CommitRunOutcomeRequest
	registered     chan *protocolv2.RegisterWorkerRequest
	renewed        chan *protocolv2.RenewRunLeaseRequest
	committed      chan *protocolv2.CommitRunOutcomeRequest
	unregistered   chan *protocolv2.UnregisterWorkerRequest
}

type testV2ProtocolServer struct {
	protocolv2.UnimplementedProtocolServiceServer
	state *testV2RuntimeState
}

func (s *testV2ProtocolServer) GetCapabilities(context.Context, *protocolv2.GetCapabilitiesRequest) (*protocolv2.GetCapabilitiesResponse, error) {
	return &protocolv2.GetCapabilitiesResponse{
		SelectedProtocol: &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		RuntimeName:      "test-runtime",
		RuntimeVersion:   "0.1.0-alpha.2",
		Limits: &protocolv2.ProtocolLimits{
			MaximumMessageBytes:       1 << 20,
			MaximumInlinePayloadBytes: 1 << 20,
			MaximumEventBatchBytes:    1 << 20,
			MaximumEventsPerBatch:     100,
			MaximumPayloadBytes:       1 << 20,
			MaximumPayloadChunkBytes:  64 << 10,
		},
	}, nil
}

type testV2WorkerServer struct {
	protocolv2.UnimplementedWorkerServiceServer
	state *testV2RuntimeState
}

type scriptedV2WorkerClient struct {
	poll   func(context.Context, *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error)
	renew  func(context.Context, *protocolv2.RenewRunLeaseRequest) (*protocolv2.RenewRunLeaseResponse, error)
	commit func(context.Context, *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error)
}

func (c *scriptedV2WorkerClient) RegisterWorker(context.Context, *protocolv2.RegisterWorkerRequest, ...grpc.CallOption) (*protocolv2.RegisterWorkerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *scriptedV2WorkerClient) UnregisterWorker(context.Context, *protocolv2.UnregisterWorkerRequest, ...grpc.CallOption) (*protocolv2.UnregisterWorkerResponse, error) {
	return &protocolv2.UnregisterWorkerResponse{}, nil
}

func (c *scriptedV2WorkerClient) PollRun(ctx context.Context, request *protocolv2.PollRunRequest, _ ...grpc.CallOption) (*protocolv2.PollRunResponse, error) {
	if c.poll == nil {
		return nil, status.Error(codes.Unimplemented, "poll not scripted")
	}
	return c.poll(ctx, request)
}

func (c *scriptedV2WorkerClient) RenewRunLease(ctx context.Context, request *protocolv2.RenewRunLeaseRequest, _ ...grpc.CallOption) (*protocolv2.RenewRunLeaseResponse, error) {
	if c.renew == nil {
		return &protocolv2.RenewRunLeaseResponse{LeaseExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
	}
	return c.renew(ctx, request)
}

func (c *scriptedV2WorkerClient) AppendRunEvents(context.Context, *protocolv2.AppendRunEventsRequest, ...grpc.CallOption) (*protocolv2.AppendRunEventsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *scriptedV2WorkerClient) PublishRunOutput(context.Context, *protocolv2.PublishRunOutputRequest, ...grpc.CallOption) (*protocolv2.PublishRunOutputResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *scriptedV2WorkerClient) CommitRunOutcome(ctx context.Context, request *protocolv2.CommitRunOutcomeRequest, _ ...grpc.CallOption) (*protocolv2.CommitRunOutcomeResponse, error) {
	if c.commit == nil {
		return &protocolv2.CommitRunOutcomeResponse{Disposition: protocolv2.CommitDisposition_COMMIT_DISPOSITION_COMMITTED}, nil
	}
	return c.commit(ctx, request)
}

func (s *testV2WorkerServer) RegisterWorker(_ context.Context, request *protocolv2.RegisterWorkerRequest) (*protocolv2.RegisterWorkerResponse, error) {
	s.state.registered <- request
	return &protocolv2.RegisterWorkerResponse{
		WorkerSessionToken:       []byte("worker-session-secret"),
		SessionExpiresAt:         timestamppb.New(time.Now().Add(time.Hour)),
		SelectedProtocol:         &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		MaximumPollWait:          durationpb.New(50 * time.Millisecond),
		LeaseDuration:            durationpb.New(time.Minute),
		RecommendedRenewInterval: durationpb.New(time.Millisecond),
		Limits: &protocolv2.ProtocolLimits{
			MaximumMessageBytes:       1 << 20,
			MaximumInlinePayloadBytes: 1 << 20,
			MaximumEventBatchBytes:    1 << 20,
			MaximumEventsPerBatch:     100,
			MaximumPayloadBytes:       1 << 20,
			MaximumPayloadChunkBytes:  64 << 10,
		},
	}, nil
}

func (s *testV2WorkerServer) UnregisterWorker(_ context.Context, request *protocolv2.UnregisterWorkerRequest) (*protocolv2.UnregisterWorkerResponse, error) {
	select {
	case s.state.unregistered <- request:
	default:
	}
	return &protocolv2.UnregisterWorkerResponse{}, nil
}

func (s *testV2WorkerServer) PollRun(ctx context.Context, request *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
	s.state.mu.Lock()
	s.state.pollRequests = append(s.state.pollRequests, request)
	call := len(s.state.pollRequests)
	s.state.mu.Unlock()
	if call == 1 {
		return nil, status.Error(codes.Unavailable, "retry the same logical poll")
	}
	if call == 2 {
		return &protocolv2.PollRunResponse{
			Result: &protocolv2.PollRunResponse_Execute{Execute: s.state.execute},
		}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *testV2WorkerServer) RenewRunLease(_ context.Context, request *protocolv2.RenewRunLeaseRequest) (*protocolv2.RenewRunLeaseResponse, error) {
	select {
	case s.state.renewed <- request:
	default:
	}
	return &protocolv2.RenewRunLeaseResponse{LeaseExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
}

func (s *testV2WorkerServer) CommitRunOutcome(_ context.Context, request *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
	s.state.mu.Lock()
	s.state.commitRequests = append(s.state.commitRequests, request)
	call := len(s.state.commitRequests)
	s.state.mu.Unlock()
	if call == 1 {
		return nil, status.Error(codes.Unavailable, "retry the same outcome commit")
	}
	s.state.committed <- request
	return &protocolv2.CommitRunOutcomeResponse{
		Disposition: protocolv2.CommitDisposition_COMMIT_DISPOSITION_COMMITTED,
		RunStatus:   protocolv2.RunStatus_RUN_STATUS_COMPLETED,
	}, nil
}

func TestWorkerRunV2NegotiatesPollsRenewsAndCommits(t *testing.T) {
	t.Setenv(envHealthDir, t.TempDir())
	state := &testV2RuntimeState{
		execute: &protocolv2.ExecuteRunRequest{
			RunId:          "run-v2",
			ExecutionId:    "execution-v2",
			ExecutionToken: []byte("execution-secret"),
			Target: &protocolv2.ComponentTarget{
				Type:    protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION,
				Name:    "greet",
				Version: "1.2.3",
			},
			Input: &protocolv2.Payload{
				Body:        &protocolv2.Payload_InlineData{InlineData: []byte(`{"name":"Ada"}`)},
				ContentType: "application/json",
			},
			Attempt: 1,
		},
		registered:   make(chan *protocolv2.RegisterWorkerRequest, 1),
		renewed:      make(chan *protocolv2.RenewRunLeaseRequest, 4),
		committed:    make(chan *protocolv2.CommitRunOutcomeRequest, 1),
		unregistered: make(chan *protocolv2.UnregisterWorkerRequest, 1),
	}
	listener := newTestV2RuntimeListener(t, state)
	worker := NewWorker("svc",
		WithWorkerID("worker-v2"),
		WithServiceVersion("1.2.3"),
		WithProjectID("project-private"),
		WithDeploymentID("deployment-private"),
		WithProtocolMode(ProtocolModeV2),
		WithCoordinatorEndpoint("http://bufnet"),
		WithMaxConcurrency(1),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	if err := RegisterFunction(worker, "greet", func(ctx *Context, input dispatchGreetInput) (dispatchGreetOutput, error) {
		select {
		case <-state.renewed:
		case <-time.After(time.Second):
			return dispatchGreetOutput{}, errors.New("lease was not renewed")
		}
		ctx.Logger().Info("greeting user", "name", input.Name)
		return dispatchGreetOutput{Message: "hello " + input.Name}, nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	registration := <-state.registered
	if registration.GetWorkerId() != "worker-v2" || registration.GetServiceVersion() != "1.2.3" {
		t.Fatalf("registration: %#v", registration)
	}
	if registration.GetMetadata()["project_id"] != "" || registration.GetMetadata()["AGNT5_DEPLOYMENT_ID"] != "" {
		t.Fatalf("routing metadata leaked into v2 registration: %#v", registration.GetMetadata())
	}
	if len(registration.GetComponents()) != 1 || registration.GetComponents()[0].GetVersion() != "1.2.3" {
		t.Fatalf("components: %#v", registration.GetComponents())
	}

	commit := <-state.committed
	if string(commit.GetExecutionToken()) != "execution-secret" || commit.GetCommitId() != "execution-v2:outcome" {
		t.Fatalf("commit identity: %#v", commit)
	}
	completed := commit.GetOutcome().GetCompleted()
	if completed == nil {
		t.Fatalf("outcome: %#v", commit.GetOutcome())
	}
	if completed.GetMetadata()["agnt5.protocol.omitted_event_count"] != "1" ||
		completed.GetMetadata()["agnt5.protocol.event_transport"] != "unavailable" {
		t.Fatalf("unsupported event diagnostics: %#v", completed.GetMetadata())
	}
	var output dispatchGreetOutput
	if err := json.Unmarshal(completed.GetOutput().GetInlineData(), &output); err != nil {
		t.Fatal(err)
	}
	if output.Message != "hello Ada" {
		t.Fatalf("output: %#v", output)
	}

	state.mu.Lock()
	polls := append([]*protocolv2.PollRunRequest(nil), state.pollRequests...)
	commits := append([]*protocolv2.CommitRunOutcomeRequest(nil), state.commitRequests...)
	state.mu.Unlock()
	if len(polls) < 2 || polls[0].GetPollId() == "" || polls[0].GetPollId() != polls[1].GetPollId() {
		t.Fatalf("logical poll was not replayed: %#v", polls)
	}
	if len(commits) != 2 || commits[0].GetCommitId() != commits[1].GetCommitId() ||
		string(commits[0].GetExecutionToken()) != string(commits[1].GetExecutionToken()) {
		t.Fatalf("outcome commit was not replayed idempotently: %#v", commits)
	}

	diagnostics := worker.ProtocolDiagnostics()
	if diagnostics.SelectedVersion != "v2.0" || diagnostics.RuntimeName != "test-runtime" {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context canceled", err)
	}
	select {
	case unregister := <-state.unregistered:
		if string(unregister.GetWorkerSessionToken()) != "worker-session-secret" {
			t.Fatalf("unregister: %#v", unregister)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not unregister its v2 session")
	}
}

func TestV2StaleExecutionErrorsAreNotRetried(t *testing.T) {
	err := &ProtocolError{Code: "STALE_EXECUTION_TOKEN", GRPCCode: codes.Aborted, Retryable: true}
	if v2OperationRetryable(err, false) {
		t.Fatal("stale execution commit must not be retried")
	}
	if shouldReconnect(err) {
		t.Fatal("stale execution error must not reconnect the worker")
	}
	if !v2ExecutionAuthorityError(err) {
		t.Fatal("stale execution error must remain scoped to its execution")
	}
}

func TestV2StaleWorkerSessionStopsInsteadOfReregistering(t *testing.T) {
	err := &ProtocolError{Code: "STALE_WORKER_SESSION", GRPCCode: codes.Unauthenticated, Retryable: true}
	if v2OperationRetryable(err, true) {
		t.Fatal("stale worker session poll must not be retried")
	}
	if shouldReconnect(err) {
		t.Fatal("a replaced worker session must not fight the replacement by reregistering")
	}
}

func TestV2SessionErrorClassificationDistinguishesExpiryAndReplacement(t *testing.T) {
	plainUnauthenticated := &ProtocolError{Code: codes.Unauthenticated.String(), GRPCCode: codes.Unauthenticated}
	expired := classifyV2SessionError(plainUnauthenticated, time.Now().Add(-time.Second))
	if !errors.Is(expired, errV2SessionExpired) || !shouldReconnect(expired) {
		t.Fatalf("expired session error = %v, want reconnectable expiry", expired)
	}
	replaced := classifyV2SessionError(plainUnauthenticated, time.Now().Add(time.Hour))
	if !errors.Is(replaced, ErrWorkerReplaced) || shouldReconnect(replaced) {
		t.Fatalf("replacement error = %v, want terminal worker replacement", replaced)
	}
	structuredStale := &ProtocolError{
		Code:       "STALE_WORKER_SESSION",
		GRPCCode:   codes.Unauthenticated,
		structured: true,
	}
	if classified := classifyV2SessionError(structuredStale, time.Now().Add(-time.Second)); !errors.Is(classified, ErrWorkerReplaced) || shouldReconnect(classified) {
		t.Fatalf("structured stale session = %v, want terminal replacement", classified)
	}
}

func TestV2SessionReplacementStopsPollSlot(t *testing.T) {
	client := &scriptedV2WorkerClient{poll: func(context.Context, *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "worker session is invalid or expired")
	}}
	worker := NewWorker("svc", WithMaxConcurrency(1))
	err := worker.runV2PollSlot(context.Background(), client, testV2Registration(time.Now().Add(time.Hour)))
	if !errors.Is(err, ErrWorkerReplaced) || shouldReconnect(err) {
		t.Fatalf("poll slot error = %v, want terminal worker replacement", err)
	}
}

func TestV2SessionExpiryDrainsEveryPollSlotBeforeReconnect(t *testing.T) {
	const slots = 3
	var active atomic.Int32
	var started atomic.Int32
	client := &scriptedV2WorkerClient{poll: func(ctx context.Context, _ *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
		started.Add(1)
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	worker := NewWorker("svc", WithMaxConcurrency(slots))
	registration := testV2Registration(time.Now().Add(50 * time.Millisecond))
	err := worker.runV2Worker(context.Background(), client, registration)
	if !errors.Is(err, errV2SessionExpired) || !shouldReconnect(err) {
		t.Fatalf("runV2Worker error = %v, want reconnectable session expiry", err)
	}
	if started.Load() != slots || active.Load() != 0 {
		t.Fatalf("poll slots started=%d active=%d, want %d started and zero active", started.Load(), active.Load(), slots)
	}
}

func TestV2ExecutionCancellationKeepsSessionAndRepolls(t *testing.T) {
	worker := NewWorker("svc", WithServiceVersion("1.2.3"), WithMaxConcurrency(1))
	if err := RegisterFunction(worker, "block", func(ctx *Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	repolled := make(chan struct{})
	var polls atomic.Int32
	var commits atomic.Int32
	client := &scriptedV2WorkerClient{
		poll: func(ctx context.Context, _ *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
			if polls.Add(1) == 1 {
				return &protocolv2.PollRunResponse{Result: &protocolv2.PollRunResponse_Execute{Execute: &protocolv2.ExecuteRunRequest{
					RunId:          "run-cancelled",
					ExecutionId:    "execution-cancelled",
					ExecutionToken: []byte("execution-secret"),
					Target: &protocolv2.ComponentTarget{
						Type:    protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION,
						Name:    "block",
						Version: "1.2.3",
					},
					Input: &protocolv2.Payload{Body: &protocolv2.Payload_InlineData{InlineData: []byte(`{}`)}},
				}}}, nil
			}
			select {
			case <-repolled:
			default:
				close(repolled)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		renew: func(context.Context, *protocolv2.RenewRunLeaseRequest) (*protocolv2.RenewRunLeaseResponse, error) {
			return &protocolv2.RenewRunLeaseResponse{CancellationRequested: true, CancellationReason: "run cancelled"}, nil
		},
		commit: func(context.Context, *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
			commits.Add(1)
			return &protocolv2.CommitRunOutcomeResponse{Disposition: protocolv2.CommitDisposition_COMMIT_DISPOSITION_COMMITTED}, nil
		},
	}
	registration := testV2Registration(time.Now().Add(time.Hour))
	registration.RecommendedRenewInterval = durationpb.New(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.runV2PollSlot(ctx, client, registration) }()
	select {
	case <-repolled:
	case <-time.After(time.Second):
		t.Fatal("execution cancellation stopped the poll slot instead of repolling")
	}
	if commits.Load() != 0 {
		t.Fatalf("cancelled execution committed %d outcomes", commits.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("poll slot error = %v, want context canceled", err)
	}
}

func TestV2StaleRenewKeepsSessionAndRepolls(t *testing.T) {
	worker := NewWorker("svc", WithServiceVersion("1.2.3"), WithMaxConcurrency(1))
	if err := RegisterFunction(worker, "block", func(ctx *Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	repolled := make(chan struct{})
	var polls atomic.Int32
	var commits atomic.Int32
	client := &scriptedV2WorkerClient{
		poll: func(ctx context.Context, _ *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
			if polls.Add(1) == 1 {
				return testV2ExecuteResponse("block"), nil
			}
			select {
			case <-repolled:
			default:
				close(repolled)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		renew: func(context.Context, *protocolv2.RenewRunLeaseRequest) (*protocolv2.RenewRunLeaseResponse, error) {
			return nil, &ProtocolError{Code: "STALE_EXECUTION_TOKEN", GRPCCode: codes.Aborted, structured: true}
		},
		commit: func(context.Context, *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
			commits.Add(1)
			return &protocolv2.CommitRunOutcomeResponse{Disposition: protocolv2.CommitDisposition_COMMIT_DISPOSITION_COMMITTED}, nil
		},
	}
	registration := testV2Registration(time.Now().Add(time.Hour))
	registration.RecommendedRenewInterval = durationpb.New(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.runV2PollSlot(ctx, client, registration) }()
	select {
	case <-repolled:
	case <-time.After(time.Second):
		t.Fatal("stale renewal stopped the poll slot instead of repolling")
	}
	if commits.Load() != 0 {
		t.Fatalf("stale execution committed %d outcomes", commits.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("poll slot error = %v, want context canceled", err)
	}
}

func TestV2StaleCommitKeepsSessionAndRepolls(t *testing.T) {
	worker := NewWorker("svc", WithServiceVersion("1.2.3"), WithMaxConcurrency(1))
	if err := RegisterFunction(worker, "greet", func(*Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	repolled := make(chan struct{})
	var polls atomic.Int32
	var commits atomic.Int32
	client := &scriptedV2WorkerClient{
		poll: func(ctx context.Context, _ *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
			if polls.Add(1) == 1 {
				return testV2ExecuteResponse("greet"), nil
			}
			select {
			case <-repolled:
			default:
				close(repolled)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		commit: func(context.Context, *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
			commits.Add(1)
			return nil, &ProtocolError{Code: "STALE_EXECUTION_TOKEN", GRPCCode: codes.Aborted, Retryable: true, structured: true}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.runV2PollSlot(ctx, client, testV2Registration(time.Now().Add(time.Hour))) }()
	select {
	case <-repolled:
	case <-time.After(time.Second):
		t.Fatal("stale commit stopped the poll slot instead of repolling")
	}
	if commits.Load() != 1 {
		t.Fatalf("commit calls = %d, want one non-retried stale commit", commits.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("poll slot error = %v, want context canceled", err)
	}
}

func TestV2MessageLimitRejectsOversizedCommitBeforeRPC(t *testing.T) {
	var calls atomic.Int32
	client := &scriptedV2WorkerClient{commit: func(context.Context, *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
		calls.Add(1)
		return &protocolv2.CommitRunOutcomeResponse{}, nil
	}}
	limits := testV2Limits()
	limits.MaximumMessageBytes = 8
	_, err := commitV2WithRetry(context.Background(), client, &protocolv2.CommitRunOutcomeRequest{
		ExecutionToken: []byte("execution-secret"),
		CommitId:       "commit-id",
		Outcome: &protocolv2.RunOutcome{Kind: &protocolv2.RunOutcome_Completed{Completed: &protocolv2.RunCompleted{
			Output: &protocolv2.Payload{Body: &protocolv2.Payload_InlineData{InlineData: []byte(`{"ok":true}`)}},
		}}},
	}, limits)
	if err == nil || calls.Load() != 0 {
		t.Fatalf("oversized commit error=%v calls=%d, want local rejection", err, calls.Load())
	}
}

func testV2Registration(expiresAt time.Time) *protocolv2.RegisterWorkerResponse {
	return &protocolv2.RegisterWorkerResponse{
		WorkerSessionToken:       []byte("worker-session-secret"),
		SessionExpiresAt:         timestamppb.New(expiresAt),
		SelectedProtocol:         &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		MaximumPollWait:          durationpb.New(50 * time.Millisecond),
		LeaseDuration:            durationpb.New(time.Minute),
		RecommendedRenewInterval: durationpb.New(time.Second),
		Limits:                   testV2Limits(),
	}
}

func testV2ExecuteResponse(componentName string) *protocolv2.PollRunResponse {
	return &protocolv2.PollRunResponse{Result: &protocolv2.PollRunResponse_Execute{Execute: &protocolv2.ExecuteRunRequest{
		RunId:          "run-1",
		ExecutionId:    "execution-1",
		ExecutionToken: []byte("execution-secret"),
		Target: &protocolv2.ComponentTarget{
			Type:    protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION,
			Name:    componentName,
			Version: "1.2.3",
		},
		Input: &protocolv2.Payload{Body: &protocolv2.Payload_InlineData{InlineData: []byte(`{}`)}},
	}}}
}

func testV2Limits() *protocolv2.ProtocolLimits {
	return &protocolv2.ProtocolLimits{
		MaximumMessageBytes:       1 << 20,
		MaximumInlinePayloadBytes: 1 << 20,
		MaximumEventBatchBytes:    1 << 20,
		MaximumEventsPerBatch:     100,
		MaximumPayloadBytes:       1 << 20,
		MaximumPayloadChunkBytes:  64 << 10,
	}
}

func newTestV2RuntimeListener(t *testing.T, state *testV2RuntimeState) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	protocolv2.RegisterProtocolServiceServer(server, &testV2ProtocolServer{state: state})
	protocolv2.RegisterWorkerServiceServer(server, &testV2WorkerServer{state: state})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener
}
