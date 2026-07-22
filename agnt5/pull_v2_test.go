package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
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
	err := &ProtocolError{Code: "STALE_EXECUTION_TOKEN", GRPCCode: codes.Aborted, Retryable: false}
	if v2OperationRetryable(err, false) {
		t.Fatal("stale execution commit must not be retried")
	}
	if shouldReconnect(err) {
		t.Fatal("stale execution error must not reconnect the worker")
	}
}

func TestV2StaleWorkerSessionStopsInsteadOfReregistering(t *testing.T) {
	err := &ProtocolError{Code: "STALE_WORKER_SESSION", GRPCCode: codes.Unauthenticated, Retryable: false}
	if v2OperationRetryable(err, true) {
		t.Fatal("stale worker session poll must not be retried")
	}
	if shouldReconnect(err) {
		t.Fatal("a replaced worker session must not fight the replacement by reregistering")
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
