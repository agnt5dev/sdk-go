package agnt5

import (
	"context"
	"errors"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestEngineStateStoreFencesRunScopedWriteAndReusesOperationID(t *testing.T) {
	server := &testEngine{
		statePuts:    make(chan *pb.PutEntityStateRequest, 2),
		statePutErrs: []error{errors.New("lost response"), nil},
	}
	store := newEngineStateStore(newTestEngineClient(t, server), "proj-1")
	runCtx := newContext(context.Background(), Invocation{
		ID:      "run-1",
		RunID:   "run-1",
		Attempt: 2,
		LeaseID: "lease-1",
		Metadata: map[string]string{
			"worker_id":         "worker-1",
			"worker_session_id": "session-1",
		},
	}, nil, "", store)

	if err := store.Set(runCtx, StateScopeRun, "run-1", "answer", 42); err != nil {
		t.Fatalf("set state: %v", err)
	}
	first := <-server.statePuts
	second := <-server.statePuts
	for index, request := range []*pb.PutEntityStateRequest{first, second} {
		if request.GetRunId() != "run-1" ||
			request.GetWorkerId() != "worker-1" ||
			request.GetWorkerSessionId() != "session-1" ||
			request.GetLeaseId() != "lease-1" ||
			request.GetAttempt() != 2 ||
			request.GetOperationId() == "" {
			t.Fatalf("state request %d: %#v", index, request)
		}
	}
	if first.GetOperationId() != second.GetOperationId() {
		t.Fatalf("operation id changed across retry: %q != %q", first.GetOperationId(), second.GetOperationId())
	}
}

func TestEngineStateStoreRejectsIncompleteRunAuthority(t *testing.T) {
	store := newEngineStateStore(newTestEngineClient(t, &testEngine{}), "proj-1")
	runCtx := newContext(context.Background(), Invocation{
		ID:      "run-1",
		RunID:   "run-1",
		LeaseID: "lease-1",
	}, nil, "", store)

	err := store.Set(runCtx, StateScopeRun, "run-1", "answer", 42)
	if err == nil || err.Error() != "agnt5: run-scoped state write is missing parked-poll authority" {
		t.Fatalf("set state error = %v", err)
	}
}
