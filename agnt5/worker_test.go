package agnt5

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewWorkerDefaultServiceVersion(t *testing.T) {
	worker := NewWorker("svc")
	if got := worker.ServiceVersion(); got != "0.4.1" {
		t.Fatalf("default service version: %q", got)
	}
}

func TestNewWorkerDefaultsAndOptions(t *testing.T) {
	t.Setenv(envCoordinatorEndpoint, "http://runtime.example:34186")
	t.Setenv(envEngineURL, "http://engine.example:34182")
	t.Setenv(envProjectID, "proj-1")
	t.Setenv(envDeploymentID, "dep-1")
	t.Setenv(envWorkerMode, "pull")
	t.Setenv(envMaxConcurrency, "42")
	t.Setenv(envJournalQueueSize, "17")
	t.Setenv(envJournalBatchSize, "5")
	t.Setenv(envJournalFlushIntervalMS, "250")

	worker := NewWorker("svc",
		WithServiceVersion("1.2.3"),
		WithServiceType("application"),
		WithCoordinatorEndpoint("http://override:34186"),
		WithEngineEndpoint("http://engine-override:34182"),
		WithWorkerMode(WorkerModePush),
		WithMaxConcurrency(7),
		WithMaxReconnects(3),
		WithMetadata(map[string]string{"owner": "sdk"}),
	)

	if worker.ServiceName() != "svc" {
		t.Fatalf("service name: %q", worker.ServiceName())
	}
	if worker.ServiceVersion() != "1.2.3" {
		t.Fatalf("service version: %q", worker.ServiceVersion())
	}
	if worker.ServiceType() != "application" {
		t.Fatalf("service type: %q", worker.ServiceType())
	}
	if worker.CoordinatorEndpoint() != "http://override:34186" {
		t.Fatalf("coordinator endpoint: %q", worker.CoordinatorEndpoint())
	}
	if worker.EngineEndpoint() != "http://engine-override:34182" {
		t.Fatalf("engine endpoint: %q", worker.EngineEndpoint())
	}
	if worker.ProjectID() != "proj-1" {
		t.Fatalf("project id: %q", worker.ProjectID())
	}
	if worker.DeploymentID() != "dep-1" {
		t.Fatalf("deployment id: %q", worker.DeploymentID())
	}
	if worker.WorkerMode() != WorkerModePush {
		t.Fatalf("worker mode: %q", worker.WorkerMode())
	}
	if worker.MaxConcurrency() != 7 {
		t.Fatalf("max concurrency: %d", worker.MaxConcurrency())
	}
	if worker.journalQueueSize != 17 || worker.journalBatchSize != 5 || worker.journalFlushEvery != 250*time.Millisecond {
		t.Fatalf("journal config: queue=%d batch=%d flush=%s", worker.journalQueueSize, worker.journalBatchSize, worker.journalFlushEvery)
	}
	metadata := worker.Metadata()
	if metadata["project_id"] != "proj-1" || metadata["deployment_id"] != "dep-1" || metadata["owner"] != "sdk" {
		t.Fatalf("metadata not populated: %#v", metadata)
	}
	if metadata["AGNT5_PROJECT_ID"] != "proj-1" ||
		metadata["AGNT5_DEPLOYMENT_ID"] != "dep-1" ||
		metadata["AGNT5_MAX_CONCURRENCY"] != "7" ||
		metadata["AGNT5_MAX_RETRIES"] != "3" {
		t.Fatalf("runtime metadata not populated: %#v", metadata)
	}
	metadata["owner"] = "mutated"
	if worker.Metadata()["owner"] != "sdk" {
		t.Fatal("metadata was not defensively copied")
	}
}

func TestNewWorkerRegistersEnvironmentConcurrencyWithoutExplicitOption(t *testing.T) {
	t.Setenv(envMaxConcurrency, "16")

	worker := NewWorker("svc")
	if worker.MaxConcurrency() != 16 {
		t.Fatalf("max concurrency: %d", worker.MaxConcurrency())
	}
	if got := worker.registerService().GetMaxConcurrency(); got != 16 {
		t.Fatalf("registered max concurrency: %d", got)
	}
}

func TestRunWithReconnectRetriesTransientErrors(t *testing.T) {
	worker := NewWorker("svc",
		WithMaxReconnects(2),
		WithReconnectBackoff(0, 0),
	)
	transientErr := errors.New("transient")
	attempts := 0
	err := worker.runWithReconnect(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return transientErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run with reconnect: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRunWithReconnectStopsAtBudget(t *testing.T) {
	worker := NewWorker("svc",
		WithMaxReconnects(1),
		WithReconnectBackoff(0, 0),
	)
	transientErr := errors.New("transient")
	attempts := 0
	err := worker.runWithReconnect(context.Background(), func(context.Context) error {
		attempts++
		return transientErr
	})
	if !errors.Is(err, transientErr) {
		t.Fatalf("run with reconnect: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestRunWithReconnectZeroRetriesMeansInfinite(t *testing.T) {
	worker := NewWorker("svc",
		WithMaxReconnects(0),
		WithReconnectBackoff(0, 0),
	)
	transientErr := errors.New("transient")
	attempts := 0
	err := worker.runWithReconnect(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 4 {
			return transientErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run with reconnect: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4", attempts)
	}
}

func TestRunWithReconnectResetsBudgetAfterStableConnection(t *testing.T) {
	worker := NewWorker("svc",
		WithMaxReconnects(1),
		WithReconnectBackoff(0, 0),
	)
	worker.reconnectResetAfter = time.Millisecond
	transientErr := errors.New("transient")
	attempts := 0
	err := worker.runWithReconnect(context.Background(), func(context.Context) error {
		attempts++
		if attempts <= 2 {
			time.Sleep(2 * time.Millisecond)
			return transientErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run with reconnect: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRunWithReconnectDoesNotRetryRegistrationRejection(t *testing.T) {
	worker := NewWorker("svc",
		WithMaxReconnects(5),
		WithReconnectBackoff(0, 0),
	)
	attempts := 0
	err := worker.runWithReconnect(context.Background(), func(context.Context) error {
		attempts++
		return ErrRegistrationRejected
	})
	if !errors.Is(err, ErrRegistrationRejected) {
		t.Fatalf("run with reconnect: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestShouldReconnectControlErrors(t *testing.T) {
	if shouldReconnect(ErrWorkerReplaced) {
		t.Fatal("worker replacement should stop the worker instead of reconnecting")
	}
	if !shouldReconnect(ErrCoordinatorDraining) {
		t.Fatal("coordinator draining should reconnect after the stream exits")
	}
}

func TestEventClassification(t *testing.T) {
	trueCases := []string{
		"output.delta",
		"lm.stream.delta",
		"lm.message.delta",
		"lm.thinking.delta",
		"lm.tool_call.delta",
		"progress.update",
		"log",
		"log.info",
	}
	for _, eventType := range trueCases {
		if !IsSSEOnlyEventType(eventType) {
			t.Fatalf("%s should be SSE-only", eventType)
		}
	}
	falseCases := []string{
		"run.started",
		"run.completed",
		"workflow.step.completed",
		"tool.call.completed",
		"agent.iteration.completed",
	}
	for _, eventType := range falseCases {
		if IsSSEOnlyEventType(eventType) {
			t.Fatalf("%s should be durable boundary event", eventType)
		}
	}
}

func TestInvocationMetadataStampsPushExecutionAuthority(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-1"))
	metadata := worker.invocationMetadata(Invocation{
		RunID:   "run-1",
		Attempt: 7,
		LeaseID: "lease-7",
		Metadata: map[string]string{
			"worker_id":         "forged-worker",
			"worker_session_id": "forged-session",
			"lease_id":          "forged-lease",
			"lease_attempt":     "99",
			"dispatch_mode":     "push",
		},
	})

	for key, want := range map[string]string{
		"worker_id":         "worker-1",
		"worker_session_id": "worker-1",
		"lease_id":          "lease-7",
		"lease_attempt":     "7",
		"dispatch_mode":     "push",
	} {
		if got := metadata[key]; got != want {
			t.Fatalf("metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestInvocationMetadataPreservesPullSessionAuthority(t *testing.T) {
	worker := NewWorker("svc", WithWorkerID("worker-1"), WithWorkerMode(WorkerModePull))
	metadata := worker.invocationMetadata(Invocation{
		RunID:   "run-1",
		Attempt: 3,
		LeaseID: "lease-3",
		Metadata: map[string]string{
			"dispatch_mode":     "pull",
			"worker_session_id": "session-3",
		},
	})

	if got := metadata["worker_session_id"]; got != "session-3" {
		t.Fatalf("worker_session_id = %q, want session-3", got)
	}
	if got := metadata["lease_attempt"]; got != "3" {
		t.Fatalf("lease_attempt = %q, want 3", got)
	}
}

func TestInvocationEventMetadataCannotOverrideExecutionAuthority(t *testing.T) {
	metadata := mergeInvocationEventMetadata(
		map[string]string{
			"dispatch_mode":            "pull",
			"worker_id":                "worker-1",
			"worker_session_id":        "session-1",
			"lease_id":                 "lease-1",
			"lease_attempt":            "1",
			"assignment_commit_offset": "42",
		},
		map[string]string{
			"worker_id":                "forged-worker",
			"lease_id":                 "forged-lease",
			"assignment_commit_offset": "999",
			"custom":                   "preserved",
		},
	)

	if got := metadata["worker_id"]; got != "worker-1" {
		t.Fatalf("worker_id = %q, want worker-1", got)
	}
	if got := metadata["lease_id"]; got != "lease-1" {
		t.Fatalf("lease_id = %q, want lease-1", got)
	}
	if got := metadata["assignment_commit_offset"]; got != "42" {
		t.Fatalf("assignment_commit_offset = %q, want 42", got)
	}
	if got := metadata["custom"]; got != "preserved" {
		t.Fatalf("custom = %q, want preserved", got)
	}
}
