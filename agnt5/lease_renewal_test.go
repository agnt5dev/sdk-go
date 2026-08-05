package agnt5

import (
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestExecutionLeaseAuthorityRequestCarriesPushFence(t *testing.T) {
	authority := executionLeaseAuthority{
		workerID:       "worker-1",
		projectID:      "project-1",
		deploymentID:   "deployment-1",
		runID:          "run-1",
		leaseID:        "lease-1",
		attempt:        3,
		mode:           pb.WorkerMode_WORKER_MODE_PUSH,
		leaseTimeout:   120 * time.Second,
		leaseExpiresAt: time.UnixMilli(200_000),
	}

	request := authority.request()

	if request.GetWorkerId() != "worker-1" || request.GetWorkerSessionId() != "" {
		t.Fatalf("worker authority: %#v", request)
	}
	if request.GetProjectId() != "project-1" || request.GetDeploymentId() != "deployment-1" {
		t.Fatalf("routing authority: %#v", request)
	}
	if request.GetRunId() != "run-1" || request.GetLeaseId() != "lease-1" {
		t.Fatalf("lease authority: %#v", request)
	}
	if request.Attempt == nil || request.GetAttempt() != 3 {
		t.Fatalf("attempt authority: %#v", request.Attempt)
	}
	if request.GetMode() != pb.WorkerMode_WORKER_MODE_PUSH {
		t.Fatalf("mode: %s", request.GetMode())
	}
}

func TestActiveLeaseRenewIntervalsAreBounded(t *testing.T) {
	if got := activeLeaseRenewInterval(120 * time.Second); got != 60*time.Second {
		t.Fatalf("renew interval: %s", got)
	}
	if got := activeLeaseDangerRetry(120 * time.Second); got != 5*time.Second {
		t.Fatalf("danger retry: %s", got)
	}
	if got := activeLeaseRenewInterval(40 * time.Millisecond); got != 20*time.Millisecond {
		t.Fatalf("short renew interval: %s", got)
	}
	for range 100 {
		got := jitterLeaseRenewInterval(120 * time.Second)
		if got < 54*time.Second || got > 66*time.Second {
			t.Fatalf("jittered interval out of bounds: %s", got)
		}
	}
}
