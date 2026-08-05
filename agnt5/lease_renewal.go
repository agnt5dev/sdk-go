package agnt5

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

const defaultPushLeaseTimeout = 120 * time.Second

type executionLeaseAuthority struct {
	workerID       string
	workerSession  string
	projectID      string
	deploymentID   string
	runID          string
	leaseID        string
	attempt        uint32
	mode           pb.WorkerMode
	leaseTimeout   time.Duration
	leaseExpiresAt time.Time
}

func (a executionLeaseAuthority) request() *pb.RenewJobLeaseRequest {
	attempt := a.attempt
	return &pb.RenewJobLeaseRequest{
		WorkerId:        a.workerID,
		WorkerSessionId: a.workerSession,
		RunId:           a.runID,
		LeaseId:         a.leaseID,
		LeaseTimeoutMs:  a.leaseTimeout.Milliseconds(),
		Attempt:         &attempt,
		Mode:            a.mode,
		ProjectId:       a.projectID,
		DeploymentId:    a.deploymentID,
	}
}

func activeLeaseRenewInterval(leaseTimeout time.Duration) time.Duration {
	if leaseTimeout <= 0 {
		leaseTimeout = defaultPushLeaseTimeout
	}
	interval := leaseTimeout / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	if interval > 60*time.Second {
		interval = 60 * time.Second
	}
	latest := leaseTimeout - time.Millisecond
	if latest < time.Millisecond {
		latest = time.Millisecond
	}
	if interval > latest {
		return latest
	}
	return interval
}

func jitterLeaseRenewInterval(leaseTimeout time.Duration) time.Duration {
	base := activeLeaseRenewInterval(leaseTimeout)
	factor := 0.9 + rand.Float64()*0.2
	interval := time.Duration(float64(base) * factor)
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func activeLeaseDangerRetry(leaseTimeout time.Duration) time.Duration {
	if leaseTimeout <= 0 {
		leaseTimeout = defaultPushLeaseTimeout
	}
	retry := leaseTimeout / 10
	if retry < time.Millisecond {
		return time.Millisecond
	}
	if retry > 5*time.Second {
		return 5 * time.Second
	}
	return retry
}

func startExecutionLeaseRenewal(
	ctx context.Context,
	client pb.EngineServiceClient,
	authority executionLeaseAuthority,
	onAuthorityLost func(pb.LeaseRenewalOutcome),
) func() {
	if client == nil || authority.leaseID == "" || authority.runID == "" {
		return func() {}
	}
	if authority.leaseTimeout <= 0 {
		authority.leaseTimeout = defaultPushLeaseTimeout
	}
	if authority.leaseExpiresAt.IsZero() {
		authority.leaseExpiresAt = time.Now().Add(authority.leaseTimeout)
	}

	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var lostOnce sync.Once
	lose := func(outcome pb.LeaseRenewalOutcome) {
		lostOnce.Do(func() {
			if onAuthorityLost != nil {
				onAuthorityLost(outcome)
			}
		})
	}
	go func() {
		defer close(done)
		delay := jitterLeaseRenewInterval(authority.leaseTimeout)
		// Test and embedded runtimes may use sub-second leases. Renew those
		// immediately because scheduler latency alone can consume the lease;
		// production leases retain the jittered half-life cadence.
		if authority.leaseTimeout < time.Second {
			delay = 0
		}
		for {
			remaining := time.Until(authority.leaseExpiresAt)
			if remaining <= 0 {
				lose(pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_AUTHORITY_LOST)
				return
			}
			if delay > remaining {
				delay = remaining
			}
			if err := sleepContext(renewCtx, delay); err != nil {
				return
			}
			if !time.Now().Before(authority.leaseExpiresAt) {
				lose(pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_AUTHORITY_LOST)
				return
			}

			rpcCtx, rpcCancel := context.WithDeadline(renewCtx, authority.leaseExpiresAt)
			response, err := client.RenewJobLease(rpcCtx, authority.request())
			rpcCancel()
			if err == nil && response.GetRenewed() {
				authority.leaseExpiresAt = time.UnixMilli(response.GetLeaseExpiresAtMs())
				delay = jitterLeaseRenewInterval(authority.leaseTimeout)
				continue
			}
			if err == nil {
				outcome := response.GetOutcome()
				if outcome == pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_UNSPECIFIED {
					outcome = pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_AUTHORITY_LOST
				}
				lose(outcome)
				return
			}
			if isSessionRegistrationRejection(err) {
				lose(pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_SESSION_INACTIVE)
				return
			}
			if renewCtx.Err() != nil {
				return
			}
			delay = activeLeaseDangerRetry(authority.leaseTimeout)
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
