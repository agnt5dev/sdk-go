package agnt5

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

const (
	defaultPollWaitMS          = int64(30_000)
	defaultClaimTimeoutMS      = int64(300_000)
	defaultCapacityEvery       = 15 * time.Second
	defaultLeaseRenewEvery     = 30 * time.Second
	defaultPollErrorBackoff    = 250 * time.Millisecond
	defaultPollErrorBackoffMax = 5 * time.Second
	defaultCompleteJobTimeout  = 3 * time.Second
	defaultCompleteJobAttempts = 3
	defaultCompleteJobBackoff  = 100 * time.Millisecond
)

type pullSlotConfig struct {
	minSlots       uint32
	maxSlots       uint32
	desiredSlots   uint32
	claimTimeoutMS int64
	rampThrottle   time.Duration
}

func (w *Worker) runPullWorker(ctx context.Context, client pb.EngineServiceClient) error {
	if w.projectID == "" || w.deploymentID == "" {
		return ErrMissingRoutingMetadata
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	config := w.pullSlotConfig()
	session, err := client.RegisterWorkerSession(runCtx, w.registerWorkerSessionRequest(config))
	if err != nil {
		return fmt.Errorf("agnt5: register pull worker session: %w", err)
	}
	sessionID := session.GetWorkerSessionId()
	if sessionID == "" {
		return fmt.Errorf("agnt5: register pull worker session returned empty session id")
	}
	if policy := session.GetEffectiveSlotPolicy(); policy != nil {
		if policy.GetMinSlots() > 0 {
			config.minSlots = clampUint32(policy.GetMinSlots(), 1, config.maxSlots)
		}
		if policy.GetMaxSlots() > 0 {
			config.maxSlots = clampUint32(policy.GetMaxSlots(), config.minSlots, 100)
		}
		if policy.GetRampThrottleMs() > 0 {
			config.rampThrottle = time.Duration(policy.GetRampThrottleMs()) * time.Millisecond
		}
	}
	config.desiredSlots = clampUint32(config.maxSlots, config.minSlots, config.maxSlots)

	var openPollSlots atomic.Uint32
	var activeSlots atomic.Uint32
	// PollJob is a long-lived unary RPC. Keep only the configured minimum
	// parked at once so idle polls cannot starve CompleteJob, lease renewal,
	// and capacity calls on the shared gRPC connection. A claimed job releases
	// its permit before execution, allowing waiting slots to claim immediately
	// and active execution to ramp independently toward maxSlots.
	pollPermits := make(chan struct{}, pollConcurrencyLimit(config))
	go w.reportPullCapacity(runCtx, client, sessionID, config, &openPollSlots, &activeSlots)

	errCh := make(chan error, config.desiredSlots)
	go w.launchPullSlots(runCtx, client, sessionID, config, pollPermits, &openPollSlots, &activeSlots, errCh)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	}
}

func (w *Worker) registerWorkerSessionRequest(config pullSlotConfig) *pb.RegisterWorkerSessionRequest {
	components := w.Components()
	return &pb.RegisterWorkerSessionRequest{
		WorkerId:     w.workerID,
		ProjectId:    w.projectID,
		DeploymentId: w.deploymentID,
		MaxSlots:     config.maxSlots,
		SlotPolicy: &pb.WorkerSlotPolicy{
			MinSlots:          config.minSlots,
			MaxSlots:          config.maxSlots,
			TargetCpuUsage:    0.75,
			TargetMemoryUsage: 0.80,
			RampThrottleMs:    1_000,
		},
		Capabilities:   protoCapabilities(components),
		Components:     protoComponentInfos(components),
		ServiceName:    w.serviceName,
		ServiceVersion: w.serviceVersion,
		ServiceType:    w.serviceType,
	}
}

func (w *Worker) pullSlotConfig() pullSlotConfig {
	maxConcurrency := w.maxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 1
	}
	maxSlots := uint32FromEnvDefault(envMaxSlots, maxConcurrency)
	if maxSlots == 0 {
		maxSlots = maxConcurrency
	}
	maxSlots = clampUint32(maxSlots, 1, 100)
	minSlots := uint32FromEnvDefault(envMinSlots, 1)
	minSlots = clampUint32(minSlots, 1, maxSlots)
	claimTimeoutMS := int64FromEnvDefault(envClaimTimeoutMS, defaultClaimTimeoutMS)
	if claimTimeoutMS <= 0 {
		claimTimeoutMS = defaultClaimTimeoutMS
	}
	return pullSlotConfig{
		minSlots:       minSlots,
		maxSlots:       maxSlots,
		desiredSlots:   minSlots,
		claimTimeoutMS: claimTimeoutMS,
		rampThrottle:   time.Second,
	}
}

func pollConcurrencyLimit(config pullSlotConfig) int {
	return int(clampUint32(config.minSlots, 1, config.maxSlots))
}

func (w *Worker) launchPullSlots(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, pollPermits chan struct{}, openPollSlots, activeSlots *atomic.Uint32, errCh chan<- error) {
	desiredSlots := clampUint32(config.desiredSlots, config.minSlots, config.maxSlots)
	for slot := uint32(0); slot < desiredSlots; slot++ {
		if slot >= config.minSlots {
			if err := sleepContext(ctx, config.rampThrottle); err != nil {
				return
			}
		}
		go func(slot uint32) {
			errCh <- w.runPullSlot(ctx, client, sessionID, config, slot, pollPermits, openPollSlots, activeSlots)
		}(slot)
	}
}

func (w *Worker) runPullSlot(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, slot uint32, pollPermits chan struct{}, openPollSlots, activeSlots *atomic.Uint32) error {
	pollBackoff := defaultPollErrorBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case pollPermits <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		openPollSlots.Add(1)
		pollResp, err := client.PollJob(ctx, &pb.PollJobRequest{
			WorkerId:        w.workerID,
			WorkerSessionId: sessionID,
			WaitMs:          defaultPollWaitMS,
			ClaimTimeoutMs:  config.claimTimeoutMS,
		})
		openPollSlots.Add(^uint32(0))
		<-pollPermits
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if sleepErr := sleepContext(ctx, pollBackoff); sleepErr != nil {
				return sleepErr
			}
			pollBackoff = nextBackoff(pollBackoff, defaultPollErrorBackoffMax)
			continue
		}
		pollBackoff = defaultPollErrorBackoff
		job := pollResp.GetJob()
		if job == nil {
			continue
		}

		activeSlots.Add(1)
		err = w.executePolledJob(ctx, client, sessionID, config.claimTimeoutMS, job)
		activeSlots.Add(^uint32(0))
		if err != nil {
			return err
		}
	}
}

func (w *Worker) executePolledJob(ctx context.Context, client pb.EngineServiceClient, sessionID string, claimTimeoutMS int64, job *pb.JobAssignment) error {
	req := dispatchRequestFromJob(job, w.serviceName)
	stopRenewal := w.startLeaseRenewal(ctx, client, sessionID, job, claimTimeoutMS)
	defer stopRenewal()

	messages := w.dispatchServiceMessages(ctx, req)
	var terminal *pb.DispatchComponentResponse
	for _, message := range messages {
		response := message.GetFunctionResponse()
		if response == nil {
			continue
		}
		if isTerminalPullResponse(response) {
			terminal = response
		}
	}
	if terminal == nil {
		terminal = dispatchResponseFromResult(req, InvocationResult{LeaseID: job.GetLeaseId()}, fmt.Errorf("agnt5: pull job produced no terminal response"))
	}
	if pullJobStreamingRequested(job.GetMetadata()) {
		flushCtx, cancel := context.WithTimeout(ctx, defaultCompleteJobTimeout)
		_ = w.flushStreamEvents(flushCtx)
		cancel()
	}
	return w.completePolledJob(ctx, client, sessionID, job, terminal)
}

func dispatchRequestFromJob(job *pb.JobAssignment, serviceName string) *pb.DispatchComponentRequest {
	invocationID := job.GetRunId()
	if invocationID == "" {
		invocationID = job.GetJobId()
	}
	metadata := cloneStringMap(job.GetMetadata())
	if metadata["dispatch_mode"] == "" {
		metadata["dispatch_mode"] = "pull"
	}
	return &pb.DispatchComponentRequest{
		InvocationId:  invocationID,
		ServiceName:   serviceName,
		ComponentType: job.GetComponentType(),
		ComponentName: job.GetComponentName(),
		InputData:     cloneBytes(job.GetInputData()),
		Metadata:      metadata,
		Attempt:       job.GetAttempt(),
		LeaseId:       job.GetLeaseId(),
		IsStreaming:   pullJobStreamingRequested(job.GetMetadata()),
	}
}

func pullJobStreamingRequested(metadata map[string]string) bool {
	return metadata["is_streaming"] == "true" || metadata["stream_mode"] == "full"
}

func isTerminalPullResponse(resp *pb.DispatchComponentResponse) bool {
	if resp == nil {
		return false
	}
	switch resp.GetEventType() {
	case "run.completed", "run.failed", "run.cancelled", "run.paused", "workflow.paused":
		return true
	default:
		return resp.GetErrorMessage() != ""
	}
}

func (w *Worker) completePolledJob(ctx context.Context, client pb.EngineServiceClient, sessionID string, job *pb.JobAssignment, resp *pb.DispatchComponentResponse) error {
	request, err := w.polledJobCompletionRequest(sessionID, job, resp)
	if err != nil {
		return err
	}
	return w.completePolledJobRequestWithRetry(ctx, client, request, defaultCompleteJobTimeout, defaultCompleteJobAttempts, defaultCompleteJobBackoff)
}

func (w *Worker) completePolledJobWithin(ctx context.Context, client pb.EngineServiceClient, sessionID string, job *pb.JobAssignment, resp *pb.DispatchComponentResponse, timeout time.Duration) error {
	request, err := w.polledJobCompletionRequest(sessionID, job, resp)
	if err != nil {
		return err
	}
	return w.completePolledJobRequestWithin(ctx, client, request, timeout)
}

func (w *Worker) polledJobCompletionRequest(sessionID string, job *pb.JobAssignment, resp *pb.DispatchComponentResponse) (*pb.CompleteJobRequest, error) {
	leaseID := resp.GetLeaseId()
	if leaseID == "" {
		leaseID = job.GetLeaseId()
	}
	if job.GetLeaseId() != "" && resp.GetLeaseId() != "" && resp.GetLeaseId() != job.GetLeaseId() {
		return nil, fmt.Errorf("agnt5: refusing to complete pull job %s with stale lease %q, want %q", job.GetJobId(), resp.GetLeaseId(), job.GetLeaseId())
	}
	jobID := job.GetJobId()
	if jobID == "" {
		jobID = runIDFromInvocationID(resp.GetInvocationId())
	}
	metadata := cloneStringMap(resp.GetMetadata())
	if eventType := resp.GetEventType(); eventType != "" {
		metadata["completion_event_type"] = eventType
	}
	return &pb.CompleteJobRequest{
		JobId:           jobID,
		WorkerId:        w.workerID,
		Success:         resp.GetSuccess(),
		OutputData:      cloneBytes(resp.GetOutputData()),
		ErrorMessage:    resp.GetErrorMessage(),
		ErrorCode:       metadata["error_code"],
		Metadata:        metadata,
		ProjectId:       w.projectID,
		LeaseId:         leaseID,
		WorkerSessionId: sessionID,
	}, nil
}

func (w *Worker) completePolledJobRequestWithRetry(ctx context.Context, client pb.EngineServiceClient, request *pb.CompleteJobRequest, timeout time.Duration, attempts int, backoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := w.completePolledJobRequestWithin(ctx, client, request, timeout); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt+1 < attempts {
			if err := sleepContext(ctx, backoff); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func (w *Worker) completePolledJobRequestWithin(ctx context.Context, client pb.EngineServiceClient, request *pb.CompleteJobRequest, timeout time.Duration) error {
	completionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := client.CompleteJob(completionCtx, request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("agnt5: complete pull job %s: %w", request.GetJobId(), err)
	}
	if !response.GetAcknowledged() {
		return fmt.Errorf("agnt5: complete pull job %s was not acknowledged", request.GetJobId())
	}
	return nil
}

func (w *Worker) startLeaseRenewal(ctx context.Context, client pb.EngineServiceClient, sessionID string, job *pb.JobAssignment, fallbackLeaseTimeoutMS int64) func() {
	if job.GetLeaseId() == "" || job.GetRunId() == "" {
		return func() {}
	}
	leaseTimeoutMS := fallbackLeaseTimeoutMS
	if value := int64FromString(job.GetMetadata()["lease_timeout_ms"]); value > 0 {
		leaseTimeoutMS = value
	}
	interval := defaultLeaseRenewEvery
	if leaseTimeoutMS > 0 {
		half := time.Duration(leaseTimeoutMS/2) * time.Millisecond
		if half > 0 && half < interval {
			interval = half
		}
	}
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				_, err := client.RenewJobLease(renewCtx, &pb.RenewJobLeaseRequest{
					WorkerId:        w.workerID,
					WorkerSessionId: sessionID,
					RunId:           job.GetRunId(),
					LeaseId:         job.GetLeaseId(),
					LeaseTimeoutMs:  leaseTimeoutMS,
				})
				if err != nil && !errors.Is(renewCtx.Err(), context.Canceled) {
					continue
				}
				if renewCtx.Err() != nil {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (w *Worker) reportPullCapacity(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, openPollSlots, activeSlots *atomic.Uint32) {
	report := func() {
		_, _ = client.ReportWorkerCapacity(ctx, &pb.ReportWorkerCapacityRequest{
			WorkerId:          w.workerID,
			WorkerSessionId:   sessionID,
			OpenPollSlots:     openPollSlots.Load(),
			ActiveSlots:       activeSlots.Load(),
			DesiredSlots:      config.desiredSlots,
			EffectiveMaxSlots: config.maxSlots,
			ObservedAtMs:      time.Now().UnixMilli(),
		})
	}
	report()
	ticker := time.NewTicker(defaultCapacityEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}

func clampUint32(value, min, max uint32) uint32 {
	if max < min {
		max = min
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func int64FromEnvDefault(name string, fallback int64) int64 {
	if value, ok := getEnvValue(name); ok {
		if parsed := int64FromString(value); parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func int64FromString(value string) int64 {
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func getEnvValue(name string) (string, bool) {
	value, ok := os.LookupEnv(name)
	return value, ok && value != ""
}
