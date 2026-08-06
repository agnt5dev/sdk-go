package agnt5

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultPollWaitMS          = int64(30_000)
	defaultClaimTimeoutMS      = int64(300_000)
	defaultCapacityEvery       = 15 * time.Second
	defaultPollErrorBackoff    = 250 * time.Millisecond
	defaultPollErrorBackoffMax = 5 * time.Second
	sessionRejectThreshold     = 3
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
	if err := w.applyProtocolNegotiation(
		session.GetSupportedProtocolCapabilities(),
		session.GetRequiredProtocolCapabilities(),
	); err != nil {
		return err
	}
	w.writeHealthMarker()
	defer w.removeHealthMarker()
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
	sessionFailures := make(chan error, 1)
	var sessionTasks sync.WaitGroup
	sessionTasks.Add(1)
	go func() {
		defer sessionTasks.Done()
		w.reportPullCapacity(runCtx, client, sessionID, config, &openPollSlots, &activeSlots, defaultCapacityEvery, sessionFailures)
	}()

	sessionTasks.Add(1)
	errCh := make(chan error, config.desiredSlots)
	go func() {
		defer sessionTasks.Done()
		w.launchPullSlots(runCtx, client, sessionID, config, pollPermits, &openPollSlots, &activeSlots, sessionFailures, errCh, &sessionTasks)
	}()

	var result error
	select {
	case <-ctx.Done():
		result = ctx.Err()
	case err := <-sessionFailures:
		result = err
	case err := <-errCh:
		if err != nil {
			result = err
		}
	}
	cancel()
	sessionTasks.Wait()
	return result
}

func (w *Worker) registerWorkerSessionRequest(config pullSlotConfig) *pb.RegisterWorkerSessionRequest {
	components := append(w.Components(), builtInScorerComponentInfos()...)
	supportedProtocols, requiredProtocols := w.protocolRegistrationCapabilities()
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })
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
		Capabilities:                  protoCapabilities(components),
		Components:                    protoComponentInfos(components),
		ServiceName:                   w.serviceName,
		ServiceVersion:                w.serviceVersion,
		ServiceType:                   w.serviceType,
		SupportedProtocolCapabilities: supportedProtocols,
		RequiredProtocolCapabilities:  requiredProtocols,
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

func (w *Worker) launchPullSlots(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, pollPermits chan struct{}, openPollSlots, activeSlots *atomic.Uint32, sessionFailures chan<- error, errCh chan<- error, sessionTasks *sync.WaitGroup) {
	desiredSlots := clampUint32(config.desiredSlots, config.minSlots, config.maxSlots)
	for slot := uint32(0); slot < desiredSlots; slot++ {
		if slot >= config.minSlots {
			if err := sleepContext(ctx, config.rampThrottle); err != nil {
				return
			}
		}
		sessionTasks.Add(1)
		go func(slot uint32) {
			defer sessionTasks.Done()
			errCh <- w.runPullSlot(ctx, client, sessionID, config, slot, pollPermits, openPollSlots, activeSlots, sessionFailures)
		}(slot)
	}
}

func (w *Worker) runPullSlot(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, slot uint32, pollPermits chan struct{}, openPollSlots, activeSlots *atomic.Uint32, sessionFailures chan<- error) error {
	pollBackoff := defaultPollErrorBackoff
	consecutiveSessionRejects := 0
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
			if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
				consecutiveSessionRejects++
				if consecutiveSessionRejects >= sessionRejectThreshold {
					return fmt.Errorf("agnt5: pull worker session rejected: %w", err)
				}
				if sleepErr := sleepContext(ctx, pollBackoff); sleepErr != nil {
					return sleepErr
				}
				pollBackoff = nextBackoff(pollBackoff, defaultPollErrorBackoffMax)
				continue
			}
			consecutiveSessionRejects = 0
			if sleepErr := sleepContext(ctx, pollBackoff); sleepErr != nil {
				return sleepErr
			}
			pollBackoff = nextBackoff(pollBackoff, defaultPollErrorBackoffMax)
			continue
		}
		consecutiveSessionRejects = 0
		pollBackoff = defaultPollErrorBackoff
		job := pollResp.GetJob()
		if job == nil {
			continue
		}

		activeSlots.Add(1)
		err = w.executePolledJob(ctx, client, sessionID, config.claimTimeoutMS, job, sessionFailures)
		activeSlots.Add(^uint32(0))
		if err != nil {
			return err
		}
	}
}

func (w *Worker) executePolledJob(ctx context.Context, client pb.EngineServiceClient, sessionID string, claimTimeoutMS int64, job *pb.JobAssignment, sessionFailures chan<- error) error {
	if err := validatePolledJobAssignment(job); err != nil {
		return err
	}
	req := dispatchRequestFromJob(job, w.serviceName)
	req.Metadata["worker_id"] = w.workerID
	req.Metadata["worker_session_id"] = sessionID
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	var authorityLost atomic.Bool
	stopRenewal := w.startLeaseRenewal(jobCtx, client, sessionID, job, claimTimeoutMS, func(outcome pb.LeaseRenewalOutcome) {
		authorityLost.Store(true)
		cancelJob()
		if outcome == pb.LeaseRenewalOutcome_LEASE_RENEWAL_OUTCOME_SESSION_INACTIVE {
			signalSessionFailure(sessionFailures, "renew pull lease", fmt.Errorf("worker session is not active"))
		}
	})
	defer stopRenewal()

	messages := w.dispatchServiceMessages(jobCtx, req)
	if authorityLost.Load() {
		return nil
	}
	var terminal *pb.DispatchComponentResponse
	var suspension *pb.WorkerSuspension
	for _, message := range messages {
		response := message.GetFunctionResponse()
		if response == nil {
			continue
		}
		if value := response.GetWorkerSuspension(); value != nil {
			suspension = value
			continue
		}
		if isTerminalPullResponse(response) {
			terminal = response
		}
	}
	if suspension != nil {
		if pullJobStreamingRequested(job.GetMetadata()) {
			flushCtx, cancel := context.WithTimeout(ctx, defaultCompleteJobTimeout)
			_ = w.flushStreamEvents(flushCtx)
			cancel()
		}
		return w.suspendPolledJob(ctx, client, job, suspension)
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

func (w *Worker) suspendPolledJob(ctx context.Context, client pb.EngineServiceClient, job *pb.JobAssignment, suspension *pb.WorkerSuspension) error {
	request, err := w.polledJobSuspensionRequest(job, suspension)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < defaultCompleteJobAttempts; attempt++ {
		suspendCtx, cancel := context.WithTimeout(ctx, defaultCompleteJobTimeout)
		response, callErr := client.SuspendActivation(suspendCtx, request)
		cancel()
		if callErr == nil && response.GetAccepted() {
			return nil
		}
		if callErr != nil {
			lastErr = callErr
		} else {
			lastErr = fmt.Errorf("runtime returned an unaccepted suspension receipt")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt+1 < defaultCompleteJobAttempts {
			if err := sleepContext(ctx, defaultCompleteJobBackoff); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("agnt5: suspend pull job %s: %w", job.GetJobId(), lastErr)
}

func (w *Worker) polledJobSuspensionRequest(job *pb.JobAssignment, suspension *pb.WorkerSuspension) (*pb.SuspendActivationRequest, error) {
	if err := validatePolledJobAssignment(job); err != nil {
		return nil, err
	}
	if suspension == nil {
		return nil, fmt.Errorf("agnt5: pull job %s returned an empty suspension", job.GetJobId())
	}
	return &pb.SuspendActivationRequest{
		ProjectId:        w.projectID,
		RunId:            job.GetRunId(),
		ActivationId:     suspension.GetActivationId(),
		Attempt:          suspension.GetAttempt(),
		FenceToken:       cloneBytes(suspension.GetFenceToken()),
		TimerKey:         suspension.GetTimerKey(),
		ReadyAtMs:        suspension.GetReadyAtMs(),
		InputDigest:      cloneBytes(suspension.GetInputDigest()),
		DefinitionDigest: cloneBytes(suspension.GetDefinitionDigest()),
		Continuation:     cloneBytes(suspension.GetContinuation()),
		DelayMs:          suspension.GetDelayMs(),
	}, nil
}

func dispatchRequestFromJob(job *pb.JobAssignment, serviceName string) *pb.DispatchComponentRequest {
	invocationID := job.GetRunId()
	if invocationID == "" {
		invocationID = job.GetJobId()
	}
	metadata := cloneStringMap(job.GetMetadata())
	metadata["dispatch_mode"] = "pull"
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
	case "run.completed", "run.failed", "run.paused", "workflow.paused":
		return true
	case "run.cancelled":
		return false
	default:
		return resp.GetErrorMessage() != ""
	}
}

func validatePolledJobAssignment(job *pb.JobAssignment) error {
	if job == nil {
		return fmt.Errorf("agnt5: PollJob returned an empty assignment")
	}
	if job.GetJobId() == "" || job.GetRunId() == "" {
		return fmt.Errorf("agnt5: PollJob assignment requires nonempty job_id and run_id")
	}
	if job.GetJobId() != job.GetRunId() {
		return fmt.Errorf(
			"agnt5: PollJob assignment job_id %q does not match run_id %q",
			job.GetJobId(),
			job.GetRunId(),
		)
	}
	if job.GetLeaseId() == "" {
		return fmt.Errorf("agnt5: PollJob assignment for job %s has no typed lease_id", job.GetJobId())
	}
	if job.GetAttempt() < 0 {
		return fmt.Errorf("agnt5: PollJob assignment for job %s has invalid attempt %d", job.GetJobId(), job.GetAttempt())
	}
	return nil
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
	if err := validatePolledJobAssignment(job); err != nil {
		return nil, err
	}
	leaseID := job.GetLeaseId()
	if resp.GetLeaseId() != "" && resp.GetLeaseId() != leaseID {
		return nil, fmt.Errorf("agnt5: refusing to complete pull job %s with stale lease %q, want %q", job.GetJobId(), resp.GetLeaseId(), job.GetLeaseId())
	}
	jobID := job.GetJobId()
	metadata := cloneStringMap(resp.GetMetadata())
	if eventType := resp.GetEventType(); eventType != "" {
		metadata["completion_event_type"] = eventType
	}
	attempt := uint32(job.GetAttempt())
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
		Attempt:         &attempt,
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

func (w *Worker) startLeaseRenewal(ctx context.Context, client pb.EngineServiceClient, sessionID string, job *pb.JobAssignment, fallbackLeaseTimeoutMS int64, onAuthorityLost func(pb.LeaseRenewalOutcome)) func() {
	if job.GetLeaseId() == "" || job.GetRunId() == "" {
		return func() {}
	}
	leaseTimeoutMS := fallbackLeaseTimeoutMS
	if leaseTimeoutMS <= 0 {
		leaseTimeoutMS = defaultClaimTimeoutMS
	}
	attempt := uint32(job.GetAttempt())
	expiresAt := time.Time{}
	if job.GetLeaseExpiresAtMs() > 0 {
		expiresAt = time.UnixMilli(job.GetLeaseExpiresAtMs())
	}
	return startExecutionLeaseRenewal(ctx, client, executionLeaseAuthority{
		workerID:       w.workerID,
		workerSession:  sessionID,
		projectID:      w.projectID,
		deploymentID:   w.deploymentID,
		runID:          job.GetRunId(),
		leaseID:        job.GetLeaseId(),
		attempt:        attempt,
		mode:           pb.WorkerMode_WORKER_MODE_PULL,
		leaseTimeout:   time.Duration(leaseTimeoutMS) * time.Millisecond,
		leaseExpiresAt: expiresAt,
	}, onAuthorityLost)
}

func (w *Worker) reportPullCapacity(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, openPollSlots, activeSlots *atomic.Uint32, reportEvery time.Duration, sessionFailures chan<- error) {
	consecutiveSessionRejects := 0
	report := func() bool {
		_, err := client.ReportWorkerCapacity(ctx, &pb.ReportWorkerCapacityRequest{
			WorkerId:          w.workerID,
			WorkerSessionId:   sessionID,
			OpenPollSlots:     openPollSlots.Load(),
			ActiveSlots:       activeSlots.Load(),
			DesiredSlots:      config.desiredSlots,
			EffectiveMaxSlots: config.maxSlots,
			ObservedAtMs:      time.Now().UnixMilli(),
		})
		if err == nil {
			consecutiveSessionRejects = 0
			return true
		}
		if isSessionRegistrationRejection(err) {
			consecutiveSessionRejects++
			if consecutiveSessionRejects >= sessionRejectThreshold {
				signalSessionFailure(sessionFailures, "report pull capacity", err)
				return false
			}
			return true
		}
		consecutiveSessionRejects = 0
		return true
	}
	if !report() {
		return
	}
	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !report() {
				return
			}
		}
	}
}

func isSessionRegistrationRejection(err error) bool {
	code := status.Code(err)
	return code == codes.PermissionDenied || code == codes.Unauthenticated
}

func signalSessionFailure(sessionFailures chan<- error, operation string, err error) {
	if sessionFailures == nil {
		return
	}
	select {
	case sessionFailures <- fmt.Errorf("agnt5: %s: worker session rejected: %w", operation, err):
	default:
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
