package agnt5

import (
	"context"
	"errors"
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
	defaultPollWaitMS = int64(30_000)
	// A live worker renews at half the lease, so this mostly sets how long a
	// crashed worker's runs wait before another worker takes them.
	defaultClaimTimeoutMS         = int64(60_000)
	defaultCapacityEvery          = 15 * time.Second
	defaultPollErrorBackoff       = 250 * time.Millisecond
	defaultPollErrorBackoffMax    = 5 * time.Second
	sessionRejectThreshold        = 3
	defaultCompleteJobTimeout     = 3 * time.Second
	defaultCompleteJobAttempts    = 3
	defaultCompleteJobBackoff     = 100 * time.Millisecond
	defaultRetireEmptyPolls       = 2
	defaultSlotRampThrottle       = time.Second
	serverSlotScalingV1Capability = "server_slot_scaling_v1"
)

type pullSlotConfig struct {
	minSlots       uint32
	maxSlots       uint32
	claimTimeoutMS int64
	rampThrottle   time.Duration
}

type serverSlotScalingHint struct {
	decision pb.SlotScalingDecision
	delta    uint32
}

type serverSlotScalingGate struct {
	mu         sync.Mutex
	lastChange time.Time
	throttle   time.Duration
}

func (g *serverSlotScalingGate) tryChange() bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.lastChange.IsZero() && now.Sub(g.lastChange) < g.throttle {
		return false
	}
	g.lastChange = now
	return true
}

type pullSlotEventType uint8

const (
	pullSlotStarted pullSlotEventType = iota
	pullSlotExited
)

type pullSlotEvent struct {
	type_         pullSlotEventType
	activeStarted uint32
	slotScaling   *serverSlotScalingHint
	retired       bool
	err           error
}

type definitiveCompletionRejection struct {
	err error
}

func (e *definitiveCompletionRejection) Error() string {
	return e.err.Error()
}

func (e *definitiveCompletionRejection) Unwrap() error {
	return e.err
}

func isDefinitiveCompletionRejection(err error) bool {
	var rejection *definitiveCompletionRejection
	return errors.As(err, &rejection)
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
	w.printConnected()
	if w.externalSession != nil {
		fmt.Printf("AGNT5 worker registered (deployment=%s)\n", w.deploymentID)
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
	scalingGate := &serverSlotScalingGate{throttle: config.rampThrottle}

	var openPollSlots atomic.Uint32
	var activeSlots atomic.Uint32
	var totalSlots atomic.Uint32
	sessionFailures := make(chan error, 1)
	var sessionTasks sync.WaitGroup
	sessionTasks.Add(1)
	go func() {
		defer sessionTasks.Done()
		w.reportPullCapacity(runCtx, client, sessionID, config, &openPollSlots, &activeSlots, &totalSlots, defaultCapacityEvery, sessionFailures)
	}()

	slotEvents := make(chan pullSlotEvent, config.maxSlots*2)
	var nextSlot uint32
	spawnSlots := func(count uint32) {
		for range count {
			w.launchPullSlot(
				runCtx,
				client,
				sessionID,
				config,
				nextSlot,
				&openPollSlots,
				&activeSlots,
				&totalSlots,
				sessionFailures,
				slotEvents,
				&sessionTasks,
			)
			nextSlot++
		}
	}
	spawnSlots(config.minSlots)
	if w.externalSession != nil {
		fmt.Printf("AGNT5 worker ready (deployment=%s min_slots=%d max_slots=%d)\n", w.deploymentID, config.minSlots, config.maxSlots)
	}

	var result error

run:
	for {
		select {
		case <-ctx.Done():
			result = ctx.Err()
			break run
		case err := <-sessionFailures:
			result = err
			break run
		case event := <-slotEvents:
			switch event.type_ {
			case pullSlotStarted:
				spawn := pullHintSpawnCount(
					totalSlots.Load(),
					event.activeStarted,
					config.maxSlots,
					event.slotScaling,
				)
				if spawn > 0 && event.slotScaling != nil && event.slotScaling.decision == pb.SlotScalingDecision_SLOT_SCALING_DECISION_SCALE_UP && !scalingGate.tryChange() {
					spawn = 0
				}
				spawnSlots(spawn)
			case pullSlotExited:
				if event.err != nil {
					result = event.err
					break run
				}
				if !event.retired {
					result = errors.New("agnt5: pull slot exited unexpectedly")
					break run
				}
			}
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
		claimTimeoutMS: claimTimeoutMS,
		rampThrottle:   defaultSlotRampThrottle,
	}
}

func pullRampSpawnCount(totalSlots, activeSlots, maxSlots uint32) uint32 {
	target := activeSlots * 2
	if target > maxSlots {
		target = maxSlots
	}
	if target <= totalSlots {
		return 0
	}
	return target - totalSlots
}

func pullHintSpawnCount(totalSlots, activeSlots, maxSlots uint32, hint *serverSlotScalingHint) uint32 {
	if hint == nil {
		return pullRampSpawnCount(totalSlots, activeSlots, maxSlots)
	}
	if hint.decision != pb.SlotScalingDecision_SLOT_SCALING_DECISION_SCALE_UP {
		return 0
	}
	room := maxSlots - min(totalSlots, maxSlots)
	return min(hint.delta, room)
}

func (w *Worker) serverSlotScalingEnabled() bool {
	w.protocolMu.RLock()
	defer w.protocolMu.RUnlock()
	return w.serverSlotScalingOn
}

func negotiatedServerSlotScalingHint(enabled bool, hint *pb.SlotScalingHint) *serverSlotScalingHint {
	if !enabled || hint == nil {
		return nil
	}
	switch hint.GetDecision() {
	case pb.SlotScalingDecision_SLOT_SCALING_DECISION_HOLD:
		return &serverSlotScalingHint{decision: pb.SlotScalingDecision_SLOT_SCALING_DECISION_HOLD}
	case pb.SlotScalingDecision_SLOT_SCALING_DECISION_SCALE_UP,
		pb.SlotScalingDecision_SLOT_SCALING_DECISION_SCALE_DOWN:
		if hint.GetSuggestedDelta() == 0 {
			return &serverSlotScalingHint{decision: pb.SlotScalingDecision_SLOT_SCALING_DECISION_HOLD}
		}
		return &serverSlotScalingHint{decision: hint.GetDecision(), delta: hint.GetSuggestedDelta()}
	default:
		return nil
	}
}

func tryRetirePullSlot(totalSlots, activeSlots *atomic.Uint32, minSlots uint32) bool {
	for {
		current := totalSlots.Load()
		busy := activeSlots.Load()
		if current <= minSlots+busy {
			return false
		}
		if totalSlots.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

func (w *Worker) launchPullSlot(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, slot uint32, openPollSlots, activeSlots, totalSlots *atomic.Uint32, sessionFailures chan<- error, slotEvents chan<- pullSlotEvent, sessionTasks *sync.WaitGroup) {
	totalSlots.Add(1)
	sessionTasks.Add(1)
	go func() {
		defer sessionTasks.Done()
		retired, err := w.runPullSlot(ctx, client, sessionID, config, slot, openPollSlots, activeSlots, totalSlots, sessionFailures, slotEvents)
		if !retired {
			totalSlots.Add(^uint32(0))
		}
		select {
		case slotEvents <- pullSlotEvent{type_: pullSlotExited, retired: retired, err: err}:
		case <-ctx.Done():
		}
	}()
}

func (w *Worker) runPullSlot(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, slot uint32, openPollSlots, activeSlots, totalSlots *atomic.Uint32, sessionFailures chan<- error, slotEvents chan<- pullSlotEvent) (bool, error) {
	pollBackoff := defaultPollErrorBackoff
	consecutiveSessionRejects := 0
	consecutiveEmptyPolls := 0
	retireThreshold := defaultRetireEmptyPolls + int(slot%2)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		openPollSlots.Add(1)
		pollResp, err := client.PollJob(ctx, &pb.PollJobRequest{
			WorkerId:        w.workerID,
			WorkerSessionId: sessionID,
			WaitMs:          defaultPollWaitMS,
			ClaimTimeoutMs:  config.claimTimeoutMS,
		})
		openPollSlots.Add(^uint32(0))
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
				consecutiveSessionRejects++
				if consecutiveSessionRejects >= sessionRejectThreshold {
					return false, fmt.Errorf("agnt5: pull worker session rejected: %w", err)
				}
				if sleepErr := sleepContext(ctx, pollBackoff); sleepErr != nil {
					return false, sleepErr
				}
				pollBackoff = nextBackoff(pollBackoff, defaultPollErrorBackoffMax)
				continue
			}
			consecutiveSessionRejects = 0
			if sleepErr := sleepContext(ctx, pollBackoff); sleepErr != nil {
				return false, sleepErr
			}
			pollBackoff = nextBackoff(pollBackoff, defaultPollErrorBackoffMax)
			continue
		}
		consecutiveSessionRejects = 0
		pollBackoff = defaultPollErrorBackoff
		slotScaling := negotiatedServerSlotScalingHint(w.serverSlotScalingEnabled(), pollResp.GetSlotScaling())
		job := pollResp.GetJob()
		if job == nil {
			if slotScaling != nil {
				consecutiveEmptyPolls = 0
				if slotScaling.decision == pb.SlotScalingDecision_SLOT_SCALING_DECISION_SCALE_DOWN &&
					tryRetirePullSlot(totalSlots, activeSlots, config.minSlots) {
					return true, nil
				}
				continue
			}
			consecutiveEmptyPolls++
			if consecutiveEmptyPolls >= retireThreshold && tryRetirePullSlot(totalSlots, activeSlots, config.minSlots) {
				return true, nil
			}
			continue
		}
		consecutiveEmptyPolls = 0

		activeStarted := activeSlots.Add(1)
		err = func() error {
			executionCtx := w.coreMetrics.claim(ctx, job.GetRunId(), slot)
			defer executionMetrics(executionCtx).event("released")
			select {
			case slotEvents <- pullSlotEvent{type_: pullSlotStarted, activeStarted: activeStarted, slotScaling: slotScaling}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return w.executePolledJob(executionCtx, client, sessionID, config.claimTimeoutMS, job, sessionFailures)
		}()
		activeSlots.Add(^uint32(0))
		if isDefinitiveCompletionRejection(err) {
			continue
		}
		if err != nil {
			return false, err
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

	// Hold lifecycle events so CompleteJob can carry them. Streaming jobs keep
	// per-event delivery so SSE viewers see boundaries live.
	runID := job.GetRunId()
	if w.pullCompletionLifecycleEnabled() && !pullJobStreamingRequested(job.GetMetadata()) {
		w.beginLifecycleFold(runID)
	}
	messages := func() []*pb.ServiceMessage {
		executionMetrics(jobCtx).event("started")
		defer executionMetrics(jobCtx).event("handler_finished")
		return w.dispatchServiceMessages(jobCtx, req)
	}()
	held := w.endLifecycleFold(runID)
	if authorityLost.Load() {
		// The lease is gone; the runtime drops late lifecycle events after
		// it re-leases the run, so a best-effort inline append is all that
		// remains useful.
		_ = w.writeEventsDirect(ctx, held)
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
		// A suspension has no CompleteJob to carry held events; append them
		// before the run parks so they precede the suspension in the journal.
		if err := w.writeEventsDirect(ctx, held); err != nil {
			return err
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
	return w.completePolledJob(ctx, client, sessionID, job, terminal, held)
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
	if job.GetTraceId() != "" {
		metadata["trace_id"] = job.GetTraceId()
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

func (w *Worker) completePolledJob(ctx context.Context, client pb.EngineServiceClient, sessionID string, job *pb.JobAssignment, resp *pb.DispatchComponentResponse, held []journalEvent) error {
	request, err := w.polledJobCompletionRequest(sessionID, job, resp)
	if err != nil {
		return err
	}
	if len(held) > 0 {
		if records, ok := lifecycleRecordsFromEvents(held); ok && w.pullCompletionLifecycleEnabled() {
			request.LifecycleRecords = records
		} else if err := w.writeEventsDirect(ctx, held); err != nil {
			return err
		}
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
			if isDefinitiveCompletionRejection(err) {
				return err
			}
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
		wrapped := fmt.Errorf("agnt5: complete pull job %s: %w", request.GetJobId(), err)
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.AlreadyExists:
			return &definitiveCompletionRejection{err: wrapped}
		default:
			return wrapped
		}
	}
	if !response.GetAcknowledged() {
		return fmt.Errorf("agnt5: complete pull job %s was not acknowledged", request.GetJobId())
	}
	executionMetrics(ctx).event("acknowledged")
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

func (w *Worker) reportPullCapacity(ctx context.Context, client pb.EngineServiceClient, sessionID string, config pullSlotConfig, openPollSlots, activeSlots, desiredSlots *atomic.Uint32, reportEvery time.Duration, sessionFailures chan<- error) {
	consecutiveSessionRejects := 0
	report := func() bool {
		w.coreMetrics.configured(config.maxSlots)
		_, err := client.ReportWorkerCapacity(ctx, &pb.ReportWorkerCapacityRequest{
			WorkerId:          w.workerID,
			WorkerSessionId:   sessionID,
			OpenPollSlots:     openPollSlots.Load(),
			ActiveSlots:       activeSlots.Load(),
			DesiredSlots:      desiredSlots.Load(),
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
