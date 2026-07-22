package agnt5

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultV2PollWait          = 30 * time.Second
	defaultV2RenewInterval     = 30 * time.Second
	defaultV2OperationAttempts = 3
	defaultV2OperationBackoff  = 100 * time.Millisecond
	defaultV2UnregisterTimeout = 3 * time.Second
	defaultV2SessionExpirySkew = 500 * time.Millisecond
)

var (
	errV2ExecutionScoped = errors.New("agnt5: protocol v2 execution ended without a committable outcome")
	errV2SessionExpired  = errors.New("agnt5: protocol v2 worker session expired")
)

func (w *Worker) runV2(ctx context.Context) error {
	conn, err := dialCoordinator(ctx, w.coordinatorEndpoint, w.grpcDialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()

	request, err := w.v2RegisterWorkerRequest()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	diagnostics, negotiatedLimits, err := w.negotiateV2(ctx, newProtocolServiceClient(conn), request.GetCapabilities())
	if err != nil {
		return err
	}
	if err := validateV2MessageSize(request, negotiatedLimits, "RegisterWorker request"); err != nil {
		return fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	client := newV2WorkerServiceClient(conn)
	var trailer metadata.MD
	registration, err := client.RegisterWorker(ctx, request, grpc.Trailer(&trailer))
	if err != nil {
		return wrapProtocolError(err, trailer)
	}
	if len(registration.GetWorkerSessionToken()) == 0 {
		return fmt.Errorf("%w: v2 worker registration returned an empty session token", ErrRegistrationRejected)
	}
	selected := registration.GetSelectedProtocol()
	if selected.GetMajor() != 2 || selected.GetMinor() != 0 {
		return fmt.Errorf("%w: v2 worker registration selected unsupported protocol %d.%d", ErrRegistrationRejected, selected.GetMajor(), selected.GetMinor())
	}
	if err := validateV2Limits(registration.GetLimits()); err != nil {
		return fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	if err := validateV2MessageSize(registration, negotiatedLimits, "RegisterWorker response"); err != nil {
		return fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	if !proto.Equal(negotiatedLimits, registration.GetLimits()) {
		return fmt.Errorf("%w: protocol limits changed between negotiation and worker registration", ErrRegistrationRejected)
	}
	registeredCapabilities := v2CapabilityMap(registration.GetCapabilities())
	if err := validateV2CapabilityRequirements(request.GetCapabilities(), registeredCapabilities); err != nil {
		return fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	expiresAt := registration.GetSessionExpiresAt()
	if expiresAt == nil || expiresAt.CheckValid() != nil || !expiresAt.AsTime().After(time.Now()) {
		return fmt.Errorf("%w: v2 worker registration returned an invalid session expiration", ErrRegistrationRejected)
	}
	diagnostics.Capabilities = registeredCapabilities
	w.setProtocolDiagnostics(diagnostics)

	w.writeHealthMarker()
	defer w.removeHealthMarker()
	defer unregisterV2Worker(client, registration.GetWorkerSessionToken())
	return w.runV2Worker(ctx, client, registration)
}

func unregisterV2Worker(client protocolv2.WorkerServiceClient, sessionToken []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultV2UnregisterTimeout)
	defer cancel()
	_, _ = client.UnregisterWorker(ctx, &protocolv2.UnregisterWorkerRequest{
		WorkerSessionToken: cloneBytes(sessionToken),
		Reason:             "worker stopped",
	})
}

func (w *Worker) runV2Worker(ctx context.Context, client protocolv2.WorkerServiceClient, registration *protocolv2.RegisterWorkerResponse) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	slots := w.maxConcurrency
	if slots == 0 {
		slots = 1
	}
	errCh := make(chan error, slots)
	var slotsDone sync.WaitGroup
	slotsDone.Add(int(slots))
	for slot := uint32(0); slot < slots; slot++ {
		go func() {
			defer slotsDone.Done()
			errCh <- w.runV2PollSlot(runCtx, client, registration)
		}()
	}

	expiryDelay := time.Until(registration.GetSessionExpiresAt().AsTime())
	if expiryDelay < 0 {
		expiryDelay = 0
	}
	expiryTimer := time.NewTimer(expiryDelay)
	defer expiryTimer.Stop()
	var result error
	select {
	case <-ctx.Done():
		result = ctx.Err()
	case err := <-errCh:
		result = err
	case <-expiryTimer.C:
		result = errV2SessionExpired
	}
	cancel()
	slotsDone.Wait()
	return result
}

func (w *Worker) runV2PollSlot(ctx context.Context, client protocolv2.WorkerServiceClient, registration *protocolv2.RegisterWorkerResponse) error {
	sessionToken := registration.GetWorkerSessionToken()
	sessionExpiresAt := registration.GetSessionExpiresAt().AsTime()
	pollWait := v2Duration(registration.GetMaximumPollWait(), defaultV2PollWait)
	renewInterval := v2Duration(registration.GetRecommendedRenewInterval(), defaultV2RenewInterval)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pollID := newWorkerID()
		response, err := pollV2WithRetry(ctx, client, &protocolv2.PollRunRequest{
			WorkerSessionToken: cloneBytes(sessionToken),
			PollId:             pollID,
			WaitTimeout:        durationpb.New(pollWait),
		}, registration.GetLimits())
		if err != nil {
			err = classifyV2SessionError(err, sessionExpiresAt)
			if isV2PollBackpressure(err) {
				if waitErr := sleepContext(ctx, defaultV2OperationBackoff); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}
		switch result := response.GetResult().(type) {
		case *protocolv2.PollRunResponse_Execute:
			if err := w.executeV2Run(ctx, client, result.Execute, renewInterval, registration.GetLimits()); err != nil {
				err = classifyV2SessionError(err, sessionExpiresAt)
				if errors.Is(err, errV2ExecutionScoped) {
					continue
				}
				return err
			}
		case *protocolv2.PollRunResponse_Idle:
			if err := waitForV2Retry(ctx, result.Idle); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: v2 poll returned no result", ErrUnexpectedRuntimeMessage)
		}
	}
}

func pollV2WithRetry(ctx context.Context, client protocolv2.WorkerServiceClient, request *protocolv2.PollRunRequest, limits *protocolv2.ProtocolLimits) (*protocolv2.PollRunResponse, error) {
	if err := validateV2MessageSize(request, limits, "PollRun request"); err != nil {
		return nil, err
	}
	backoff := defaultV2OperationBackoff
	for attempt := 1; attempt <= defaultV2OperationAttempts; attempt++ {
		var trailer metadata.MD
		response, err := client.PollRun(ctx, request, grpc.Trailer(&trailer))
		if err == nil {
			if sizeErr := validateV2MessageSize(response, limits, "PollRun response"); sizeErr != nil {
				return nil, sizeErr
			}
			return response, nil
		}
		wrapped := wrapProtocolError(err, trailer)
		if !v2OperationRetryable(wrapped, true) || attempt == defaultV2OperationAttempts {
			return nil, wrapped
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = nextBackoff(backoff, defaultPollErrorBackoffMax)
	}
	return nil, fmt.Errorf("agnt5: v2 poll retry loop exhausted")
}

func (w *Worker) executeV2Run(ctx context.Context, client protocolv2.WorkerServiceClient, request *protocolv2.ExecuteRunRequest, renewInterval time.Duration, limits *protocolv2.ProtocolLimits) error {
	invocation, err := invocationFromV2Execute(request)
	if err != nil {
		return err
	}
	if request.GetTarget().GetVersion() != w.serviceVersion {
		return fmt.Errorf("%w: v2 target version %q does not match registered version %q", ErrUnexpectedRuntimeMessage, request.GetTarget().GetVersion(), w.serviceVersion)
	}
	if uint64(len(invocation.Input)) > limits.GetMaximumInlinePayloadBytes() {
		return fmt.Errorf("agnt5: v2 input exceeds negotiated inline payload limit")
	}
	if uint64(len(invocation.Input)) > limits.GetMaximumPayloadBytes() {
		return fmt.Errorf("agnt5: v2 input exceeds negotiated payload limit")
	}

	executionCtx := ctx
	var cancelExecution context.CancelFunc
	if timeout := request.GetExecutionTimeout(); timeout != nil && timeout.AsDuration() > 0 {
		executionCtx, cancelExecution = context.WithTimeout(ctx, timeout.AsDuration())
	} else {
		executionCtx, cancelExecution = context.WithCancel(ctx)
	}
	defer cancelExecution()
	renewDone := make(chan error, 1)
	stopRenewal := make(chan struct{})
	go func() {
		renewDone <- renewV2Lease(executionCtx, client, request.GetExecutionToken(), renewInterval, cancelExecution, stopRenewal, limits)
	}()

	result, invokeErr := w.invoke(executionCtx, invocation)
	close(stopRenewal)
	if renewErr := <-renewDone; renewErr != nil {
		return renewErr
	}
	if invokeErr == nil && executionCtx.Err() != nil {
		invokeErr = executionCtx.Err()
	}
	annotateUnsupportedV2Events(&result)
	if uint64(len(result.Output)) > limits.GetMaximumInlinePayloadBytes() {
		invokeErr = fmt.Errorf("agnt5: v2 output exceeds negotiated inline payload limit")
		result.Output = nil
	}
	if uint64(len(result.Output)) > limits.GetMaximumPayloadBytes() {
		invokeErr = fmt.Errorf("agnt5: v2 output exceeds negotiated payload limit")
		result.Output = nil
	}
	outcome, outcomeErr := v2OutcomeFromResult(result, invokeErr)
	if outcomeErr != nil {
		outcome, _ = v2OutcomeFromResult(result, outcomeErr)
	}
	commit := &protocolv2.CommitRunOutcomeRequest{
		ExecutionToken: cloneBytes(request.GetExecutionToken()),
		CommitId:       v2CommitID(request),
		Outcome:        outcome,
	}
	response, err := commitV2WithRetry(ctx, client, commit, limits)
	if err != nil {
		if v2ExecutionAuthorityError(err) || v2OperationRetryable(err, false) {
			return fmt.Errorf("%w: commit outcome: %w", errV2ExecutionScoped, err)
		}
		return err
	}
	switch response.GetDisposition() {
	case protocolv2.CommitDisposition_COMMIT_DISPOSITION_COMMITTED,
		protocolv2.CommitDisposition_COMMIT_DISPOSITION_ALREADY_COMMITTED:
		return nil
	default:
		return fmt.Errorf("agnt5: v2 runtime returned invalid commit disposition %s", response.GetDisposition())
	}
}

func renewV2Lease(ctx context.Context, client protocolv2.WorkerServiceClient, executionToken []byte, interval time.Duration, cancelExecution context.CancelFunc, stop <-chan struct{}, limits *protocolv2.ProtocolLimits) error {
	if interval <= 0 {
		interval = defaultV2RenewInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case <-ticker.C:
			var trailer metadata.MD
			request := &protocolv2.RenewRunLeaseRequest{
				ExecutionToken: cloneBytes(executionToken),
			}
			if err := validateV2MessageSize(request, limits, "RenewRunLease request"); err != nil {
				cancelExecution()
				return err
			}
			response, err := client.RenewRunLease(ctx, request, grpc.Trailer(&trailer))
			if err != nil {
				cancelExecution()
				return fmt.Errorf("%w: renew execution lease: %w", errV2ExecutionScoped, wrapProtocolError(err, trailer))
			}
			if err := validateV2MessageSize(response, limits, "RenewRunLease response"); err != nil {
				cancelExecution()
				return err
			}
			if response.GetCancellationRequested() {
				cancelExecution()
				return fmt.Errorf("%w: runtime requested cancellation: %s", errV2ExecutionScoped, response.GetCancellationReason())
			}
		}
	}
}

func commitV2WithRetry(ctx context.Context, client protocolv2.WorkerServiceClient, request *protocolv2.CommitRunOutcomeRequest, limits *protocolv2.ProtocolLimits) (*protocolv2.CommitRunOutcomeResponse, error) {
	if err := validateV2MessageSize(request, limits, "CommitRunOutcome request"); err != nil {
		return nil, err
	}
	backoff := defaultV2OperationBackoff
	for attempt := 1; attempt <= defaultV2OperationAttempts; attempt++ {
		var trailer metadata.MD
		response, err := client.CommitRunOutcome(ctx, request, grpc.Trailer(&trailer))
		if err == nil {
			if sizeErr := validateV2MessageSize(response, limits, "CommitRunOutcome response"); sizeErr != nil {
				return nil, sizeErr
			}
			return response, nil
		}
		wrapped := wrapProtocolError(err, trailer)
		if !v2OperationRetryable(wrapped, false) || attempt == defaultV2OperationAttempts {
			return nil, wrapped
		}
		if err := sleepContext(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = nextBackoff(backoff, defaultPollErrorBackoffMax)
	}
	return nil, fmt.Errorf("agnt5: v2 commit retry loop exhausted")
}

func v2OperationRetryable(err error, poll bool) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) {
		if v2ExecutionAuthorityError(protocolErr) || protocolErr.Code == "STALE_WORKER_SESSION" {
			return false
		}
		if protocolErr.Retryable {
			return true
		}
		return poll && protocolErr.GRPCCode == codes.ResourceExhausted
	}
	code := status.Code(err)
	return code == codes.Unavailable || (poll && code == codes.ResourceExhausted)
}

func waitForV2Retry(ctx context.Context, idle *protocolv2.PollIdle) error {
	delay := defaultV2OperationBackoff
	if retryAt := idle.GetRetryAt(); retryAt != nil {
		until := time.Until(retryAt.AsTime())
		if until > 0 {
			delay = min(until, defaultV2PollWait)
		}
	}
	return sleepContext(ctx, delay)
}

func classifyV2SessionError(err error, expiresAt time.Time) error {
	if err == nil {
		return nil
	}
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.GRPCCode != codes.Unauthenticated {
		return err
	}
	if protocolErr.structured {
		if protocolErr.Code == "STALE_WORKER_SESSION" {
			return fmt.Errorf("%w: %v", ErrWorkerReplaced, err)
		}
		return err
	}
	if !expiresAt.IsZero() && !time.Now().Add(defaultV2SessionExpirySkew).Before(expiresAt) {
		return fmt.Errorf("%w: %v", errV2SessionExpired, err)
	}
	return fmt.Errorf("%w: %v", ErrWorkerReplaced, err)
}

func isV2PollBackpressure(err error) bool {
	var protocolErr *ProtocolError
	return errors.As(err, &protocolErr) && protocolErr.GRPCCode == codes.ResourceExhausted
}

func v2ExecutionAuthorityError(err error) bool {
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		return false
	}
	switch protocolErr.Code {
	case "INVALID_EXECUTION_TOKEN", "STALE_EXECUTION_TOKEN", "EXECUTION_SUPERSEDED":
		return true
	default:
		return false
	}
}

func v2Duration(value *durationpb.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	duration := value.AsDuration()
	if duration <= 0 {
		return fallback
	}
	return duration
}
