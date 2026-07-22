package agnt5

import (
	"context"
	"errors"
	"fmt"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultV2PollWait          = 30 * time.Second
	defaultV2RenewInterval     = 30 * time.Second
	defaultV2OperationAttempts = 3
	defaultV2OperationBackoff  = 100 * time.Millisecond
	defaultV2UnregisterTimeout = 3 * time.Second
)

func (w *Worker) runV2(ctx context.Context) error {
	conn, err := dialCoordinator(ctx, w.coordinatorEndpoint, w.grpcDialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()

	diagnostics, err := w.negotiateV2(ctx, newProtocolServiceClient(conn))
	if err != nil {
		return err
	}
	w.setProtocolDiagnostics(diagnostics)

	request, err := w.v2RegisterWorkerRequest()
	if err != nil {
		return err
	}
	client := newV2WorkerServiceClient(conn)
	var trailer metadata.MD
	registration, err := client.RegisterWorker(ctx, request, grpc.Trailer(&trailer))
	if err != nil {
		return wrapProtocolError(err, trailer)
	}
	if len(registration.GetWorkerSessionToken()) == 0 {
		return fmt.Errorf("agnt5: v2 worker registration returned an empty session token")
	}
	selected := registration.GetSelectedProtocol()
	if selected.GetMajor() != 2 || selected.GetMinor() != 0 {
		return fmt.Errorf("agnt5: v2 worker registration selected unsupported protocol %d.%d", selected.GetMajor(), selected.GetMinor())
	}
	if err := validateV2Limits(registration.GetLimits()); err != nil {
		return err
	}

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
	for slot := uint32(0); slot < slots; slot++ {
		go func() {
			errCh <- w.runV2PollSlot(runCtx, client, registration)
		}()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (w *Worker) runV2PollSlot(ctx context.Context, client protocolv2.WorkerServiceClient, registration *protocolv2.RegisterWorkerResponse) error {
	sessionToken := registration.GetWorkerSessionToken()
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
		})
		if err != nil {
			return err
		}
		switch result := response.GetResult().(type) {
		case *protocolv2.PollRunResponse_Execute:
			if err := w.executeV2Run(ctx, client, result.Execute, renewInterval, registration.GetLimits()); err != nil {
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

func pollV2WithRetry(ctx context.Context, client protocolv2.WorkerServiceClient, request *protocolv2.PollRunRequest) (*protocolv2.PollRunResponse, error) {
	backoff := defaultV2OperationBackoff
	for attempt := 1; attempt <= defaultV2OperationAttempts; attempt++ {
		var trailer metadata.MD
		response, err := client.PollRun(ctx, request, grpc.Trailer(&trailer))
		if err == nil {
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

	executionCtx := ctx
	var cancelExecution context.CancelFunc
	if timeout := request.GetExecutionTimeout(); timeout != nil && timeout.AsDuration() > 0 {
		executionCtx, cancelExecution = context.WithTimeout(ctx, timeout.AsDuration())
	} else {
		executionCtx, cancelExecution = context.WithCancel(ctx)
	}
	defer cancelExecution()
	renewErrCh := make(chan error, 1)
	stopRenewal := make(chan struct{})
	go renewV2Lease(executionCtx, client, request.GetExecutionToken(), renewInterval, cancelExecution, stopRenewal, renewErrCh)

	result, invokeErr := w.invoke(executionCtx, invocation)
	close(stopRenewal)
	select {
	case renewErr := <-renewErrCh:
		if renewErr != nil {
			return renewErr
		}
	default:
	}
	annotateUnsupportedV2Events(&result)
	if uint64(len(result.Output)) > limits.GetMaximumInlinePayloadBytes() {
		invokeErr = fmt.Errorf("agnt5: v2 output exceeds negotiated inline payload limit")
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
	response, err := commitV2WithRetry(ctx, client, commit)
	if err != nil {
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

func renewV2Lease(ctx context.Context, client protocolv2.WorkerServiceClient, executionToken []byte, interval time.Duration, cancelExecution context.CancelFunc, stop <-chan struct{}, errCh chan<- error) {
	if interval <= 0 {
		interval = defaultV2RenewInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			var trailer metadata.MD
			response, err := client.RenewRunLease(ctx, &protocolv2.RenewRunLeaseRequest{
				ExecutionToken: cloneBytes(executionToken),
			}, grpc.Trailer(&trailer))
			if err != nil {
				cancelExecution()
				select {
				case errCh <- wrapProtocolError(err, trailer):
				default:
				}
				return
			}
			if response.GetCancellationRequested() {
				cancelExecution()
				select {
				case errCh <- fmt.Errorf("agnt5: v2 execution cancelled by runtime: %s", response.GetCancellationReason()):
				default:
				}
				return
			}
		}
	}
}

func commitV2WithRetry(ctx context.Context, client protocolv2.WorkerServiceClient, request *protocolv2.CommitRunOutcomeRequest) (*protocolv2.CommitRunOutcomeResponse, error) {
	backoff := defaultV2OperationBackoff
	for attempt := 1; attempt <= defaultV2OperationAttempts; attempt++ {
		var trailer metadata.MD
		response, err := client.CommitRunOutcome(ctx, request, grpc.Trailer(&trailer))
		if err == nil {
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
		if until > 0 && until < defaultV2PollWait {
			delay = until
		}
	}
	return sleepContext(ctx, delay)
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
