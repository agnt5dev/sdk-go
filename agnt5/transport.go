package agnt5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
)

func (w *Worker) registerOnce(ctx context.Context, client pb.WorkerCoordinatorServiceClient) error {
	stream, err := w.openRegisteredStream(ctx, client)
	if err != nil {
		return err
	}
	return stream.CloseSend()
}

func (w *Worker) runWorkerStream(ctx context.Context, client pb.WorkerCoordinatorServiceClient) error {
	return w.runWorkerStreamWithLeaseClient(ctx, client, nil)
}

func (w *Worker) runWorkerStreamWithLeaseClient(ctx context.Context, client pb.WorkerCoordinatorServiceClient, leaseClient pb.EngineServiceClient) error {
	if ctx == nil {
		ctx = context.Background()
	}
	stream, err := w.openRegisteredStream(ctx, client)
	if err != nil {
		return err
	}
	w.writeHealthMarker()
	defer w.removeHealthMarker()
	defer func() {
		_ = stream.CloseSend()
	}()
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	recvCh := receiveRuntimeMessages(stream)
	doneCh := make(chan dispatchResult)
	leaseLossCh := make(chan executionLeaseLoss, 1)
	inFlight := make(map[string]context.CancelFunc)
	renewalStops := make(map[string]func())
	suppressed := make(map[string]bool)
	slots := w.dispatchSlots()
	for {
		select {
		case <-ctx.Done():
			stopStream()
			cancelInFlight(inFlight)
			stopLeaseRenewals(renewalStops)
			return ctx.Err()
		case received := <-recvCh:
			if errors.Is(received.err, io.EOF) {
				stopStream()
				cancelInFlight(inFlight)
				stopLeaseRenewals(renewalStops)
				return nil
			}
			if received.err != nil {
				stopStream()
				cancelInFlight(inFlight)
				stopLeaseRenewals(renewalStops)
				return received.err
			}
			if err := w.handleRuntimeMessageWithLease(streamCtx, received.message, leaseClient, inFlight, renewalStops, suppressed, leaseLossCh, doneCh, slots); err != nil {
				stopStream()
				cancelInFlight(inFlight)
				stopLeaseRenewals(renewalStops)
				return err
			}
		case loss := <-leaseLossCh:
			suppressed[loss.invocationID] = true
			if cancel, ok := inFlight[loss.invocationID]; ok {
				cancel()
			}
			if stop, ok := renewalStops[loss.invocationID]; ok {
				stop()
				delete(renewalStops, loss.invocationID)
			}
		case result := <-doneCh:
			if stop, ok := renewalStops[result.invocationID]; ok {
				stop()
				delete(renewalStops, result.invocationID)
			}
			if cancel, ok := inFlight[result.invocationID]; ok {
				cancel()
				delete(inFlight, result.invocationID)
			}
			if suppressed[result.invocationID] {
				delete(suppressed, result.invocationID)
				continue
			}
			for _, message := range result.messages {
				if err := stream.Send(message); err != nil {
					stopStream()
					cancelInFlight(inFlight)
					stopLeaseRenewals(renewalStops)
					return err
				}
			}
		}
	}
}

func (w *Worker) openRegisteredStream(ctx context.Context, client pb.WorkerCoordinatorServiceClient) (grpc.BidiStreamingClient[pb.ServiceMessage, pb.RuntimeMessage], error) {
	stream, err := client.WorkerStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(w.registrationServiceMessage()); err != nil {
		return nil, err
	}
	message, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := registrationResponseError(message); err != nil {
		return nil, err
	}
	response := message.GetRegisterServiceResponse()
	if err := w.applyProtocolNegotiation(
		response.GetSupportedProtocolCapabilities(),
		response.GetRequiredProtocolCapabilities(),
	); err != nil {
		return nil, err
	}
	return stream, nil
}

func registrationResponseError(message *pb.RuntimeMessage) error {
	response := message.GetRegisterServiceResponse()
	if response == nil {
		return fmt.Errorf("%w: %v", ErrUnexpectedRuntimeMessage, message.GetMessageType())
	}
	if !response.GetAck() {
		if response.GetError() != "" {
			return fmt.Errorf("%w: %s", ErrRegistrationRejected, response.GetError())
		}
		return ErrRegistrationRejected
	}
	return nil
}

func (w *Worker) handleRuntimeMessage(ctx context.Context, message *pb.RuntimeMessage, inFlight map[string]context.CancelFunc, doneCh chan<- dispatchResult, slots chan struct{}) error {
	return w.handleRuntimeMessageWithLease(
		ctx,
		message,
		nil,
		inFlight,
		make(map[string]func()),
		make(map[string]bool),
		make(chan executionLeaseLoss, 1),
		doneCh,
		slots,
	)
}

func (w *Worker) handleRuntimeMessageWithLease(
	ctx context.Context,
	message *pb.RuntimeMessage,
	leaseClient pb.EngineServiceClient,
	inFlight map[string]context.CancelFunc,
	renewalStops map[string]func(),
	suppressed map[string]bool,
	leaseLossCh chan<- executionLeaseLoss,
	doneCh chan<- dispatchResult,
	slots chan struct{},
) error {
	switch message.GetMessageType() {
	case pb.RuntimeMessageType_WORKER_REPLACED:
		cancelInFlight(inFlight)
		stopLeaseRenewals(renewalStops)
		return ErrWorkerReplaced
	case pb.RuntimeMessageType_COORDINATOR_DRAINING:
		cancelInFlight(inFlight)
		stopLeaseRenewals(renewalStops)
		return ErrCoordinatorDraining
	}

	switch data := message.GetMessageData().(type) {
	case *pb.RuntimeMessage_DispatchComponent:
		if data.DispatchComponent == nil {
			return fmt.Errorf("%w: empty dispatch component", ErrUnexpectedRuntimeMessage)
		}
		invocationID := data.DispatchComponent.GetInvocationId()
		delete(suppressed, invocationID)
		if err := acquireDispatchSlot(ctx, slots); err != nil {
			return err
		}
		dispatchCtx, cancel := context.WithCancel(ctx)
		inFlight[invocationID] = cancel
		if stop, ok := renewalStops[invocationID]; ok {
			stop()
			delete(renewalStops, invocationID)
		}
		if leaseClient != nil && data.DispatchComponent.GetLeaseId() != "" {
			authority, err := w.pushLeaseAuthority(data.DispatchComponent)
			if err != nil {
				suppressed[invocationID] = true
				cancel()
				delete(inFlight, invocationID)
				releaseDispatchSlot(slots)
				return nil
			}
			renewalStops[invocationID] = startExecutionLeaseRenewal(
				dispatchCtx,
				leaseClient,
				authority,
				func(outcome pb.LeaseRenewalOutcome) {
					select {
					case leaseLossCh <- executionLeaseLoss{invocationID: invocationID, outcome: outcome}:
					case <-ctx.Done():
					}
				},
			)
		}
		go func(req *pb.DispatchComponentRequest) {
			defer releaseDispatchSlot(slots)
			result := dispatchResult{
				invocationID: invocationID,
				messages:     w.dispatchServiceMessages(dispatchCtx, req),
			}
			select {
			case doneCh <- result:
			case <-ctx.Done():
			}
		}(data.DispatchComponent)
		return nil
	case *pb.RuntimeMessage_CancelExecution:
		if data.CancelExecution != nil {
			invocationID := data.CancelExecution.GetInvocationId()
			if stop, ok := renewalStops[invocationID]; ok {
				stop()
				delete(renewalStops, invocationID)
			}
			if cancel, ok := inFlight[invocationID]; ok {
				cancel()
			}
		}
		return nil
	case *pb.RuntimeMessage_CoordinatorDraining:
		cancelInFlight(inFlight)
		stopLeaseRenewals(renewalStops)
		return ErrCoordinatorDraining
	default:
		return fmt.Errorf("%w: %v", ErrUnexpectedRuntimeMessage, message.GetMessageType())
	}
}

type executionLeaseLoss struct {
	invocationID string
	outcome      pb.LeaseRenewalOutcome
}

func (w *Worker) pushLeaseAuthority(request *pb.DispatchComponentRequest) (executionLeaseAuthority, error) {
	if request.GetAttempt() < 0 {
		return executionLeaseAuthority{}, fmt.Errorf("agnt5: negative push lease attempt %d", request.GetAttempt())
	}
	projectID := canonicalProjectID(request.GetMetadata())
	if projectID == "" {
		projectID = w.projectID
	}
	deploymentID := request.GetDeploymentId()
	if deploymentID == "" {
		deploymentID = request.GetMetadata()["deployment_id"]
	}
	if deploymentID == "" {
		deploymentID = w.deploymentID
	}
	if projectID == "" || deploymentID == "" {
		return executionLeaseAuthority{}, ErrMissingRoutingMetadata
	}
	leaseTimeout := defaultPushLeaseTimeout
	if raw := request.GetMetadata()["lease_timeout_ms"]; raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			leaseTimeout = time.Duration(parsed) * time.Millisecond
		}
	}
	leaseExpiresAt := time.Time{}
	if raw := request.GetMetadata()["lease_expires_at_ms"]; raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			leaseExpiresAt = time.UnixMilli(parsed)
		}
	}
	return executionLeaseAuthority{
		workerID:       w.workerID,
		projectID:      projectID,
		deploymentID:   deploymentID,
		runID:          runIDFromInvocationID(request.GetInvocationId()),
		leaseID:        request.GetLeaseId(),
		attempt:        uint32(request.GetAttempt()),
		mode:           pb.WorkerMode_WORKER_MODE_PUSH,
		leaseTimeout:   leaseTimeout,
		leaseExpiresAt: leaseExpiresAt,
	}, nil
}

type receivedRuntimeMessage struct {
	message *pb.RuntimeMessage
	err     error
}

type dispatchResult struct {
	invocationID string
	messages     []*pb.ServiceMessage
}

func receiveRuntimeMessages(stream grpc.BidiStreamingClient[pb.ServiceMessage, pb.RuntimeMessage]) <-chan receivedRuntimeMessage {
	out := make(chan receivedRuntimeMessage, 1)
	go func() {
		for {
			message, err := stream.Recv()
			out <- receivedRuntimeMessage{message: message, err: err}
			if err != nil {
				return
			}
		}
	}()
	return out
}

func cancelInFlight(inFlight map[string]context.CancelFunc) {
	for invocationID, cancel := range inFlight {
		cancel()
		delete(inFlight, invocationID)
	}
}

func stopLeaseRenewals(renewals map[string]func()) {
	for invocationID, stop := range renewals {
		stop()
		delete(renewals, invocationID)
	}
}

func (w *Worker) dispatchSlots() chan struct{} {
	if w.maxConcurrency == 0 {
		return nil
	}
	return make(chan struct{}, w.maxConcurrency)
}

func acquireDispatchSlot(ctx context.Context, slots chan struct{}) error {
	if slots == nil {
		return nil
	}
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseDispatchSlot(slots chan struct{}) {
	if slots == nil {
		return
	}
	<-slots
}
