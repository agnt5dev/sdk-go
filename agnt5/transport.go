package agnt5

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	inFlight := make(map[string]context.CancelFunc)
	slots := w.dispatchSlots()
	for {
		select {
		case <-ctx.Done():
			stopStream()
			cancelInFlight(inFlight)
			return ctx.Err()
		case received := <-recvCh:
			if errors.Is(received.err, io.EOF) {
				stopStream()
				cancelInFlight(inFlight)
				return nil
			}
			if received.err != nil {
				stopStream()
				cancelInFlight(inFlight)
				return received.err
			}
			if err := w.handleRuntimeMessage(streamCtx, received.message, inFlight, doneCh, slots); err != nil {
				stopStream()
				cancelInFlight(inFlight)
				return err
			}
		case result := <-doneCh:
			if cancel, ok := inFlight[result.invocationID]; ok {
				cancel()
				delete(inFlight, result.invocationID)
			}
			for _, message := range result.messages {
				if err := stream.Send(message); err != nil {
					stopStream()
					cancelInFlight(inFlight)
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
	switch message.GetMessageType() {
	case pb.RuntimeMessageType_WORKER_REPLACED:
		cancelInFlight(inFlight)
		return ErrWorkerReplaced
	case pb.RuntimeMessageType_COORDINATOR_DRAINING:
		cancelInFlight(inFlight)
		return ErrCoordinatorDraining
	}

	switch data := message.GetMessageData().(type) {
	case *pb.RuntimeMessage_DispatchComponent:
		if data.DispatchComponent == nil {
			return fmt.Errorf("%w: empty dispatch component", ErrUnexpectedRuntimeMessage)
		}
		invocationID := data.DispatchComponent.GetInvocationId()
		if err := acquireDispatchSlot(ctx, slots); err != nil {
			return err
		}
		dispatchCtx, cancel := context.WithCancel(ctx)
		inFlight[invocationID] = cancel
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
			if cancel, ok := inFlight[data.CancelExecution.GetInvocationId()]; ok {
				cancel()
			}
		}
		return nil
	case *pb.RuntimeMessage_CoordinatorDraining:
		cancelInFlight(inFlight)
		return ErrCoordinatorDraining
	default:
		return fmt.Errorf("%w: %v", ErrUnexpectedRuntimeMessage, message.GetMessageType())
	}
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
