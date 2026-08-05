package agnt5

import (
	"errors"
	"fmt"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrComponentNotFound        = errors.New("agnt5: component not found")
	ErrDuplicateComponent       = errors.New("agnt5: duplicate component")
	ErrInvalidComponentName     = errors.New("agnt5: invalid component name")
	ErrNilHandler               = errors.New("agnt5: nil handler")
	ErrNilWorker                = errors.New("agnt5: nil worker")
	ErrMissingRoutingMetadata   = errors.New("agnt5: missing project or deployment routing metadata")
	ErrInvalidStepName          = errors.New("agnt5: invalid step name")
	ErrRegistrationRejected     = errors.New("agnt5: worker registration rejected")
	ErrTransportNotImplemented  = errors.New("agnt5: worker transport not implemented")
	ErrUnexpectedRuntimeMessage = errors.New("agnt5: unexpected runtime message")
	ErrWorkerReplaced           = errors.New("agnt5: worker replaced")
	ErrCoordinatorDraining      = errors.New("agnt5: coordinator draining")
	ErrAgentModelRequired       = errors.New("agnt5: agent model is required")
	ErrAgentMaxTurnsExceeded    = errors.New("agnt5: agent max turns exceeded")
	ErrToolNotFound             = errors.New("agnt5: tool not found")
	ErrMCPTransportClosed       = errors.New("agnt5: MCP transport closed")
	ErrDurabilityUnavailable    = errors.New("agnt5: durable activation unavailable")
	ErrNondeterministicReplay   = errors.New("agnt5: non-deterministic replay")
	ErrStaleActivationAuthority = errors.New("agnt5: stale activation authority")
	ErrActivationCancelled      = errors.New("agnt5: activation cancelled")
	ErrActivationContended      = errors.New("agnt5: activation is already active")
	ErrActivationUnknownOutcome = errors.New("agnt5: activation outcome is unknown")
)

// ActivationErrorCode is stable across the Go SDK and runtime activation protocol.
type ActivationErrorCode string

const (
	ActivationErrorDurabilityUnavailable  ActivationErrorCode = "DURABILITY_UNAVAILABLE"
	ActivationErrorNondeterministicReplay ActivationErrorCode = "NON_DETERMINISTIC_REPLAY"
	ActivationErrorStaleAuthority         ActivationErrorCode = "STALE_AUTHORITY"
	ActivationErrorCancelled              ActivationErrorCode = "CANCELLED"
	ActivationErrorContended              ActivationErrorCode = "CONTENDED"
	ActivationErrorUnknownOutcome         ActivationErrorCode = "UNKNOWN_OUTCOME"
	ActivationErrorPayloadConflict        ActivationErrorCode = "PAYLOAD_CONFLICT"
	ActivationErrorIllegalTransition      ActivationErrorCode = "ILLEGAL_TRANSITION"
	ActivationErrorInvalidArgument        ActivationErrorCode = "INVALID_ARGUMENT"
	ActivationErrorReferenceRequired      ActivationErrorCode = "REFERENCE_REQUIRED"
	ActivationErrorStateVersionConflict   ActivationErrorCode = "STATE_VERSION_CONFLICT"
)

// ActivationError preserves a stable correctness error and its durable identity.
type ActivationError struct {
	Code         ActivationErrorCode
	ActivationID string
	Attempt      uint32
	Message      string
	Cause        error
}

func (e *ActivationError) Error() string {
	if e == nil {
		return "agnt5: durable activation error"
	}
	identity := ""
	if e.ActivationID != "" {
		identity = fmt.Sprintf(" activation=%s attempt=%d", e.ActivationID, e.Attempt)
	}
	return fmt.Sprintf("agnt5: durable activation %s%s: %s", e.Code, identity, e.Message)
}

func (e *ActivationError) Unwrap() error { return e.Cause }

func (e *ActivationError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrDurabilityUnavailable:
		return e.Code == ActivationErrorDurabilityUnavailable
	case ErrNondeterministicReplay:
		return e.Code == ActivationErrorNondeterministicReplay
	case ErrStaleActivationAuthority:
		return e.Code == ActivationErrorStaleAuthority
	case ErrActivationCancelled:
		return e.Code == ActivationErrorCancelled
	case ErrActivationContended:
		return e.Code == ActivationErrorContended
	case ErrActivationUnknownOutcome:
		return e.Code == ActivationErrorUnknownOutcome
	default:
		return false
	}
}

func newActivationError(code ActivationErrorCode, message, activationID string, attempt uint32, cause error) error {
	return &ActivationError{Code: code, Message: message, ActivationID: activationID, Attempt: attempt, Cause: cause}
}

func activationRPCError(operation string, err error) error {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return newActivationError(ActivationErrorUnknownOutcome, operation+" failed without a gRPC status", "", 0, err)
	}
	for _, detail := range grpcStatus.Details() {
		activationDetail, ok := detail.(*pb.ActivationErrorDetail)
		if !ok {
			continue
		}
		code := activationErrorCodeFromProto(activationDetail.GetCode())
		return newActivationError(code, activationDetail.GetMessage(), activationDetail.GetActivationId(), activationDetail.GetAttempt(), err)
	}
	code := ActivationErrorUnknownOutcome
	if grpcStatus.Code() == codes.Unimplemented {
		code = ActivationErrorDurabilityUnavailable
	} else if grpcStatus.Code() == codes.Canceled {
		code = ActivationErrorCancelled
	}
	return newActivationError(code, fmt.Sprintf("%s failed without typed activation details: %s", operation, grpcStatus.Message()), "", 0, err)
}

func activationErrorCodeFromProto(code pb.ActivationErrorCode) ActivationErrorCode {
	switch code {
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_NONDETERMINISTIC_REPLAY:
		return ActivationErrorNondeterministicReplay
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_STALE_AUTHORITY:
		return ActivationErrorStaleAuthority
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_UNKNOWN_WRITE_OUTCOME:
		return ActivationErrorUnknownOutcome
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_PAYLOAD_CONFLICT:
		return ActivationErrorPayloadConflict
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_ILLEGAL_TRANSITION:
		return ActivationErrorIllegalTransition
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_REFERENCE_REQUIRED:
		return ActivationErrorReferenceRequired
	case pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_STATE_VERSION_CONFLICT:
		return ActivationErrorStateVersionConflict
	default:
		return ActivationErrorInvalidArgument
	}
}
