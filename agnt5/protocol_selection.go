package agnt5

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const protocolErrorTrailer = "agnt5-protocol-error-bin"

// ProtocolError preserves structured v2 failures without exposing generated
// protobuf messages through the public SDK API.
type ProtocolError struct {
	Code       string
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Details    map[string]string
	GRPCCode   codes.Code
	cause      error
	structured bool
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Code == "" {
		return fmt.Sprintf("agnt5: protocol error: %s", e.Message)
	}
	return fmt.Sprintf("agnt5: protocol error %s: %s", e.Code, e.Message)
}

func (e *ProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (w *Worker) negotiateV2(ctx context.Context, client protocolv2.ProtocolServiceClient, requirements []*protocolv2.CapabilityRequirement) (ProtocolDiagnostics, *protocolv2.ProtocolLimits, error) {
	request := &protocolv2.GetCapabilitiesRequest{
		MinimumProtocol: newV2ProtocolVersion(),
		MaximumProtocol: newV2ProtocolVersion(),
		Capabilities:    cloneV2CapabilityRequirements(requirements),
	}
	var trailer metadata.MD
	response, err := client.GetCapabilities(ctx, request, grpc.Trailer(&trailer))
	if err != nil {
		return ProtocolDiagnostics{}, nil, wrapProtocolError(err, trailer)
	}
	selected := response.GetSelectedProtocol()
	if selected.GetMajor() != 2 || selected.GetMinor() != 0 {
		return ProtocolDiagnostics{}, nil, fmt.Errorf("%w: runtime selected unsupported protocol %d.%d for forced v2 mode", ErrRegistrationRejected, selected.GetMajor(), selected.GetMinor())
	}
	if err := validateV2Limits(response.GetLimits()); err != nil {
		return ProtocolDiagnostics{}, nil, fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	if err := validateV2MessageSize(response, response.GetLimits(), "GetCapabilities response"); err != nil {
		return ProtocolDiagnostics{}, nil, fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	capabilities := v2CapabilityMap(response.GetCapabilities())
	if err := validateV2CapabilityRequirements(requirements, capabilities); err != nil {
		return ProtocolDiagnostics{}, nil, fmt.Errorf("%w: %w", ErrRegistrationRejected, err)
	}
	return ProtocolDiagnostics{
		RequestedMode:   ProtocolModeV2,
		SelectedVersion: "v2.0",
		ArtifactVersion: ProtocolArtifactVersion,
		RuntimeName:     response.GetRuntimeName(),
		RuntimeVersion:  response.GetRuntimeVersion(),
		Capabilities:    capabilities,
	}, response.GetLimits(), nil
}

func validateV2Limits(limits *protocolv2.ProtocolLimits) error {
	if limits == nil || limits.GetMaximumMessageBytes() == 0 ||
		limits.GetMaximumInlinePayloadBytes() == 0 || limits.GetMaximumEventBatchBytes() == 0 ||
		limits.GetMaximumEventsPerBatch() == 0 || limits.GetMaximumPayloadBytes() == 0 ||
		limits.GetMaximumPayloadChunkBytes() == 0 {
		return fmt.Errorf("agnt5: runtime returned invalid zero-valued protocol limits")
	}
	return nil
}

func newV2ProtocolVersion() *protocolv2.ProtocolVersion {
	return &protocolv2.ProtocolVersion{Major: 2, Minor: 0}
}

func wrapProtocolError(err error, trailer metadata.MD) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var existing *ProtocolError
	if errors.As(err, &existing) {
		return err
	}
	grpcStatus, _ := status.FromError(err)
	wrapped := &ProtocolError{
		Code:      grpcStatus.Code().String(),
		Message:   grpcStatus.Message(),
		Retryable: grpcStatus.Code() == codes.Unavailable,
		Details:   map[string]string{},
		GRPCCode:  grpcStatus.Code(),
		cause:     err,
	}
	values := trailer.Get(protocolErrorTrailer)
	if len(values) == 0 {
		return wrapped
	}
	var detail protocolv2.ProtocolError
	if unmarshalErr := proto.Unmarshal([]byte(values[len(values)-1]), &detail); unmarshalErr != nil {
		return wrapped
	}
	wrapped.Code = protocolErrorCode(detail.GetCode())
	if detail.GetMessage() != "" {
		wrapped.Message = detail.GetMessage()
	}
	wrapped.Retryable = detail.GetRetryable()
	if retryAfter := detail.GetRetryAfter(); retryAfter != nil {
		wrapped.RetryAfter = retryAfter.AsDuration()
	}
	wrapped.Details = cloneStringMap(detail.GetDetails())
	wrapped.structured = true
	return wrapped
}

func protocolErrorCode(code protocolv2.ProtocolErrorCode) string {
	name := code.String()
	name = strings.TrimPrefix(name, "PROTOCOL_ERROR_CODE_")
	if name == "" {
		return strconv.Itoa(int(code))
	}
	return name
}

func v2FallbackAllowed(mode ProtocolMode, selected *protocolv2.ProtocolVersion, err error) bool {
	if mode != ProtocolModeAuto {
		return false
	}
	if err == nil {
		return selected != nil && selected.GetMajor() == 1
	}
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) {
		return protocolErr.GRPCCode == codes.Unimplemented
	}
	return status.Code(err) == codes.Unimplemented
}

func v2CapabilityMap(capabilities []*protocolv2.Capability) map[string]uint32 {
	out := make(map[string]uint32, len(capabilities))
	for _, capability := range capabilities {
		if capability.GetName() == "" {
			continue
		}
		out[capability.GetName()] = capability.GetVersion()
	}
	return out
}

func validateV2CapabilityRequirements(requirements []*protocolv2.CapabilityRequirement, capabilities map[string]uint32) error {
	for _, requirement := range requirements {
		if requirement == nil || !requirement.GetRequired() {
			continue
		}
		if capabilities[requirement.GetName()] < requirement.GetMinimumVersion() {
			return fmt.Errorf("agnt5: runtime does not satisfy required protocol capability %s@%d", requirement.GetName(), requirement.GetMinimumVersion())
		}
	}
	return nil
}

func cloneV2CapabilityRequirements(in []*protocolv2.CapabilityRequirement) []*protocolv2.CapabilityRequirement {
	out := make([]*protocolv2.CapabilityRequirement, 0, len(in))
	for _, requirement := range in {
		if requirement == nil {
			continue
		}
		out = append(out, proto.Clone(requirement).(*protocolv2.CapabilityRequirement))
	}
	return out
}

func validateV2MessageSize(message proto.Message, limits *protocolv2.ProtocolLimits, label string) error {
	if message == nil || limits == nil {
		return nil
	}
	if size := uint64(proto.Size(message)); size > limits.GetMaximumMessageBytes() {
		return fmt.Errorf("agnt5: %s is %d bytes, exceeding negotiated maximum_message_bytes %d", label, size, limits.GetMaximumMessageBytes())
	}
	return nil
}
