package agnt5

import (
	"context"
	"errors"
	"testing"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeProtocolClient struct {
	response *protocolv2.GetCapabilitiesResponse
	err      error
}

func (c fakeProtocolClient) GetCapabilities(context.Context, *protocolv2.GetCapabilitiesRequest, ...grpc.CallOption) (*protocolv2.GetCapabilitiesResponse, error) {
	return c.response, c.err
}

func TestNegotiateV2ReturnsSafeDiagnostics(t *testing.T) {
	worker := NewWorker("svc", WithProtocolMode(ProtocolModeV2))
	diagnostics, _, err := worker.negotiateV2(context.Background(), fakeProtocolClient{response: &protocolv2.GetCapabilitiesResponse{
		SelectedProtocol: &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		RuntimeName:      "runtime",
		RuntimeVersion:   "0.1.0-alpha.2",
		Limits: &protocolv2.ProtocolLimits{
			MaximumMessageBytes:       1 << 20,
			MaximumInlinePayloadBytes: 1 << 20,
			MaximumEventBatchBytes:    1 << 20,
			MaximumEventsPerBatch:     1,
			MaximumPayloadBytes:       1 << 20,
			MaximumPayloadChunkBytes:  1 << 20,
		},
		Capabilities: []*protocolv2.Capability{
			{Name: "worker.pull", Version: 1},
		},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.SelectedVersion != "v2.0" || diagnostics.ArtifactVersion != ProtocolArtifactVersion {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	if diagnostics.Capabilities["worker.pull"] != 1 {
		t.Fatalf("capabilities: %#v", diagnostics.Capabilities)
	}
}

func TestNegotiateV2RejectsInvalidLimits(t *testing.T) {
	worker := NewWorker("svc", WithProtocolMode(ProtocolModeV2))
	_, _, err := worker.negotiateV2(context.Background(), fakeProtocolClient{response: &protocolv2.GetCapabilitiesResponse{
		SelectedProtocol: &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		Limits:           &protocolv2.ProtocolLimits{},
	}}, nil)
	if err == nil {
		t.Fatal("expected invalid protocol limits to fail negotiation")
	}
}

func TestNegotiateV2RequiresDeclaredCapabilities(t *testing.T) {
	requirement := &protocolv2.CapabilityRequirement{Name: v2CapabilityTriggersEvent, MinimumVersion: 1, Required: true}
	response := &protocolv2.GetCapabilitiesResponse{
		SelectedProtocol: &protocolv2.ProtocolVersion{Major: 2, Minor: 0},
		Limits: &protocolv2.ProtocolLimits{
			MaximumMessageBytes:       1 << 20,
			MaximumInlinePayloadBytes: 1 << 20,
			MaximumEventBatchBytes:    1 << 20,
			MaximumEventsPerBatch:     1,
			MaximumPayloadBytes:       1 << 20,
			MaximumPayloadChunkBytes:  1 << 20,
		},
	}
	worker := NewWorker("svc", WithProtocolMode(ProtocolModeV2))
	if _, _, err := worker.negotiateV2(context.Background(), fakeProtocolClient{response: response}, []*protocolv2.CapabilityRequirement{requirement}); err == nil {
		t.Fatal("expected missing required capability to fail negotiation")
	} else if !errors.Is(err, ErrRegistrationRejected) || shouldReconnect(err) {
		t.Fatalf("missing capability error = %v, want terminal registration rejection", err)
	}
	response.Capabilities = []*protocolv2.Capability{{Name: v2CapabilityTriggersEvent, Version: 1}}
	if _, _, err := worker.negotiateV2(context.Background(), fakeProtocolClient{response: response}, []*protocolv2.CapabilityRequirement{requirement}); err != nil {
		t.Fatalf("required capability should negotiate: %v", err)
	}
}

func TestV2FallbackClassification(t *testing.T) {
	unimplemented := &ProtocolError{GRPCCode: codes.Unimplemented}
	if !v2FallbackAllowed(ProtocolModeAuto, nil, unimplemented) {
		t.Fatal("auto should allow explicit unimplemented fallback")
	}
	for _, code := range []codes.Code{
		codes.Unauthenticated,
		codes.PermissionDenied,
		codes.InvalidArgument,
		codes.DeadlineExceeded,
		codes.Unavailable,
	} {
		if v2FallbackAllowed(ProtocolModeAuto, nil, &ProtocolError{GRPCCode: code}) {
			t.Fatalf("auto must not fall back for %s", code)
		}
	}
	if v2FallbackAllowed(ProtocolModeV2, nil, unimplemented) {
		t.Fatal("forced v2 must not fall back")
	}
	if !v2FallbackAllowed(ProtocolModeAuto, &protocolv2.ProtocolVersion{Major: 1}, nil) {
		t.Fatal("auto should accept an explicit v1 selection")
	}
}

func TestWrapProtocolErrorPreservesStructuredDetail(t *testing.T) {
	detail := &protocolv2.ProtocolError{
		Code:      protocolv2.ProtocolErrorCode_PROTOCOL_ERROR_CODE_STALE_EXECUTION_TOKEN,
		Message:   "stale execution",
		Retryable: false,
		Details:   map[string]string{"execution": "superseded"},
	}
	encoded, err := proto.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	trailer := metadata.Pairs(protocolErrorTrailer, string(encoded))
	wrapped := wrapProtocolError(status.Error(codes.Aborted, "aborted"), trailer)
	protocolErr, ok := wrapped.(*ProtocolError)
	if !ok {
		t.Fatalf("error type = %T", wrapped)
	}
	if protocolErr.Code != "STALE_EXECUTION_TOKEN" || protocolErr.Message != "stale execution" || protocolErr.Details["execution"] != "superseded" {
		t.Fatalf("protocol error: %#v", protocolErr)
	}
}
