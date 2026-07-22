package agnt5

import (
	"fmt"
	"os"
)

// ProtocolArtifactVersion is the immutable public protocol projection used by
// this SDK implementation.
const ProtocolArtifactVersion = "0.1.0-alpha.3"

// ProtocolMode controls worker protocol selection.
type ProtocolMode string

const (
	// ProtocolModeAuto preserves the v1 default during the dual-stack rollout.
	ProtocolModeAuto ProtocolMode = "auto"
	// ProtocolModeV1 forces the existing v1 worker transport.
	ProtocolModeV1 ProtocolMode = "v1"
	// ProtocolModeV2 forces protocol v2 negotiation and pull delivery.
	ProtocolModeV2 ProtocolMode = "v2"
)

// ProtocolDiagnostics is safe to expose in logs and health diagnostics.
type ProtocolDiagnostics struct {
	RequestedMode   ProtocolMode
	SelectedVersion string
	ArtifactVersion string
	RuntimeName     string
	RuntimeVersion  string
	Capabilities    map[string]uint32
	FallbackReason  string
}

func (d ProtocolDiagnostics) clone() ProtocolDiagnostics {
	d.Capabilities = cloneUint32Map(d.Capabilities)
	return d
}

func protocolModeFromEnv() (ProtocolMode, error) {
	return parseProtocolMode(os.Getenv(envProtocolMode))
}

func parseProtocolMode(value string) (ProtocolMode, error) {
	if value == "" {
		return ProtocolModeAuto, nil
	}
	switch ProtocolMode(value) {
	case ProtocolModeAuto, ProtocolModeV1, ProtocolModeV2:
		return ProtocolMode(value), nil
	default:
		return ProtocolModeAuto, fmt.Errorf("agnt5: invalid %s value %q: expected auto, v1, or v2", envProtocolMode, value)
	}
}

func protocolFallbackReason(mode ProtocolMode) string {
	if mode == ProtocolModeAuto {
		return "auto_default_v1"
	}
	return ""
}

func (w *Worker) setProtocolDiagnostics(diagnostics ProtocolDiagnostics) {
	diagnostics = diagnostics.clone()
	w.protocolMu.Lock()
	w.protocolDiagnostics = diagnostics
	w.protocolMu.Unlock()

	w.metadataMu.Lock()
	defer w.metadataMu.Unlock()
	w.metadata["agnt5.protocol.requested"] = string(diagnostics.RequestedMode)
	w.metadata["agnt5.protocol.selected"] = diagnostics.SelectedVersion
	w.metadata["agnt5.protocol.artifact_version"] = diagnostics.ArtifactVersion
	if diagnostics.FallbackReason != "" {
		w.metadata["agnt5.protocol.fallback_reason"] = diagnostics.FallbackReason
	} else {
		delete(w.metadata, "agnt5.protocol.fallback_reason")
	}
}

func cloneUint32Map(in map[string]uint32) map[string]uint32 {
	if len(in) == 0 {
		return map[string]uint32{}
	}
	out := make(map[string]uint32, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
