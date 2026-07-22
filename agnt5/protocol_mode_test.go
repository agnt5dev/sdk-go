package agnt5

import (
	"context"
	"strings"
	"testing"
)

func TestProtocolModeDefaultsToAuto(t *testing.T) {
	t.Setenv(envProtocolMode, "")
	worker := NewWorker("svc")
	if worker.ProtocolMode() != ProtocolModeAuto {
		t.Fatalf("protocol mode = %q, want auto", worker.ProtocolMode())
	}
}

func TestProtocolModeOptionOverridesEnvironment(t *testing.T) {
	t.Setenv(envProtocolMode, "invalid")
	worker := NewWorker("svc", WithProtocolMode(ProtocolModeV2))
	if worker.ProtocolMode() != ProtocolModeV2 || worker.protocolConfigErr != nil {
		t.Fatalf("protocol mode = %q err=%v", worker.ProtocolMode(), worker.protocolConfigErr)
	}
}

func TestInvalidProtocolModeFailsAtStartup(t *testing.T) {
	t.Setenv(envProtocolMode, "V2")
	worker := NewWorker("svc")
	err := worker.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), envProtocolMode) {
		t.Fatalf("Run error = %v, want invalid protocol mode", err)
	}
}

func TestProtocolDiagnosticsAreDefensivelyCopied(t *testing.T) {
	worker := NewWorker("svc")
	worker.setProtocolDiagnostics(ProtocolDiagnostics{
		RequestedMode:   ProtocolModeV2,
		SelectedVersion: "v2.0",
		ArtifactVersion: ProtocolArtifactVersion,
		Capabilities:    map[string]uint32{"worker.pull": 1},
	})
	diagnostics := worker.ProtocolDiagnostics()
	diagnostics.Capabilities["worker.pull"] = 99
	if got := worker.ProtocolDiagnostics().Capabilities["worker.pull"]; got != 1 {
		t.Fatalf("capability version = %d, want 1", got)
	}
}
