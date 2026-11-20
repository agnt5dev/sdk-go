package agnt5

import (
	"os"
	"testing"
)

// Workers export logs and traces to AGNT5's production collector when no OTLP
// endpoint is set. Point this package's tests at a closed local port instead,
// so running them sends nothing off the machine. Tests that check the fallback
// clear the variable and supply their own collector.
func TestMain(m *testing.M) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	}
	os.Exit(m.Run())
}
