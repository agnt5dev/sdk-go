package agnt5

import (
	"os"
	"path/filepath"
)

const (
	envHealthDir     = "AGNT5_HEALTH_DIR"
	defaultHealthDir = "/tmp/health"
)

// healthMarkerPath returns the readiness marker path consumed by the platform's
// bootstrap health checker (GET /health/ready on the agnt5-init sidecar).
func healthMarkerPath(workerID string) string {
	return filepath.Join(envOrDefault(envHealthDir, defaultHealthDir), "worker_"+workerID+".txt")
}

// writeHealthMarker signals readiness by creating the marker file. Errors are
// deliberately ignored: failing to write the marker must never take down the
// worker.
func (w *Worker) writeHealthMarker() {
	if err := os.MkdirAll(envOrDefault(envHealthDir, defaultHealthDir), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(healthMarkerPath(w.workerID), nil, 0o644)
}

// removeHealthMarker signals not-ready by deleting the marker file.
func (w *Worker) removeHealthMarker() {
	_ = os.Remove(healthMarkerPath(w.workerID))
}
