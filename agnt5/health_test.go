package agnt5

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestHealthMarkerWriteAndRemove(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envHealthDir, filepath.Join(dir, "nested"))
	t.Setenv(envWorkerID, "11111111-2222-4333-8444-555555555555")
	w := NewWorker("svc")

	w.writeHealthMarker()
	marker := filepath.Join(dir, "nested", "worker_11111111-2222-4333-8444-555555555555.txt")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker not written: %v", err)
	}

	w.removeHealthMarker()
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker not removed: %v", err)
	}
	w.removeHealthMarker()
}

func TestNewWorkerIDIsUUIDv4(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	id := newWorkerID()
	if !re.MatchString(id) {
		t.Fatalf("newWorkerID() = %q, want RFC-4122 v4 UUID", id)
	}
	if other := newWorkerID(); other == id {
		t.Fatalf("newWorkerID() returned duplicate id %q", id)
	}
}
