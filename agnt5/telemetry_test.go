package agnt5

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func TestApplicationLogsExportRunAndResourceAttributes(t *testing.T) {
	exporter := &recordingLogExporter{}
	worker := NewWorker(
		"research-service",
		WithWorkerID("worker-1"),
		WithProjectID("project-1"),
		WithDeploymentID("deployment-1"),
		WithWorkspaceID("workspace-1"),
	)
	worker.telemetry = newTelemetry(worker, exporter)
	defer worker.shutdownTelemetry()

	ctx := newContext(context.Background(), Invocation{ID: "invocation-1", RunID: "run-1"}, nil, "project-1")
	ctx.setTelemetry(worker.currentTelemetry())
	ctx.Logger().Info("research started", "query", "durable agents", "attempt", 2)
	if err := worker.currentTelemetry().provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("force flush telemetry: %v", err)
	}

	records := exporter.Records()
	if len(records) != 1 {
		t.Fatalf("expected one OTLP record, got %d", len(records))
	}
	record := records[0]
	if got := record.Body().AsString(); got != "research started" {
		t.Errorf("record body = %q, want research started", got)
	}
	if got := record.SeverityText(); got != "INFO" {
		t.Errorf("severity = %q, want INFO", got)
	}
	attrs := recordAttributes(record)
	for key, want := range map[string]string{
		"log_source":    "application",
		"agnt5.run.id":  "run-1",
		"run_id":        "run-1",
		"field.query":   "durable agents",
		"field.attempt": "2",
	} {
		if got := attrs[key]; got != want {
			t.Errorf("record attribute %s = %q, want %q", key, got, want)
		}
	}
	resourceAttrs := resourceAttributes(record)
	for key, want := range map[string]string{
		"service.name":        "agnt5-worker",
		"service.namespace":   "agnt5",
		"agnt5.app.name":      "research-service",
		"agnt5.worker.id":     "worker-1",
		"service.instance.id": "worker-1",
		"agnt5.project.id":    "project-1",
		"agnt5.deployment.id": "deployment-1",
		"agnt5.workspace.id":  "workspace-1",
	} {
		if got := resourceAttrs[key]; got != want {
			t.Errorf("resource attribute %s = %q, want %q", key, got, want)
		}
	}
	if events := ctx.Events(); len(events) != 1 || events[0].Type != EventTypeLogInfo {
		t.Fatalf("journal log event was not preserved: %#v", events)
	}
}

func TestLifecycleLogsAreRunScoped(t *testing.T) {
	exporter := &recordingLogExporter{}
	worker := NewWorker("service", WithWorkerID("worker-1"))
	worker.telemetry = newTelemetry(worker, exporter)
	defer worker.shutdownTelemetry()

	worker.logLifecycle(context.Background(), Invocation{ID: "invocation-1", RunID: "run-1"}, "ERROR", "component failed", "error", "boom")
	if err := worker.currentTelemetry().provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("force flush telemetry: %v", err)
	}
	records := exporter.Records()
	if len(records) != 1 {
		t.Fatalf("expected one lifecycle record, got %d", len(records))
	}
	if got := recordAttributes(records[0])["agnt5.run.id"]; got != "run-1" {
		t.Errorf("agnt5.run.id = %q, want run-1", got)
	}
	if got := records[0].SeverityText(); got != "ERROR" {
		t.Errorf("severity = %q, want ERROR", got)
	}
}

type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}
	return nil
}

func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }

func (e *recordingLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

func recordAttributes(record sdklog.Record) map[string]string {
	attrs := make(map[string]string)
	record.WalkAttributes(func(attr attribute.KeyValue) bool {
		attrs[string(attr.Key)] = attr.Value.AsString()
		return true
	})
	return attrs
}

func resourceAttributes(record sdklog.Record) map[string]string {
	attrs := make(map[string]string)
	for _, attr := range record.Resource().Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	return attrs
}
