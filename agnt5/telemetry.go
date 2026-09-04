package agnt5

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

const defaultOTLPLogsEndpoint = "grpc.agnt5.com:3418"

// telemetry owns the OTLP log pipeline for a worker. It is intentionally
// best-effort: journal events remain the durable source of truth.
type telemetry struct {
	logger   log.Logger
	provider *sdklog.LoggerProvider
	resource *resource.Resource
}

func (w *Worker) initializeTelemetry(ctx context.Context) {
	w.telemetryMu.Lock()
	defer w.telemetryMu.Unlock()
	if w.telemetryInitialized {
		return
	}
	w.telemetryInitialized = true

	exporterOptions := []otlploggrpc.Option{}
	if !otlpEndpointConfigured() {
		exporterOptions = append(exporterOptions,
			otlploggrpc.WithEndpoint(defaultOTLPLogsEndpoint),
			otlploggrpc.WithInsecure(),
		)
	}
	exporter, err := otlploggrpc.New(ctx, exporterOptions...)
	if err != nil {
		return
	}
	w.telemetry = newTelemetry(w, exporter)
}

func (w *Worker) shutdownTelemetry() {
	w.telemetryMu.Lock()
	telemetry := w.telemetry
	w.telemetry = nil
	w.telemetryMu.Unlock()
	if telemetry == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = telemetry.provider.Shutdown(shutdownCtx)
}

func (w *Worker) currentTelemetry() *telemetry {
	w.telemetryMu.Lock()
	defer w.telemetryMu.Unlock()
	return w.telemetry
}

func newTelemetry(w *Worker, exporter sdklog.Exporter) *telemetry {
	res := telemetryResource(w)
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	return &telemetry{
		logger:   provider.Logger("github.com/agnt5dev/sdk-go/agnt5"),
		provider: provider,
		resource: res,
	}
}

func telemetryResource(w *Worker) *resource.Resource {
	attrs := []attribute.KeyValue{
		attribute.String("service.name", "agnt5-worker"),
		attribute.String("service.namespace", "agnt5"),
		attribute.String("service.version", w.serviceVersion),
		attribute.String("agnt5.app.name", w.serviceName),
		attribute.String("agnt5.worker.id", w.workerID),
		attribute.String("service.instance.id", w.workerID),
	}
	appendIfSet := func(key, value string) {
		if strings.TrimSpace(value) != "" {
			attrs = append(attrs, attribute.String(key, value))
		}
	}
	appendIfSet("agnt5.project.id", w.projectID)
	appendIfSet("agnt5.deployment.id", w.deploymentID)
	appendIfSet("agnt5.workspace.id", w.workspaceID)
	return resource.NewWithAttributes("", attrs...)
}

func (c *Context) logApplication(level, message string, fields map[string]any) {
	if c == nil || c.telemetry == nil {
		return
	}
	c.telemetry.emit(c.Context, level, message, c.RunID(), fields)
}

func (w *Worker) logLifecycle(ctx context.Context, inv Invocation, level, message string, keyvals ...any) {
	telemetry := w.currentTelemetry()
	if telemetry == nil {
		return
	}
	telemetry.emit(ctx, level, message, runIDForTelemetry(inv), keyvalsToMap(keyvals))
}

func runIDForTelemetry(inv Invocation) string {
	if strings.TrimSpace(inv.RunID) != "" {
		return inv.RunID
	}
	return inv.ID
}

func (t *telemetry) emit(ctx context.Context, level, message, runID string, fields map[string]any) {
	if t == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record := log.Record{}
	now := time.Now()
	record.SetTimestamp(now)
	record.SetObservedTimestamp(now)
	record.SetSeverity(severityForLevel(level))
	record.SetSeverityText(level)
	record.SetBody(attribute.StringValue(message))
	attrs := []attribute.KeyValue{
		attribute.String("log_source", "application"),
		attribute.String("agnt5.run.id", runID),
		attribute.String("run_id", runID),
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		attrs = append(attrs, attribute.String("field."+key, stringifyLogValue(fields[key])))
	}
	record.AddAttributes(attrs...)
	t.logger.Emit(ctx, record)
}

func severityForLevel(level string) log.Severity {
	switch level {
	case "DEBUG":
		return log.SeverityDebug
	case "WARN":
		return log.SeverityWarn
	case "ERROR":
		return log.SeverityError
	default:
		return log.SeverityInfo
	}
}

func stringifyLogValue(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(toString(value)), "\n", "\\n"))
}

func toString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func otlpEndpointConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
}
