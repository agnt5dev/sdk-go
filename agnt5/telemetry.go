package agnt5

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// defaultOTLPEndpoint is AGNT5's OTLP ingest. It is the same plaintext gRPC
// endpoint the Python and TypeScript SDKs export to, so a worker with no OTLP
// configuration, such as one self-hosted outside AGNT5, still reaches it.
const defaultOTLPEndpoint = "grpc.agnt5.com:3418"

// telemetry owns the OTLP log and trace pipelines for a worker. It is intentionally
// best-effort: journal events remain the durable source of truth.
type telemetry struct {
	logger             log.Logger
	provider           *sdklog.LoggerProvider
	resource           *resource.Resource
	traceProvider      *sdktrace.TracerProvider
	tracer             trace.Tracer
	identityAttributes []attribute.KeyValue
}

func (w *Worker) initializeTelemetry(ctx context.Context) {
	w.telemetryMu.Lock()
	defer w.telemetryMu.Unlock()
	if w.telemetryInitialized {
		return
	}
	w.telemetryInitialized = true

	defaultEndpoint := w.defaultOTLPEndpoint
	if defaultEndpoint == "" {
		defaultEndpoint = defaultOTLPEndpoint
	}
	exporterOptions := []otlploggrpc.Option{}
	if !otlpEndpointConfigured() {
		exporterOptions = append(exporterOptions,
			otlploggrpc.WithEndpoint(defaultEndpoint),
			otlploggrpc.WithInsecure(),
		)
	}
	exporter, err := otlploggrpc.New(ctx, exporterOptions...)
	if err != nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")), "none") {
		w.telemetry = newTelemetry(w, exporter)
		return
	}
	// Traces fall back to the same endpoint as logs. Skipping them when no
	// endpoint was set left self-hosted workers with logs but no spans, while
	// the Python and TypeScript SDKs export both by default.
	traceOptions := []otlptracegrpc.Option{}
	if !otlpTraceEndpointConfigured() {
		traceOptions = append(traceOptions,
			otlptracegrpc.WithEndpoint(defaultEndpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
	traceExporter, traceErr := otlptracegrpc.New(ctx, traceOptions...)
	if traceErr != nil {
		w.telemetry = newTelemetry(w, exporter)
		return
	}
	w.telemetry = newTelemetry(w, exporter, traceExporter)
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
	var shutdown sync.WaitGroup
	shutdown.Go(func() { _ = telemetry.provider.Shutdown(shutdownCtx) })
	if telemetry.traceProvider != nil {
		shutdown.Go(func() { _ = telemetry.traceProvider.Shutdown(shutdownCtx) })
	}
	shutdown.Wait()
}

func (w *Worker) currentTelemetry() *telemetry {
	w.telemetryMu.Lock()
	defer w.telemetryMu.Unlock()
	return w.telemetry
}

func newTelemetry(w *Worker, exporter sdklog.Exporter, traceExporters ...sdktrace.SpanExporter) *telemetry {
	res := telemetryResource(w)
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	t := &telemetry{
		logger:   provider.Logger("github.com/agnt5dev/sdk-go/agnt5"),
		provider: provider,
		resource: res,
	}
	// Trace authorization reads span tags, so retain canonical worker identity
	// on each span as well as the resource. Invocation metadata cannot override it.
	for _, attr := range res.Attributes() {
		switch attr.Key {
		case "agnt5.workspace.id", "agnt5.project.id", "agnt5.deployment.id":
			t.identityAttributes = append(t.identityAttributes, attr)
		}
	}
	if len(traceExporters) > 0 && traceExporters[0] != nil {
		t.traceProvider = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithIDGenerator(runtimeTraceIDGenerator{}), sdktrace.WithBatcher(traceExporters[0]))
		t.tracer = t.traceProvider.Tracer("github.com/agnt5dev/sdk-go/agnt5")
	}
	return t
}

func telemetryResource(w *Worker) *resource.Resource {
	attrs := []attribute.KeyValue{
		attribute.String("service.name", "agnt5-worker"),
		attribute.String("service.namespace", "agnt5"),
		attribute.String("service.version", w.serviceVersion),
		attribute.String("agnt5.app.name", w.serviceName),
		attribute.String("agnt5.app_name", w.serviceName),
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
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		attrs = append(attrs, attribute.String("trace_id", sc.TraceID().String()), attribute.String("span_id", sc.SpanID().String()))
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

func otlpTraceEndpointConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
}
