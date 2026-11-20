package agnt5

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

type localLogCollector struct {
	collectorlogs.UnimplementedLogsServiceServer
	received chan *collectorlogs.ExportLogsServiceRequest
}

func (c *localLogCollector) Export(_ context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	c.received <- req
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

type localTraceCollector struct {
	collectortrace.UnimplementedTraceServiceServer
	received chan *collectortrace.ExportTraceServiceRequest
}

func (c *localTraceCollector) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	c.received <- req
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func TestTelemetryOTLPDispatchAndShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	logs := &localLogCollector{received: make(chan *collectorlogs.ExportLogsServiceRequest, 10)}
	traces := &localTraceCollector{received: make(chan *collectortrace.ExportTraceServiceRequest, 10)}
	collectorlogs.RegisterLogsServiceServer(server, logs)
	collectortrace.RegisterTraceServiceServer(server, traces)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	for _, signal := range []string{"", "_LOGS", "_TRACES"} {
		t.Setenv("OTEL_EXPORTER_OTLP"+signal+"_ENDPOINT", "http://"+listener.Addr().String())
		t.Setenv("OTEL_EXPORTER_OTLP"+signal+"_HEADERS", "")
	}
	worker := NewWorker("wire-test", WithWorkerID("worker-1"), WithWorkspaceID("12345678-1234-4234-8234-123456789abc"), WithProjectID("project-1"), WithDeploymentID("deployment-1"))
	worker.initializeTelemetry(context.Background())
	t.Cleanup(worker.shutdownTelemetry)
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(io.Discard, nil)))
	if err := RegisterFunction(worker, "greet", func(ctx *Context, input string) (string, error) {
		logger.InfoContext(ctx, "application message")
		return input, nil
	}); err != nil {
		t.Fatal(err)
	}
	const traceID = "0123456789abcdef0123456789abcdef"
	messages := worker.dispatchServiceMessages(context.Background(), &pb.DispatchComponentRequest{
		InvocationId: "run-1", ComponentName: "greet", ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
		InputData: []byte(`"hello"`), Metadata: map[string]string{"traceparent": "00-" + traceID + "-0123456789abcdef-01"},
	})
	if len(messages) == 0 || !messages[len(messages)-1].GetFunctionResponse().GetSuccess() {
		t.Fatal("dispatch failed")
	}
	// Shutdown must drain both exporters without waiting for a batch timer.
	worker.shutdownTelemetry()
	// Exporters have returned from shutdown, so all acknowledged requests are
	// now buffered. Batch timers may have split records across several RPCs.
	logRequest := &collectorlogs.ExportLogsServiceRequest{}
	traceRequest := &collectortrace.ExportTraceServiceRequest{}
	for len(logs.received) > 0 {
		logRequest.ResourceLogs = append(logRequest.ResourceLogs, (<-logs.received).ResourceLogs...)
	}
	for len(traces.received) > 0 {
		traceRequest.ResourceSpans = append(traceRequest.ResourceSpans, (<-traces.received).ResourceSpans...)
	}
	spanCount := 0
	var spanID []byte
	for _, resource := range traceRequest.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				spanCount++
				spanID = span.SpanId
				attrs := make(map[string]string)
				for _, attr := range span.Attributes {
					attrs[attr.Key] = attr.Value.GetStringValue()
				}
				if attrs["agnt5.workspace.id"] != "12345678-1234-4234-8234-123456789abc" || attrs["agnt5.project.id"] != "project-1" || attrs["agnt5.deployment.id"] != "deployment-1" {
					t.Fatalf("wire span identity = %#v", attrs)
				}
			}
		}
	}
	if spanCount != 1 {
		t.Fatalf("dispatch produced %d spans, want 1", spanCount)
	}
	recordCount := 0
	for _, resource := range logRequest.ResourceLogs {
		attrs := map[string]string{}
		for _, attr := range resource.Resource.Attributes {
			attrs[attr.Key] = attr.Value.GetStringValue()
		}
		if attrs["agnt5.app_name"] != "wire-test" || attrs["agnt5.project.id"] != "project-1" || attrs["agnt5.deployment.id"] != "deployment-1" || attrs["agnt5.worker.id"] != "worker-1" {
			t.Fatalf("wire resource = %#v", attrs)
		}
		for _, scope := range resource.ScopeLogs {
			for _, record := range scope.LogRecords {
				recordCount++
				attrs := map[string]string{}
				for _, attr := range record.Attributes {
					attrs[attr.Key] = attr.Value.GetStringValue()
				}
				if attrs["log_source"] != "application" || attrs["agnt5.run.id"] != "run-1" || attrs["run_id"] != "run-1" || attrs["trace_id"] != traceID || string(record.SpanId) != string(spanID) {
					t.Fatalf("wire record correlation = %#v", attrs)
				}
			}
		}
	}
	if recordCount != 4 {
		t.Fatalf("wire logs = %d, want application plus three lifecycle lines", recordCount)
	}
}

func TestTelemetryTraceExporterDefaultsToAGNT5Endpoint(t *testing.T) {
	traces := startLocalTraceCollector(t)
	clearOTLPEndpointEnv(t)
	worker := NewWorker("service", WithWorkerID("worker-1"), WithProjectID("project-1"), WithDeploymentID("deployment-1"))
	// Stand-in for grpc.agnt5.com:3418: the worker must reach it over
	// plaintext gRPC with no OTLP environment at all, as sdk-core does.
	worker.defaultOTLPEndpoint = traces.addr
	worker.initializeTelemetry(context.Background())
	if worker.telemetry == nil || worker.telemetry.provider == nil {
		t.Fatal("log pipeline was disabled")
	}
	if worker.telemetry.traceProvider == nil || worker.telemetry.tracer == nil {
		t.Fatal("trace export disabled when no OTLP endpoint is set")
	}
	if err := RegisterFunction(worker, "greet", func(_ *Context, input string) (string, error) { return input, nil }); err != nil {
		t.Fatal(err)
	}
	messages := worker.dispatchServiceMessages(context.Background(), &pb.DispatchComponentRequest{
		InvocationId: "run-1", ComponentName: "greet", ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION, InputData: []byte(`"hello"`),
	})
	if len(messages) == 0 || !messages[len(messages)-1].GetFunctionResponse().GetSuccess() {
		t.Fatal("dispatch failed")
	}
	worker.shutdownTelemetry()
	spans := traces.drain()
	if len(spans) != 1 || spans[0].span.Name != "function.greet" || spans[0].attrs["agnt5.run.id"] != "run-1" {
		t.Fatalf("spans at default endpoint = %v", spans)
	}
}

func TestTelemetryTraceExporterHonorsTracesExporterNone(t *testing.T) {
	clearOTLPEndpointEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	worker := NewWorker("service")
	worker.defaultOTLPEndpoint = "127.0.0.1:1"
	worker.initializeTelemetry(context.Background())
	t.Cleanup(worker.shutdownTelemetry)
	if worker.telemetry == nil || worker.telemetry.provider == nil {
		t.Fatal("log pipeline was disabled")
	}
	if worker.telemetry.traceProvider != nil || worker.telemetry.tracer != nil {
		t.Fatal("trace export enabled with OTEL_TRACES_EXPORTER=none")
	}
}

// A self-hosted worker bootstraps from a service key and learns its project,
// deployment, workspace and worker IDs from the control plane. Its spans must
// reach the default collector carrying those IDs and the run's trace ID, or
// the run shows no trace.
func TestSelfHostedWorkerRunExportsAttributedSpans(t *testing.T) {
	const (
		workspaceID  = "12345678-1234-4234-8234-123456789abc"
		projectID    = "0b6e2a51-4a8f-4c55-9a8e-6f1f3f1a2b3c"
		deploymentID = "5f0c6a0e-2b7d-4f0e-8a3c-1d2e3f4a5b6c"
		runID        = "01a1fa2f-245c-7c73-af9e-ca128706318d"
		traceID      = "4bf92f3577b34da6a3ce929d0e0e4736"
	)
	traces := startLocalTraceCollector(t)
	clearOTLPEndpointEnv(t)
	clearExternalWorkerEnv(t)
	t.Setenv(envWorkspaceID, "")
	t.Setenv(envWorkerID, "")

	claims, _ := json.Marshal(map[string]string{"workspace_id": workspaceID, "project_id": projectID, "deployment_id": deploymentID})
	token := "eyJhbGciOiJFZERTQSJ9." + base64.RawURLEncoding.EncodeToString(claims) + ".c2lnbmF0dXJl"
	controlPlane := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/worker-discovery":
			_ = json.NewEncoder(writer).Encode(externalWorkerConnection{
				ProjectID: projectID, EnvironmentID: "environment-1", DeploymentID: deploymentID, WorkerPoolID: "pool-1",
				Placement: "self_hosted", RuntimeEndpoint: "http://127.0.0.1:34182", Protocol: externalWorkerProtocolPullV1,
			})
		case "/api/v1/worker-token":
			_ = json.NewEncoder(writer).Encode(externalWorkerTokenResponse{WorkloadToken: token, TokenType: "Bearer", ExpiresAt: time.Now().Add(5 * time.Minute)})
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(controlPlane.Close)
	t.Setenv(envAPIKey, "agnt5_sk_test")
	t.Setenv(envControlPlaneURL, controlPlane.URL)
	t.Setenv(envEnvironment, "production")

	engine := &testEngine{
		job: &pb.JobAssignment{
			JobId: runID, RunId: runID, TraceId: traceID, LeaseId: "lease-1",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_WORKFLOW, ComponentName: "greet",
			InputData: []byte(`"hello"`), Metadata: map[string]string{"workspace_id": "untrusted-workspace"},
		},
		registered: make(chan *pb.RegisterWorkerSessionRequest, 1),
		completed:  make(chan *pb.CompleteJobRequest, 1),
		capacity:   make(chan *pb.ReportWorkerCapacityRequest, 4),
	}
	listener := newTestEngineListener(t, engine)
	worker := NewWorker("self-hosted", WithMaxConcurrency(1), withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))))
	worker.defaultOTLPEndpoint = traces.addr
	worker.checkpointWriter = &recordingCheckpointWriter{}
	if err := RegisterWorkflow(worker, "greet", func(ctx *Context, input string) (string, error) {
		return Step(ctx, "shout", func(*Context) (string, error) { return input + "!", nil })
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case completion := <-engine.completed:
		if !completion.GetSuccess() {
			t.Fatalf("completion = %#v", completion)
		}
	case err := <-done:
		t.Fatalf("worker stopped before completing the run: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("run did not complete")
	}
	cancel()
	// Run returns only after shutdown has flushed the trace queue.
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run: %v", err)
	}

	spans := traces.drain()
	names := map[string]bool{}
	for _, span := range spans {
		names[span.span.Name] = true
		if got := hex.EncodeToString(span.span.TraceId); got != traceID {
			t.Errorf("span %q trace id = %s, want the run's %s", span.span.Name, got, traceID)
		}
		for key, want := range map[string]string{"agnt5.workspace.id": workspaceID, "agnt5.project.id": projectID, "agnt5.deployment.id": deploymentID, "agnt5.run.id": runID} {
			if span.attrs[key] != want {
				t.Errorf("span %q %s = %q, want %q", span.span.Name, key, span.attrs[key], want)
			}
		}
		for key, want := range map[string]string{"agnt5.project.id": projectID, "agnt5.deployment.id": deploymentID, "agnt5.workspace.id": workspaceID, "agnt5.worker.id": worker.WorkerID()} {
			if span.resource[key] != want {
				t.Errorf("span %q resource %s = %q, want %q", span.span.Name, key, span.resource[key], want)
			}
		}
	}
	if !names["workflow.greet"] || !names["workflow.step.shout"] {
		t.Fatalf("exported spans = %v, want the workflow span and its step span", names)
	}
}

type exportedSpan struct {
	span     *tracepb.Span
	attrs    map[string]string
	resource map[string]string
}

func (s exportedSpan) String() string { return s.span.Name }

type testTraceCollector struct {
	addr     string
	received chan *collectortrace.ExportTraceServiceRequest
}

func startLocalTraceCollector(t *testing.T) *testTraceCollector {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	traces := &localTraceCollector{received: make(chan *collectortrace.ExportTraceServiceRequest, 100)}
	collectortrace.RegisterTraceServiceServer(server, traces)
	collectorlogs.RegisterLogsServiceServer(server, &localLogCollector{received: make(chan *collectorlogs.ExportLogsServiceRequest, 100)})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return &testTraceCollector{addr: listener.Addr().String(), received: traces.received}
}

func (c *testTraceCollector) drain() []exportedSpan {
	var spans []exportedSpan
	for len(c.received) > 0 {
		for _, resourceSpans := range (<-c.received).ResourceSpans {
			resource := map[string]string{}
			for _, attr := range resourceSpans.GetResource().GetAttributes() {
				resource[attr.Key] = attr.Value.GetStringValue()
			}
			for _, scope := range resourceSpans.ScopeSpans {
				for _, span := range scope.Spans {
					attrs := map[string]string{}
					for _, attr := range span.Attributes {
						attrs[attr.Key] = attr.Value.GetStringValue()
					}
					spans = append(spans, exportedSpan{span: span, attrs: attrs, resource: resource})
				}
			}
		}
	}
	return spans
}

func clearOTLPEndpointEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "OTEL_TRACES_EXPORTER"} {
		t.Setenv(name, "")
	}
}
