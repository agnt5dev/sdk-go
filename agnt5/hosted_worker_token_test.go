package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// fakeWorkloadBroker records every exchange and mints "runtime-<identity>"
// tokens so tests can tell which identity token a refresh sent.
type fakeWorkloadBroker struct {
	mu       sync.Mutex
	requests []map[string]any
	headers  []http.Header
	paths    []string
	fail     int
	lifetime time.Duration
}

func (b *fakeWorkloadBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paths = append(b.paths, r.Method+" "+r.URL.Path)
	b.headers = append(b.headers, r.Header.Clone())
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	b.requests = append(b.requests, body)
	if b.fail > 0 {
		b.fail--
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"workload token mint failed"}`))
		return
	}
	lifetime := b.lifetime
	if lifetime == 0 {
		lifetime = time.Hour
	}
	identity, _ := body["token"].(string)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":      "runtime-" + identity,
		"expires_at": time.Now().Add(lifetime).UTC().Format(time.RFC3339),
	})
}

func (b *fakeWorkloadBroker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.requests)
}

func writeWorkloadIdentity(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("write identity token: %v", err)
	}
}

func hostedTokenTestSource(t *testing.T, brokerURL, tokenFile string) *hostedWorkloadTokenSource {
	t.Helper()
	t.Setenv(envWorkloadBrokerURL, brokerURL)
	t.Setenv(envWorkloadTokenFile, tokenFile)
	config, enabled, err := hostedWorkloadConfigFromEnv()
	if err != nil || !enabled {
		t.Fatalf("hostedWorkloadConfigFromEnv() = enabled %v, err %v", enabled, err)
	}
	return newHostedWorkloadTokenSource(config, &bytes.Buffer{})
}

func bearerFrom(t *testing.T, source *hostedWorkloadTokenSource) string {
	t.Helper()
	md, err := source.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	return md["authorization"]
}

func TestHostedWorkloadConfigRequiresBrokerAndTokenFile(t *testing.T) {
	for _, tc := range []struct {
		name, broker, file string
		enabled, wantErr   bool
	}{
		{name: "neither"},
		{name: "broker only", broker: "http://broker.local:34189"},
		{name: "token file only", file: "/var/run/token"},
		{name: "both", broker: "http://broker.local:34189", file: "/var/run/token", enabled: true},
		{name: "bad scheme", broker: "grpc://broker.local", file: "/var/run/token", wantErr: true},
		{name: "no host", broker: "http://", file: "/var/run/token", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envWorkloadBrokerURL, tc.broker)
			t.Setenv(envWorkloadTokenFile, tc.file)
			config, enabled, err := hostedWorkloadConfigFromEnv()
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if enabled != tc.enabled {
				t.Fatalf("enabled = %v, want %v", enabled, tc.enabled)
			}
			if enabled && config.tokenFile != tc.file {
				t.Fatalf("tokenFile = %q, want %q", config.tokenFile, tc.file)
			}
		})
	}
}

func TestStartHostedWorkloadTokenSelection(t *testing.T) {
	broker := &fakeWorkloadBroker{}
	server := httptest.NewServer(broker)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "pod-identity")
	t.Setenv(envWorkloadBrokerURL, server.URL)
	t.Setenv(envWorkloadTokenFile, tokenFile)

	t.Run("self-hosted session wins", func(t *testing.T) {
		worker := NewWorker("svc")
		worker.warnWriter = &bytes.Buffer{}
		worker.externalSession = &externalWorkerSession{}
		before := len(worker.grpcDialOptions)
		worker.startHostedWorkloadToken(context.Background())()
		if worker.hostedToken != nil || len(worker.grpcDialOptions) != before {
			t.Fatal("hosted token installed alongside a self-hosted session")
		}
		if broker.count() != 0 {
			t.Fatalf("broker called %d times for a self-hosted worker", broker.count())
		}
	})

	t.Run("hosted worker gets a token", func(t *testing.T) {
		worker := NewWorker("svc")
		worker.warnWriter = &bytes.Buffer{}
		before := len(worker.grpcDialOptions)
		stop := worker.startHostedWorkloadToken(context.Background())
		defer stop()
		if worker.hostedToken == nil || len(worker.grpcDialOptions) != before+1 {
			t.Fatal("hosted token credentials were not installed")
		}
		if got := bearerFrom(t, worker.hostedToken); got != "Bearer runtime-pod-identity" {
			t.Fatalf("authorization = %q", got)
		}
		// A second Run reuses the installed credentials instead of stacking them.
		worker.startHostedWorkloadToken(context.Background())()
		if len(worker.grpcDialOptions) != before+1 {
			t.Fatal("restarting the worker added duplicate credentials")
		}
	})

	t.Run("not configured", func(t *testing.T) {
		t.Setenv(envWorkloadBrokerURL, "")
		worker := NewWorker("svc")
		worker.startHostedWorkloadToken(context.Background())()
		if worker.hostedToken != nil {
			t.Fatal("hosted token installed without broker configuration")
		}
	})
}

func TestHostedWorkloadTokenBrokerRequestShape(t *testing.T) {
	broker := &fakeWorkloadBroker{}
	server := httptest.NewServer(broker)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "  pod-identity\n")
	source := hostedTokenTestSource(t, server.URL+"/", tokenFile)

	if err := source.refresh(context.Background()); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}
	if got := broker.paths[0]; got != "POST /internal/workload/token" {
		t.Fatalf("request = %q", got)
	}
	if got := broker.headers[0].Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := broker.headers[0].Get("Authorization"); got != "" {
		t.Fatalf("broker request carried Authorization %q", got)
	}
	body := broker.requests[0]
	if body["token"] != "pod-identity" {
		t.Fatalf("token = %#v, want trimmed identity", body["token"])
	}
	audience, _ := body["audience"].([]any)
	if len(audience) != 1 || audience[0] != "agnt5-runtime" {
		t.Fatalf("audience = %#v", body["audience"])
	}
	if len(body) != 2 {
		t.Fatalf("unexpected request fields: %#v", body)
	}
}

func TestHostedWorkloadTokenRefreshRereadsTokenFile(t *testing.T) {
	broker := &fakeWorkloadBroker{}
	server := httptest.NewServer(broker)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "identity-1")
	source := hostedTokenTestSource(t, server.URL, tokenFile)

	if err := source.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh() error = %v", err)
	}
	if got := bearerFrom(t, source); got != "Bearer runtime-identity-1" {
		t.Fatalf("first authorization = %q", got)
	}
	writeWorkloadIdentity(t, tokenFile, "identity-2")
	if err := source.refresh(context.Background()); err != nil {
		t.Fatalf("second refresh() error = %v", err)
	}
	if got := broker.requests[1]["token"]; got != "identity-2" {
		t.Fatalf("second exchange sent %#v, want the rotated identity", got)
	}
	if got := bearerFrom(t, source); got != "Bearer runtime-identity-2" {
		t.Fatalf("second authorization = %q", got)
	}
}

func TestHostedWorkloadTokenRefreshesBeforeExpiry(t *testing.T) {
	// A two-second token is refreshed halfway through its life.
	broker := &fakeWorkloadBroker{lifetime: 2 * time.Second}
	server := httptest.NewServer(broker)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "identity-1")
	t.Setenv(envWorkloadBrokerURL, server.URL)
	t.Setenv(envWorkloadTokenFile, tokenFile)
	worker := NewWorker("svc")
	worker.warnWriter = &bytes.Buffer{}
	stop := worker.startHostedWorkloadToken(context.Background())
	defer stop()

	writeWorkloadIdentity(t, tokenFile, "identity-2")
	waitFor(t, 5*time.Second, func() bool {
		return bearerFrom(t, worker.hostedToken) == "Bearer runtime-identity-2"
	})
}

func TestHostedWorkloadTokenBrokerOutageDoesNotStopWorker(t *testing.T) {
	broker := &fakeWorkloadBroker{fail: 2}
	server := httptest.NewServer(broker)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "secret-pod-identity")
	t.Setenv(envWorkloadBrokerURL, server.URL)
	t.Setenv(envWorkloadTokenFile, tokenFile)
	warnings := &lockedBuffer{}
	worker := NewWorker("svc")
	worker.warnWriter = warnings
	stop := worker.startHostedWorkloadToken(context.Background())
	defer stop()

	if got := bearerFrom(t, worker.hostedToken); got != "" {
		t.Fatalf("authorization = %q before the broker recovered", got)
	}
	if !strings.Contains(warnings.String(), "continuing without one") {
		t.Fatalf("startup failure was not reported: %q", warnings.String())
	}
	waitFor(t, 10*time.Second, func() bool {
		return bearerFrom(t, worker.hostedToken) == "Bearer runtime-secret-pod-identity"
	})
	if logged := warnings.String(); strings.Contains(logged, "secret-pod-identity") {
		t.Fatalf("warnings leaked a token: %q", logged)
	}
}

func TestHostedWorkloadTokenOmitsExpiredToken(t *testing.T) {
	source := &hostedWorkloadTokenSource{now: time.Now, token: "stale", expiresAt: time.Now().Add(-time.Second)}
	if got := bearerFrom(t, source); got != "" {
		t.Fatalf("authorization = %q for an expired token", got)
	}
}

func TestHostedWorkloadTokenAttachedToUnaryAndStreamCalls(t *testing.T) {
	broker := &fakeWorkloadBroker{}
	brokerServer := httptest.NewServer(broker)
	defer brokerServer.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeWorkloadIdentity(t, tokenFile, "pod-identity")
	source := hostedTokenTestSource(t, brokerServer.URL, tokenFile)
	if err := source.refresh(context.Background()); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	var mu sync.Mutex
	seen := map[string]string{}
	record := func(ctx context.Context, kind string) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		defer mu.Unlock()
		seen[kind] = strings.Join(md.Get("authorization"), ",")
	}
	runtime := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			record(ctx, "unary")
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			record(stream.Context(), "stream")
			return handler(srv, stream)
		}),
	)
	healthpb.RegisterHealthServer(runtime, health.NewServer())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = runtime.Serve(listener) }()
	defer runtime.Stop()

	conn, err := dialRuntime(context.Background(), "http://"+listener.Addr().String(), grpc.WithPerRPCCredentials(source))
	if err != nil {
		t.Fatalf("dialRuntime() error = %v", err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("unary call: %v", err)
	}
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("stream call: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("stream recv: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, kind := range []string{"unary", "stream"} {
		if got := seen[kind]; got != "Bearer runtime-pod-identity" {
			t.Fatalf("%s call authorization = %q", kind, got)
		}
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
