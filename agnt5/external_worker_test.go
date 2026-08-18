package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func clearExternalWorkerEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		envAPIKey,
		envAPIKeyFile,
		envControlPlaneURL,
		envEnvironment,
		envCoordinatorEndpoint,
		envEngineURL,
		envProjectID,
		envDeploymentID,
	} {
		t.Setenv(name, "")
	}
}

func TestExternalWorkerConfigPreservesLegacyAPIKeyWorker(t *testing.T) {
	clearExternalWorkerEnv(t)
	t.Setenv(envAPIKey, "agnt5_sk_legacy")
	t.Setenv(envProjectID, "project-1")
	t.Setenv(envDeploymentID, "deployment-1")
	t.Setenv(envCoordinatorEndpoint, "http://localhost:34186")

	_, enabled, err := externalWorkerConfigFromEnv(legacyRoutingConfiguredFromEnv())
	if err != nil {
		t.Fatalf("externalWorkerConfigFromEnv() error = %v", err)
	}
	if enabled {
		t.Fatal("legacy worker coordinates unexpectedly enabled external bootstrap")
	}
}

func TestExternalWorkerConfigPreservesProgrammaticLegacyRouting(t *testing.T) {
	clearExternalWorkerEnv(t)
	t.Setenv(envAPIKey, "agnt5_sk_legacy")
	worker := NewWorker(
		"legacy-worker",
		WithCoordinatorEndpoint("http://runtime.internal:34186"),
		WithProjectID("project-1"),
		WithDeploymentID("deployment-1"),
	)
	_, enabled, err := externalWorkerConfigFromEnv(worker.legacyRoutingSet)
	if err != nil {
		t.Fatalf("externalWorkerConfigFromEnv() error = %v", err)
	}
	if enabled {
		t.Fatal("programmatic legacy routing unexpectedly enabled external bootstrap")
	}
}

func TestExternalWorkerCredentialFileIsTrimmed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api-key")
	if err := os.WriteFile(path, []byte("  agnt5_sk_file\n"), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	credential, err := (externalWorkerCredential{file: path}).load()
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if credential != "agnt5_sk_file" {
		t.Fatalf("credential = %q", credential)
	}
}

func TestExternalWorkerRequiresTLSOutsideLoopback(t *testing.T) {
	for _, endpoint := range []string{
		"https://runtime.agnt5.com",
		"http://localhost:34182",
		"http://127.0.0.1:34182",
		"http://[::1]:34182",
	} {
		if _, err := validateExternalEndpoint(endpoint, "runtime"); err != nil {
			t.Fatalf("validateExternalEndpoint(%q) error = %v", endpoint, err)
		}
	}
	if _, err := validateExternalEndpoint("http://runtime.agnt5.com:34182", "runtime"); err == nil {
		t.Fatal("remote plaintext runtime endpoint unexpectedly accepted")
	}
	if _, err := validateExternalEndpoint("https://user:password@runtime.agnt5.com", "runtime"); err == nil {
		t.Fatal("runtime endpoint with user info unexpectedly accepted")
	}
}

func TestExternalWorkerDiscoversExchangesAndRefreshes(t *testing.T) {
	var mu sync.Mutex
	tokenRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s", request.Method)
		}
		if request.Header.Get("X-API-KEY") != "agnt5_sk_test" {
			t.Errorf("X-API-KEY = %q", request.Header.Get("X-API-KEY"))
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/worker-discovery":
			var body struct {
				Environment           string   `json:"environment"`
				SupportedAuthProfiles []string `json:"supported_auth_profiles"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode discovery request: %v", err)
			}
			if body.Environment != "production" {
				t.Errorf("environment = %q", body.Environment)
			}
			if len(body.SupportedAuthProfiles) != 2 || body.SupportedAuthProfiles[0] != authProfileBootstrapMTLS {
				t.Errorf("supported auth profiles = %v", body.SupportedAuthProfiles)
			}
			_ = json.NewEncoder(writer).Encode(externalWorkerConnection{
				ProjectID:       "project-1",
				EnvironmentID:   "environment-1",
				DeploymentID:    "deployment-1",
				WorkerPoolID:    "external-deployment-1",
				Placement:       "customer_docker",
				RuntimeEndpoint: "http://127.0.0.1:34182",
				Protocol:        externalWorkerProtocolPullV1,
			})
		case "/api/v1/worker-token":
			var body map[string]string
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode token request: %v", err)
			}
			if body["project_id"] != "project-1" || body["worker_pool_id"] != "external-deployment-1" {
				t.Errorf("token authority = %+v", body)
			}
			mu.Lock()
			tokenRequests++
			requestNumber := tokenRequests
			mu.Unlock()
			expiresAt := time.Now().Add(30 * time.Second)
			if requestNumber > 1 {
				expiresAt = time.Now().Add(5 * time.Minute)
			}
			_ = json.NewEncoder(writer).Encode(externalWorkerTokenResponse{
				WorkloadToken: "token-" + strconv.Itoa(requestNumber),
				TokenType:     "Bearer",
				ExpiresAt:     expiresAt,
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	controlPlane, err := validateExternalEndpoint(server.URL, "control plane")
	if err != nil {
		t.Fatalf("validate test server: %v", err)
	}
	config := externalWorkerBootstrapConfig{
		controlPlaneURL: controlPlane,
		environment:     "production",
		credential:      externalWorkerCredential{inline: "agnt5_sk_test"},
		httpClient:      server.Client(),
	}
	session, err := connectExternalWorker(context.Background(), config)
	if err != nil {
		t.Fatalf("connectExternalWorker() error = %v", err)
	}
	metadata, err := session.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	if metadata["authorization"] != "Bearer token-2" {
		t.Fatalf("authorization = %q", metadata["authorization"])
	}
	metadata, err = session.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("second GetRequestMetadata() error = %v", err)
	}
	if metadata["authorization"] != "Bearer token-2" {
		t.Fatalf("reused authorization = %q", metadata["authorization"])
	}
	mu.Lock()
	defer mu.Unlock()
	if tokenRequests != 2 {
		t.Fatalf("token requests = %d, want 2", tokenRequests)
	}
}

func TestConfigureExternalWorkerDerivesRoutingAndPullMode(t *testing.T) {
	clearExternalWorkerEnv(t)
	requests := 0
	discovery := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/worker-discovery":
			discovery++
			deploymentID := fmt.Sprintf("deployment-discovered-%d", discovery)
			_ = json.NewEncoder(writer).Encode(externalWorkerConnection{
				ProjectID:       "project-discovered",
				EnvironmentID:   "environment-discovered",
				DeploymentID:    deploymentID,
				WorkerPoolID:    "external-" + deploymentID,
				Placement:       "customer_docker",
				RuntimeEndpoint: "http://127.0.0.1:34182",
				Protocol:        externalWorkerProtocolPullV1,
			})
		case "/api/v1/worker-token":
			_ = json.NewEncoder(writer).Encode(externalWorkerTokenResponse{
				WorkloadToken: "runtime-token",
				TokenType:     "Bearer",
				ExpiresAt:     time.Now().Add(5 * time.Minute),
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	t.Setenv(envAPIKey, "agnt5_sk_test")
	t.Setenv(envControlPlaneURL, server.URL)

	worker := NewWorker("external-worker")
	if err := worker.configureExternalWorker(context.Background()); err != nil {
		t.Fatalf("configureExternalWorker() error = %v", err)
	}
	if worker.projectID != "project-discovered" || worker.deploymentID != "deployment-discovered-1" {
		t.Fatalf("routing = project %q deployment %q", worker.projectID, worker.deploymentID)
	}
	if worker.coordinatorEndpoint != "http://127.0.0.1:34182" || worker.engineEndpoint != worker.coordinatorEndpoint {
		t.Fatalf("runtime endpoints = coordinator %q engine %q", worker.coordinatorEndpoint, worker.engineEndpoint)
	}
	if worker.workerMode != WorkerModePull || worker.externalSession == nil {
		t.Fatalf("external worker mode/session = %q/%v", worker.workerMode, worker.externalSession != nil)
	}
	if err := worker.configureExternalWorker(context.Background()); err != nil {
		t.Fatalf("second configureExternalWorker() error = %v", err)
	}
	if requests != 2 {
		t.Fatalf("bootstrap requests = %d, want exactly one discovery and one exchange", requests)
	}

	installedSession := worker.externalSession
	installedDialOptions := len(worker.grpcDialOptions)
	if err := worker.rediscoverExternalWorker(context.Background()); err != nil {
		t.Fatalf("rediscoverExternalWorker() error = %v", err)
	}
	if worker.deploymentID != "deployment-discovered-2" {
		t.Fatalf("rediscovered deployment = %q", worker.deploymentID)
	}
	if worker.externalSession != installedSession {
		t.Fatal("rediscovery replaced the credential object installed in gRPC dial options")
	}
	if len(worker.grpcDialOptions) != installedDialOptions {
		t.Fatalf("rediscovery added gRPC dial options: %d -> %d", installedDialOptions, len(worker.grpcDialOptions))
	}
	metadata, err := installedSession.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("rediscovered GetRequestMetadata() error = %v", err)
	}
	if metadata["authorization"] != "Bearer runtime-token" || requests != 4 {
		t.Fatalf("rediscovered authorization/requests = %q/%d", metadata["authorization"], requests)
	}
}

func TestExternalWorkerConfigRejectsCredentialAmbiguity(t *testing.T) {
	clearExternalWorkerEnv(t)
	t.Setenv(envAPIKey, "agnt5_sk_inline")
	t.Setenv(envAPIKeyFile, "/run/secrets/agnt5-api-key")
	_, _, err := externalWorkerConfigFromEnv(false)
	if err == nil || !strings.Contains(err.Error(), "only one") {
		t.Fatalf("externalWorkerConfigFromEnv() error = %v", err)
	}
}
