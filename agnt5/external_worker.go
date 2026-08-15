package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultControlPlaneURL       = "https://api.agnt5.com"
	externalWorkerProtocolPullV1 = "pull.v1"
	externalTokenRefreshSkew     = time.Minute
	envAPIKeyFile                = "AGNT5_API_KEY_FILE"
	envControlPlaneURL           = "AGNT5_CONTROL_PLANE_URL"
	envExternalWorker            = "AGNT5_EXTERNAL_WORKER"
	envEnvironment               = "AGNT5_ENVIRONMENT"
)

type externalWorkerCredential struct {
	inline string
	file   string
}

func (c externalWorkerCredential) load() (string, error) {
	value := c.inline
	if c.file != "" {
		contents, err := os.ReadFile(c.file)
		if err != nil {
			return "", fmt.Errorf("agnt5: read API key file %s: %w", filepath.Clean(c.file), err)
		}
		value = string(contents)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("agnt5: external worker bootstrap credential is empty")
	}
	return value, nil
}

type externalWorkerBootstrapConfig struct {
	controlPlaneURL *url.URL
	environment     string
	credential      externalWorkerCredential
	httpClient      *http.Client
}

type externalWorkerConnection struct {
	ProjectID       string `json:"project_id"`
	EnvironmentID   string `json:"environment_id"`
	DeploymentID    string `json:"deployment_id"`
	WorkerPoolID    string `json:"worker_pool_id"`
	Placement       string `json:"placement"`
	RuntimeEndpoint string `json:"runtime_endpoint"`
	Protocol        string `json:"protocol"`
}

type externalWorkerTokenResponse struct {
	WorkloadToken string    `json:"workload_token"`
	TokenType     string    `json:"token_type"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type externalWorkerSession struct {
	config     externalWorkerBootstrapConfig
	connection externalWorkerConnection
	mu         sync.Mutex
	token      string
	expiresAt  time.Time
}

func externalWorkerConfigFromEnv(legacyRoutingSet bool) (externalWorkerBootstrapConfig, bool, error) {
	keyFile := strings.TrimSpace(os.Getenv(envAPIKeyFile))
	inlineKey := strings.TrimSpace(os.Getenv(envAPIKey))
	explicit := envBool(envExternalWorker)
	if keyFile == "" && !explicit && (inlineKey == "" || legacyRoutingSet) {
		return externalWorkerBootstrapConfig{}, false, nil
	}
	if keyFile != "" && inlineKey != "" {
		return externalWorkerBootstrapConfig{}, false, fmt.Errorf("agnt5: configure only one of %s or %s", envAPIKeyFile, envAPIKey)
	}
	if keyFile == "" && inlineKey == "" {
		return externalWorkerBootstrapConfig{}, false, fmt.Errorf("agnt5: %s requires %s or %s", envExternalWorker, envAPIKeyFile, envAPIKey)
	}
	controlPlane := strings.TrimSpace(os.Getenv(envControlPlaneURL))
	if controlPlane == "" {
		controlPlane = defaultControlPlaneURL
	}
	parsed, err := validateExternalEndpoint(controlPlane, "control plane")
	if err != nil {
		return externalWorkerBootstrapConfig{}, false, err
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("external worker bootstrap redirects are not allowed")
		},
	}
	return externalWorkerBootstrapConfig{
		controlPlaneURL: parsed,
		environment:     strings.TrimSpace(os.Getenv(envEnvironment)),
		credential:      externalWorkerCredential{inline: inlineKey, file: keyFile},
		httpClient:      client,
	}, true, nil
}

func connectExternalWorker(ctx context.Context, config externalWorkerBootstrapConfig) (*externalWorkerSession, error) {
	credential, err := config.credential.load()
	if err != nil {
		return nil, err
	}
	var connection externalWorkerConnection
	if err := externalWorkerRequest(ctx, config, credential, "api/v1/worker-discovery", map[string]string{
		"environment": config.environment,
	}, &connection); err != nil {
		return nil, err
	}
	if err := validateExternalWorkerConnection(connection); err != nil {
		return nil, err
	}
	session := &externalWorkerSession{config: config, connection: connection}
	if err := session.refreshLocked(ctx, credential); err != nil {
		return nil, err
	}
	return session, nil
}

// GetRequestMetadata implements credentials.PerRPCCredentials. The bootstrap
// key remains on the customer host; only the short-lived runtime token is sent
// on gRPC calls.
func (s *externalWorkerSession) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" || time.Until(s.expiresAt) <= externalTokenRefreshSkew {
		credential, err := s.config.credential.load()
		if err != nil {
			return nil, err
		}
		if err := s.refreshLocked(ctx, credential); err != nil {
			return nil, err
		}
	}
	return map[string]string{"authorization": "Bearer " + s.token}, nil
}

// Endpoint validation already rejects non-loopback plaintext. Returning false
// here permits the explicit localhost development exception while production
// endpoints remain protected by verified TLS transport credentials.
func (*externalWorkerSession) RequireTransportSecurity() bool { return false }

func (s *externalWorkerSession) refreshLocked(ctx context.Context, credential string) error {
	var token externalWorkerTokenResponse
	err := externalWorkerRequest(ctx, s.config, credential, "api/v1/worker-token", map[string]string{
		"project_id":     s.connection.ProjectID,
		"environment_id": s.connection.EnvironmentID,
		"deployment_id":  s.connection.DeploymentID,
		"worker_pool_id": s.connection.WorkerPoolID,
	}, &token)
	if err != nil {
		return err
	}
	if strings.TrimSpace(token.WorkloadToken) == "" || !strings.EqualFold(token.TokenType, "Bearer") || !token.ExpiresAt.After(time.Now()) {
		return errors.New("agnt5: worker token exchange returned an invalid credential")
	}
	s.token = token.WorkloadToken
	s.expiresAt = token.ExpiresAt
	return nil
}

func externalWorkerRequest(ctx context.Context, config externalWorkerBootstrapConfig, credential, path string, body any, target any) error {
	endpoint := *config.controlPlaneURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + path
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("agnt5: encode external worker bootstrap request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("agnt5: build external worker bootstrap request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-API-KEY", credential)
	response, err := config.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("agnt5: external worker bootstrap request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		var problem struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(limited, &problem)
		if problem.Error == "" {
			problem.Error = "request_failed"
		}
		return fmt.Errorf("agnt5: external worker bootstrap returned %s: %s", response.Status, problem.Error)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("agnt5: decode external worker bootstrap response: %w", err)
	}
	return nil
}

func validateExternalWorkerConnection(connection externalWorkerConnection) error {
	if connection.ProjectID == "" || connection.EnvironmentID == "" || connection.DeploymentID == "" || connection.WorkerPoolID == "" {
		return errors.New("agnt5: worker discovery omitted immutable placement authority")
	}
	if connection.Placement != "customer_docker" && connection.Placement != "customer_kubernetes" {
		return fmt.Errorf("agnt5: worker discovery returned non-external placement %q", connection.Placement)
	}
	if connection.Protocol != externalWorkerProtocolPullV1 {
		return fmt.Errorf("agnt5: unsupported external worker protocol %q", connection.Protocol)
	}
	_, err := validateExternalEndpoint(connection.RuntimeEndpoint, "runtime")
	return err
}

func validateExternalEndpoint(raw, name string) (*url.URL, error) {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("agnt5: invalid %s endpoint: %w", name, err)
	}
	if endpoint.User != nil || endpoint.Host == "" {
		return nil, fmt.Errorf("agnt5: invalid %s endpoint: host is required and user info is forbidden", name)
	}
	switch endpoint.Scheme {
	case "https":
		return endpoint, nil
	case "http":
		if isLoopbackHost(endpoint.Hostname()) {
			return endpoint, nil
		}
		return nil, fmt.Errorf("agnt5: %s endpoint must use verified TLS outside local development", name)
	default:
		return nil, fmt.Errorf("agnt5: %s endpoint must use https", name)
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func envBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func legacyRoutingConfiguredFromEnv() bool {
	for _, name := range []string{envCoordinatorEndpoint, envEngineURL, envProjectID, envDeploymentID} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

func (w *Worker) configureExternalWorker(ctx context.Context) error {
	w.externalMu.Lock()
	defer w.externalMu.Unlock()
	if w.externalSession != nil {
		return nil
	}
	config, enabled, err := externalWorkerConfigFromEnv(w.legacyRoutingSet)
	if err != nil || !enabled {
		return err
	}
	session, err := connectExternalWorker(ctx, config)
	if err != nil {
		return err
	}
	w.coordinatorEndpoint = session.connection.RuntimeEndpoint
	w.engineEndpoint = session.connection.RuntimeEndpoint
	w.projectID = session.connection.ProjectID
	w.deploymentID = session.connection.DeploymentID
	w.workerMode = WorkerModePull
	w.grpcDialOptions = append(w.grpcDialOptions, grpc.WithPerRPCCredentials(session))
	w.externalSession = session
	w.syncRuntimeMetadata()
	fmt.Printf("AGNT5 external worker authenticated (environment=%s deployment=%s)\n", session.connection.EnvironmentID, session.connection.DeploymentID)
	return nil
}

// rediscoverExternalWorker refreshes placement before a reconnect attempt.
// The credential object already installed in gRPC dial options is updated in
// place so callers do not accumulate duplicate PerRPCCredentials options.
func (w *Worker) rediscoverExternalWorker(ctx context.Context) error {
	w.externalMu.Lock()
	defer w.externalMu.Unlock()
	current := w.externalSession
	if current == nil {
		return nil
	}

	refreshed, err := connectExternalWorker(ctx, current.config)
	if err != nil {
		return err
	}
	current.mu.Lock()
	current.config = refreshed.config
	current.connection = refreshed.connection
	current.token = refreshed.token
	current.expiresAt = refreshed.expiresAt
	current.mu.Unlock()

	w.coordinatorEndpoint = refreshed.connection.RuntimeEndpoint
	w.engineEndpoint = refreshed.connection.RuntimeEndpoint
	w.projectID = refreshed.connection.ProjectID
	w.deploymentID = refreshed.connection.DeploymentID
	w.syncRuntimeMetadata()
	return nil
}
