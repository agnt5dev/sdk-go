package agnt5

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGatewayURL = "https://gw.agnt5.com"
	envAPIKey         = "AGNT5_API_KEY"
	envGatewayURL     = "AGNT5_GATEWAY_URL"
	envTenantID       = "AGNT5_TENANT_ID"
)

// Client invokes deployed AGNT5 components through the runtime gateway.
type Client struct {
	gatewayURL   *url.URL
	apiKey       string
	tenantID     string
	deploymentID string
	httpClient   *http.Client
}

type clientConfig struct {
	apiKey       *string
	tenantID     *string
	deploymentID *string
	httpClient   *http.Client
	timeout      time.Duration
}

// ClientOption mutates Client configuration during construction.
type ClientOption func(*clientConfig)

// WithAPIKey sets the service key sent as X-API-KEY.
func WithAPIKey(apiKey string) ClientOption {
	return func(config *clientConfig) {
		config.apiKey = &apiKey
	}
}

// WithTenantID sets the default sub-tenant sent as X-TENANT-ID.
func WithTenantID(tenantID string) ClientOption {
	return func(config *clientConfig) {
		config.tenantID = &tenantID
	}
}

// WithClientDeploymentID sets the deployment routing key sent as X-DEPLOYMENT-ID.
func WithClientDeploymentID(deploymentID string) ClientOption {
	return func(config *clientConfig) {
		config.deploymentID = &deploymentID
	}
}

// WithHTTPClient sets the HTTP client used for gateway requests.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(config *clientConfig) {
		if httpClient != nil {
			config.httpClient = httpClient
		}
	}
}

// WithClientTimeout sets the default HTTP request timeout.
func WithClientTimeout(timeout time.Duration) ClientOption {
	return func(config *clientConfig) {
		if timeout > 0 {
			config.timeout = timeout
		}
	}
}

// NewClient constructs a gateway client. Empty gatewayURL falls back to
// AGNT5_GATEWAY_URL, then https://gw.agnt5.com.
func NewClient(gatewayURL string, opts ...ClientOption) (*Client, error) {
	config := clientConfig{timeout: 30 * time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	gatewayURL = strings.TrimSpace(gatewayURL)
	if gatewayURL == "" {
		gatewayURL = strings.TrimSpace(os.Getenv(envGatewayURL))
	}
	if gatewayURL == "" {
		gatewayURL = defaultGatewayURL
	}
	if !strings.Contains(gatewayURL, "://") {
		gatewayURL = "http://" + gatewayURL
	}
	parsed, err := url.Parse(strings.TrimRight(gatewayURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("agnt5: invalid gateway URL %q: %w", gatewayURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("agnt5: unsupported gateway URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("agnt5: invalid gateway URL %q: missing host", gatewayURL)
	}

	apiKey := os.Getenv(envAPIKey)
	if config.apiKey != nil {
		apiKey = *config.apiKey
	}
	if apiKey != "" && !strings.HasPrefix(apiKey, "agnt5_sk_") {
		return nil, fmt.Errorf("agnt5: invalid API key format: keys must start with agnt5_sk_")
	}
	tenantID := os.Getenv(envTenantID)
	if config.tenantID != nil {
		tenantID = *config.tenantID
	}
	deploymentID := os.Getenv(envDeploymentID)
	if config.deploymentID != nil {
		deploymentID = *config.deploymentID
	}

	httpClient := config.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: config.timeout}
	}

	return &Client{
		gatewayURL:   parsed,
		apiKey:       apiKey,
		tenantID:     tenantID,
		deploymentID: deploymentID,
		httpClient:   httpClient,
	}, nil
}

// RunStatus is the gateway-visible lifecycle status for a run.
type RunStatus string

const (
	RunStatusEnqueued          RunStatus = "enqueued"
	RunStatusQueued            RunStatus = "queued"
	RunStatusStarted           RunStatus = "started"
	RunStatusRunning           RunStatus = "running"
	RunStatusCompleted         RunStatus = "completed"
	RunStatusFailed            RunStatus = "failed"
	RunStatusCancelled         RunStatus = "cancelled"
	RunStatusPaused            RunStatus = "paused"
	RunStatusAwaitingInput     RunStatus = "awaiting_input"
	RunStatusAwaitingUserInput RunStatus = "awaiting_user_input"
	RunStatusTimeout           RunStatus = "timeout"
	RunStatusUnknown           RunStatus = "unknown"
)

// RunErrorDetail is structured error information returned by the gateway.
type RunErrorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// RunResponse is returned by Run, GetResult, and WaitForResult.
type RunResponse struct {
	RunID       string          `json:"run_id"`
	StatusCode  int             `json:"status_code"`
	Status      RunStatus       `json:"status"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       *RunErrorDetail `json:"error,omitempty"`
	DurationMS  *int64          `json:"duration_ms,omitempty"`
	TraceID     string          `json:"trace_id,omitempty"`
	Component   string          `json:"component,omitempty"`
	CreatedAt   *time.Time      `json:"created_at,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	FailedAt    *time.Time      `json:"failed_at,omitempty"`
	SessionID   string          `json:"session_id,omitempty"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
	Raw         map[string]any  `json:"-"`
}

// IsSuccess reports whether the run completed successfully.
func (r *RunResponse) IsSuccess() bool {
	return r != nil && r.StatusCode == http.StatusOK && r.Status == RunStatusCompleted
}

// IsPending reports whether the run is queued or still executing.
func (r *RunResponse) IsPending() bool {
	if r == nil {
		return false
	}
	return r.StatusCode == http.StatusAccepted || r.Status == RunStatusEnqueued ||
		r.Status == RunStatusQueued || r.Status == RunStatusStarted || r.Status == RunStatusRunning
}

// IsError reports whether the run reached an error terminal state.
func (r *RunResponse) IsError() bool {
	if r == nil {
		return false
	}
	return r.StatusCode >= http.StatusInternalServerError || r.Status == RunStatusFailed ||
		r.Status == RunStatusCancelled || r.Status == RunStatusTimeout
}

// DecodeOutput unmarshals the run output into target.
func (r *RunResponse) DecodeOutput(target any) error {
	if r == nil || len(r.Output) == 0 || string(r.Output) == "null" {
		return io.EOF
	}
	return json.Unmarshal(r.Output, target)
}

// RaiseForStatus returns a RunError when the response is an error state.
func (r *RunResponse) RaiseForStatus() error {
	if !r.IsError() {
		return nil
	}
	if r.Error != nil {
		return &RunError{
			Message:   r.Error.Message,
			RunID:     r.RunID,
			ErrorCode: r.Error.Code,
			Metadata:  r.Error.Details,
		}
	}
	return &RunError{Message: fmt.Sprintf("run failed with status %s", r.Status), RunID: r.RunID}
}

// SubmitLinks contains links returned for async submissions.
type SubmitLinks struct {
	SelfURL string `json:"self"`
}

// SubmitResponse is returned by Submit.
type SubmitResponse struct {
	RunID      string       `json:"run_id"`
	StatusCode int          `json:"status_code"`
	Status     RunStatus    `json:"status"`
	TraceID    string       `json:"trace_id,omitempty"`
	Component  string       `json:"component,omitempty"`
	CreatedAt  *time.Time   `json:"created_at,omitempty"`
	Links      *SubmitLinks `json:"links,omitempty"`
	Raw        map[string]any
}

// StatusURL returns the status link when the gateway supplied one.
func (r *SubmitResponse) StatusURL() string {
	if r == nil || r.Links == nil {
		return ""
	}
	return r.Links.SelfURL
}

// StatusResponse is returned by GetStatus.
type StatusResponse struct {
	RunID       string         `json:"run_id"`
	StatusCode  int            `json:"status_code"`
	Status      RunStatus      `json:"status"`
	TraceID     string         `json:"trace_id,omitempty"`
	Component   string         `json:"component,omitempty"`
	CreatedAt   *time.Time     `json:"created_at,omitempty"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Raw         map[string]any `json:"-"`
}

// IsComplete reports whether the run is in a terminal status.
func (r *StatusResponse) IsComplete() bool {
	if r == nil {
		return false
	}
	switch r.Status {
	case RunStatusCompleted, RunStatusFailed, RunStatusCancelled, RunStatusTimeout, RunStatusPaused:
		return true
	default:
		return false
	}
}

// IsRunning reports whether the run is actively executing.
func (r *StatusResponse) IsRunning() bool {
	return r != nil && (r.Status == RunStatusRunning || r.Status == RunStatusStarted)
}

// RunEvent is one journal event returned by GetEvents.
type RunEvent struct {
	ID                  string          `json:"id,omitempty"`
	EventType           string          `json:"event_type"`
	RunID               string          `json:"run_id"`
	Data                json.RawMessage `json:"data,omitempty"`
	InputData           json.RawMessage `json:"input_data,omitempty"`
	OutputData          json.RawMessage `json:"output_data,omitempty"`
	StepKey             string          `json:"step_key,omitempty"`
	ParentEventID       string          `json:"parent_event_id,omitempty"`
	CorrelationID       string          `json:"correlation_id,omitempty"`
	ParentCorrelationID string          `json:"parent_correlation_id,omitempty"`
	Metadata            map[string]any  `json:"metadata,omitempty"`
	TraceID             string          `json:"trace_id,omitempty"`
	TimestampNS         int64           `json:"timestamp_ns,omitempty"`
	CreatedAt           *time.Time      `json:"created_at,omitempty"`
	Raw                 map[string]any  `json:"-"`
}

// EventsResponse is returned by GetEvents.
type EventsResponse struct {
	Items []RunEvent     `json:"items"`
	Count int            `json:"count"`
	Raw   map[string]any `json:"-"`
}

// ReceivedEvent is one event decoded from a streaming SSE response.
type ReceivedEvent struct {
	EventType    string         `json:"event_type"`
	Data         map[string]any `json:"data"`
	ContentIndex int            `json:"content_index"`
	Sequence     int            `json:"sequence"`
	RunID        string         `json:"run_id,omitempty"`
}

// RunError is returned when a run or stream reaches an error state.
type RunError struct {
	Message     string
	RunID       string
	ErrorCode   string
	Attempts    int
	MaxAttempts int
	Metadata    map[string]any
}

func (e *RunError) Error() string {
	if e == nil {
		return ""
	}
	if e.RunID == "" {
		return e.Message
	}
	return fmt.Sprintf("%s (run_id=%s)", e.Message, e.RunID)
}

// WasRetried reports whether the run made more than one attempt.
func (e *RunError) WasRetried() bool {
	return e != nil && e.Attempts > 1
}

// ExhaustedRetries reports whether all configured attempts were used.
func (e *RunError) ExhaustedRetries() bool {
	return e != nil && e.Attempts > 0 && e.MaxAttempts > 0 && e.Attempts >= e.MaxAttempts
}

// ClientError describes a non-run HTTP failure from the gateway.
type ClientError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *ClientError) Error() string {
	if e == nil {
		return ""
	}
	if e.Body == "" {
		return fmt.Sprintf("agnt5: %s %s returned HTTP %d", e.Method, e.URL, e.StatusCode)
	}
	return fmt.Sprintf("agnt5: %s %s returned HTTP %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

type runConfig struct {
	componentType ComponentType
	sessionID     string
	userID        string
	tenant        string
	timeout       time.Duration
	headers       map[string]string
}

// RunOption mutates Run and streaming request configuration.
type RunOption func(*runConfig)

// WithRunComponentType sets the component kind for Run or StreamEvents.
func WithRunComponentType(componentType ComponentType) RunOption {
	return func(config *runConfig) {
		if componentType != "" {
			config.componentType = componentType
		}
	}
}

// WithRunSessionID sets X-Session-ID.
func WithRunSessionID(sessionID string) RunOption {
	return func(config *runConfig) {
		config.sessionID = sessionID
	}
}

// WithRunUserID sets X-User-ID.
func WithRunUserID(userID string) RunOption {
	return func(config *runConfig) {
		config.userID = userID
	}
}

// WithRunTenant overrides the default X-TENANT-ID for this request.
func WithRunTenant(tenantID string) RunOption {
	return func(config *runConfig) {
		config.tenant = tenantID
	}
}

// WithRunTimeout sets a per-request context timeout.
func WithRunTimeout(timeout time.Duration) RunOption {
	return func(config *runConfig) {
		if timeout > 0 {
			config.timeout = timeout
		}
	}
}

// WithRunHeader sets one additional HTTP header for this request.
func WithRunHeader(key, value string) RunOption {
	return func(config *runConfig) {
		if strings.TrimSpace(key) == "" {
			return
		}
		if config.headers == nil {
			config.headers = make(map[string]string)
		}
		config.headers[key] = value
	}
}

// WithRunHeaders adds HTTP headers for this request.
func WithRunHeaders(headers map[string]string) RunOption {
	return func(config *runConfig) {
		if config.headers == nil {
			config.headers = make(map[string]string, len(headers))
		}
		for key, value := range headers {
			if strings.TrimSpace(key) != "" {
				config.headers[key] = value
			}
		}
	}
}

type submitConfig struct {
	componentType ComponentType
	metadata      map[string]string
	tenant        string
}

// SubmitOption mutates Submit request configuration.
type SubmitOption func(*submitConfig)

// WithSubmitComponentType sets the component kind for Submit.
func WithSubmitComponentType(componentType ComponentType) SubmitOption {
	return func(config *submitConfig) {
		if componentType != "" {
			config.componentType = componentType
		}
	}
}

// WithSubmitMetadata passes metadata through the async job queue.
func WithSubmitMetadata(metadata map[string]string) SubmitOption {
	return func(config *submitConfig) {
		if config.metadata == nil {
			config.metadata = make(map[string]string, len(metadata))
		}
		for key, value := range metadata {
			config.metadata[key] = value
		}
	}
}

// WithSubmitTenant overrides the default X-TENANT-ID for this request.
func WithSubmitTenant(tenantID string) SubmitOption {
	return func(config *submitConfig) {
		config.tenant = tenantID
	}
}

// Run executes a component synchronously through /v1/{type}/{component}/run.
func (c *Client) Run(ctx context.Context, component string, input any, opts ...RunOption) (*RunResponse, error) {
	config := newRunConfig(opts...)
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{
		"v1", componentCollection(config.componentType), component, "run",
	}, inputOrEmptyObject(input), c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers), config.timeout)
	if err != nil {
		return nil, err
	}
	switch statusCode {
	case http.StatusNotFound:
		return syntheticRunResponse(body, statusCode, RunStatusFailed, "NOT_FOUND", fmt.Sprintf("Component %q not found", component)), nil
	case http.StatusServiceUnavailable:
		return syntheticRunResponse(body, statusCode, RunStatusFailed, "SERVICE_UNAVAILABLE", "Service unavailable"), nil
	case http.StatusGatewayTimeout:
		return syntheticRunResponse(body, statusCode, RunStatusTimeout, "TIMEOUT", "Execution timeout"), nil
	}
	if statusCode >= http.StatusBadRequest {
		response, parseErr := parseRunResponse(body, statusCode)
		if parseErr == nil {
			return response, nil
		}
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	return parseRunResponse(body, statusCode)
}

// Submit enqueues a component asynchronously through /v1/{type}/{component}/submit.
func (c *Client) Submit(ctx context.Context, component string, input any, opts ...SubmitOption) (*SubmitResponse, error) {
	config := newSubmitConfig(opts...)
	bodyValue := inputOrEmptyObject(input)
	if len(config.metadata) > 0 {
		bodyValue = map[string]any{
			"input":    bodyValue,
			"metadata": config.metadata,
		}
	}
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{
		"v1", componentCollection(config.componentType), component, "submit",
	}, bodyValue, c.requestHeaders("", "", config.tenant, nil), 0)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	return parseSubmitResponse(body, statusCode)
}

// GetStatus returns the current status for a run.
func (c *Client) GetStatus(ctx context.Context, runID string) (*StatusResponse, error) {
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodGet, []string{"v1", "status", runID}, nil, c.requestHeaders("", "", "", nil), 0)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodGet, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	return parseStatusResponse(body, statusCode)
}

// GetResult returns the terminal result for a run.
func (c *Client) GetResult(ctx context.Context, runID string) (*RunResponse, error) {
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodGet, []string{"v1", "result", runID}, nil, c.requestHeaders("", "", "", nil), 0)
	if err != nil {
		return nil, err
	}
	if statusCode == http.StatusNotFound {
		return syntheticRunResponse(body, statusCode, RunStatusUnknown, "NOT_READY", "Run not found or not complete"), nil
	}
	if statusCode >= http.StatusBadRequest {
		response, parseErr := parseRunResponse(body, statusCode)
		if parseErr == nil {
			return response, nil
		}
		return nil, &ClientError{Method: http.MethodGet, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	return parseRunResponse(body, statusCode)
}

// WaitForResult polls status until a run reaches a terminal status or timeout expires.
func (c *Client) WaitForResult(ctx context.Context, runID string, timeout, pollInterval time.Duration) (*RunResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		status, err := c.GetStatus(ctx, runID)
		if err != nil {
			return nil, err
		}
		if status.IsComplete() {
			return c.GetResult(ctx, runID)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return timeoutRunResponse(runID, timeout), nil
		case <-ticker.C:
		}
	}
}

// GetEvents returns all journal events for a run as JSON.
func (c *Client) GetEvents(ctx context.Context, runID string) (*EventsResponse, error) {
	headers := c.requestHeaders("", "", "", nil)
	headers.Set("Accept", "application/json")
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodGet, []string{"v1", "runs", runID, "events"}, nil, headers, 0)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodGet, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	return parseEventsResponse(body)
}

// Stream consumes output.delta chunks from a streaming component run.
func (c *Client) Stream(ctx context.Context, component string, input any, handle func(string) error, opts ...RunOption) error {
	if handle == nil {
		return errors.New("agnt5: nil stream handler")
	}
	return c.StreamEvents(ctx, component, input, func(event ReceivedEvent) error {
		if event.EventType == "run.failed" {
			return parseRunErrorMap(event.Data, event.RunID)
		}
		if event.EventType == "output.delta" {
			if value, ok := event.Data["delta"].(string); ok {
				return handle(value)
			}
			if value, ok := event.Data["content"].(string); ok {
				return handle(value)
			}
			if value, ok := event.Data["output_data"]; ok && value != nil {
				if text, ok := value.(string); ok {
					return handle(text)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					return err
				}
				return handle(string(encoded))
			}
		}
		if chunk, ok := event.Data["chunk"].(string); ok {
			return handle(chunk)
		}
		return nil
	}, opts...)
}

// StreamEvents consumes typed SSE events from /v1/{type}/{component}/stream.
func (c *Client) StreamEvents(ctx context.Context, component string, input any, handle func(ReceivedEvent) error, opts ...RunOption) error {
	if handle == nil {
		return errors.New("agnt5: nil stream event handler")
	}
	config := newRunConfig(opts...)
	headers := c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers)
	headers.Set("Accept", "text/event-stream")

	statusCode, body, err := c.doStream(ctx, []string{
		"v1", componentCollection(config.componentType), component, "stream",
	}, inputOrEmptyObject(input), headers, config.timeout, func(event ReceivedEvent) error {
		payload := gatewayEventPayload(event.Data)
		event.Data = payload
		event.ContentIndex = fieldInt(payload, "index", "content_index", "contentIndex")
		if payloadSequence := fieldInt(payload, "sequence"); payloadSequence != 0 {
			event.Sequence = payloadSequence
		}
		return handle(event)
	})
	if err != nil {
		return err
	}
	if statusCode >= http.StatusBadRequest {
		runErr := parseRunErrorMap(decodeJSONMapOrEmpty(body), "")
		if runErr.Message != "" {
			return runErr
		}
		return &ClientError{Method: http.MethodPost, URL: c.endpoint("v1", componentCollection(config.componentType), component, "stream"), StatusCode: statusCode, Body: string(body)}
	}
	return nil
}

func newRunConfig(opts ...RunOption) runConfig {
	config := runConfig{componentType: ComponentTypeFunction}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return config
}

func newSubmitConfig(opts ...SubmitOption) submitConfig {
	config := submitConfig{componentType: ComponentTypeFunction}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return config
}

func (c *Client) doJSON(ctx context.Context, method string, path []string, bodyValue any, headers http.Header, timeout time.Duration) (int, []byte, string, error) {
	return c.doJSONEndpoint(ctx, method, c.endpoint(path...), bodyValue, headers, timeout)
}

func (c *Client) doJSONEndpoint(ctx context.Context, method, endpoint string, bodyValue any, headers http.Header, timeout time.Duration) (int, []byte, string, error) {
	if c == nil {
		return 0, nil, "", errors.New("agnt5: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var body io.Reader
	if bodyValue != nil {
		encoded, err := json.Marshal(bodyValue)
		if err != nil {
			return 0, nil, "", fmt.Errorf("agnt5: encode request body: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return 0, nil, endpoint, err
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if bodyValue != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, endpoint, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, endpoint, err
	}
	return resp.StatusCode, responseBody, endpoint, nil
}

func (c *Client) doStream(ctx context.Context, path []string, bodyValue any, headers http.Header, timeout time.Duration, handle func(ReceivedEvent) error) (int, []byte, error) {
	if c == nil {
		return 0, nil, errors.New("agnt5: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	encoded, err := json.Marshal(bodyValue)
	if err != nil {
		return 0, nil, fmt.Errorf("agnt5: encode request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(path...), bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return resp.StatusCode, nil, readErr
		}
		return resp.StatusCode, body, nil
	}
	return resp.StatusCode, nil, parseSSE(resp.Body, handle)
}

func (c *Client) endpoint(path ...string) string {
	joined := c.gatewayURL.JoinPath(path...)
	return joined.String()
}

func (c *Client) requestHeaders(sessionID, userID, tenantOverride string, extra map[string]string) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		headers.Set("X-API-KEY", c.apiKey)
	}
	tenantID := c.tenantID
	if tenantOverride != "" {
		tenantID = tenantOverride
	}
	if tenantID != "" {
		headers.Set("X-TENANT-ID", tenantID)
	}
	if c.deploymentID != "" {
		headers.Set("X-DEPLOYMENT-ID", c.deploymentID)
	}
	if sessionID != "" {
		headers.Set("X-Session-ID", sessionID)
	}
	if userID != "" {
		headers.Set("X-User-ID", userID)
	}
	for key, value := range extra {
		headers.Set(key, value)
	}
	return headers
}

func componentCollection(componentType ComponentType) string {
	switch componentType {
	case "", ComponentTypeFunction:
		return "functions"
	case ComponentTypeWorkflow:
		return "workflows"
	case ComponentTypeAgent:
		return "agents"
	case ComponentTypeTool:
		return "tools"
	case ComponentTypeScorer:
		return "scorers"
	case ComponentTypeMCP:
		return "mcp"
	case ComponentTypeEntity:
		return "entity"
	case ComponentTypeChat:
		return "chat"
	default:
		value := strings.TrimSpace(string(componentType))
		if strings.HasSuffix(value, "s") {
			return value
		}
		return value + "s"
	}
}

func inputOrEmptyObject(input any) any {
	if input == nil {
		return map[string]any{}
	}
	return input
}

func parseRunResponse(body []byte, fallbackStatusCode int) (*RunResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		value, valueErr := decodeJSONValue(body)
		if valueErr != nil {
			return nil, err
		}
		return &RunResponse{
			StatusCode: statusCodeFor(RunStatusCompleted, fallbackStatusCode),
			Status:     RunStatusCompleted,
			Output:     rawJSONValue(value),
		}, nil
	}
	wrapped := hasAny(data, "run_id", "runId", "status", "status_code", "error", "error_code", "output", "result")
	if !wrapped {
		return &RunResponse{
			StatusCode: statusCodeFor(RunStatusCompleted, fallbackStatusCode),
			Status:     RunStatusCompleted,
			Output:     rawJSONValue(data),
			Raw:        data,
		}, nil
	}
	status := parseRunStatus(fieldString(data, "status"))
	statusCode := fieldInt(data, "status_code", "statusCode")
	if statusCode == 0 {
		statusCode = statusCodeFor(status, fallbackStatusCode)
	}
	outputValue, ok := data["output"]
	if (!ok || outputValue == nil) && data["result"] != nil {
		if result, ok := data["result"].(map[string]any); ok {
			if output, ok := result["output"].(map[string]any); ok {
				if value, ok := output["output_data"]; ok {
					outputValue = value
				} else {
					outputValue = output
				}
			}
		}
	}
	duration := fieldInt64Ptr(data, "duration_ms", "durationMs")
	return &RunResponse{
		RunID:       firstString(data, "run_id", "runId"),
		StatusCode:  statusCode,
		Status:      status,
		Output:      rawJSONValue(outputValue),
		Error:       parseRunErrorDetail(data),
		DurationMS:  duration,
		TraceID:     firstString(data, "trace_id", "traceId"),
		Component:   firstString(data, "function", "component", "component_name", "componentName"),
		CreatedAt:   fieldTime(data, "created_at", "createdAt"),
		StartedAt:   fieldTime(data, "started_at", "startedAt"),
		CompletedAt: fieldTime(data, "completed_at", "completedAt"),
		FailedAt:    fieldTime(data, "failed_at", "failedAt"),
		SessionID:   firstString(data, "session_id", "sessionId"),
		Metadata:    fieldMap(data, "metadata"),
		Raw:         data,
	}, nil
}

func parseSubmitResponse(body []byte, fallbackStatusCode int) (*SubmitResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	status := parseRunStatus(fieldString(data, "status"))
	if status == RunStatusUnknown {
		status = RunStatusEnqueued
	}
	statusCode := fieldInt(data, "status_code", "statusCode")
	if statusCode == 0 {
		if fallbackStatusCode != 0 {
			statusCode = fallbackStatusCode
		} else {
			statusCode = http.StatusAccepted
		}
	}
	var links *SubmitLinks
	if rawLinks := fieldMap(data, "links"); rawLinks != nil {
		links = &SubmitLinks{SelfURL: firstString(rawLinks, "self", "self_url", "selfUrl")}
	}
	return &SubmitResponse{
		RunID:      firstString(data, "run_id", "runId"),
		StatusCode: statusCode,
		Status:     status,
		TraceID:    firstString(data, "trace_id", "traceId"),
		Component:  firstString(data, "function", "component", "component_name", "componentName"),
		CreatedAt:  fieldTime(data, "created_at", "createdAt"),
		Links:      links,
		Raw:        data,
	}, nil
}

func parseStatusResponse(body []byte, fallbackStatusCode int) (*StatusResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	status := parseRunStatus(fieldString(data, "status"))
	statusCode := fieldInt(data, "status_code", "statusCode")
	if statusCode == 0 {
		statusCode = statusCodeFor(status, fallbackStatusCode)
	}
	return &StatusResponse{
		RunID:       firstString(data, "run_id", "runId"),
		StatusCode:  statusCode,
		Status:      status,
		TraceID:     firstString(data, "trace_id", "traceId"),
		Component:   firstString(data, "function", "component", "component_name", "componentName"),
		CreatedAt:   fieldTime(data, "created_at", "createdAt", "submittedAt"),
		StartedAt:   fieldTime(data, "started_at", "startedAt"),
		CompletedAt: fieldTime(data, "completed_at", "completedAt"),
		Metadata:    fieldMap(data, "metadata"),
		Raw:         data,
	}, nil
}

func parseEventsResponse(body []byte) (*EventsResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	rawItems, _ := data["items"].([]any)
	if rawItems == nil {
		rawItems, _ = data["events"].([]any)
	}
	items := make([]RunEvent, 0, len(rawItems))
	for _, item := range rawItems {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}
		items = append(items, parseEventMap(itemMap))
	}
	count := fieldInt(data, "count")
	if count == 0 {
		count = len(items)
	}
	return &EventsResponse{Items: items, Count: count, Raw: data}, nil
}

func parseEventMap(data map[string]any) RunEvent {
	return RunEvent{
		ID:                  firstString(data, "id"),
		EventType:           firstString(data, "event_type", "eventType"),
		RunID:               firstString(data, "run_id", "runId"),
		Data:                rawJSONValue(data["data"]),
		InputData:           rawJSONValue(data["input_data"]),
		OutputData:          rawJSONValue(data["output_data"]),
		StepKey:             firstString(data, "step_key", "stepKey"),
		ParentEventID:       firstString(data, "parent_event_id", "parentEventId"),
		CorrelationID:       firstString(data, "correlation_id", "correlationId"),
		ParentCorrelationID: firstString(data, "parent_correlation_id", "parentCorrelationId"),
		Metadata:            fieldMap(data, "metadata"),
		TraceID:             firstString(data, "trace_id", "traceId"),
		TimestampNS:         fieldInt64(data, "timestamp_ns", "timestampNs"),
		CreatedAt:           fieldTime(data, "created_at", "createdAt"),
		Raw:                 data,
	}
}

func syntheticRunResponse(body []byte, httpStatus int, status RunStatus, code, message string) *RunResponse {
	data := decodeJSONMapOrEmpty(body)
	runID := firstString(data, "run_id", "runId")
	if statusValue := fieldString(data, "status"); statusValue != "" {
		status = parseRunStatus(statusValue)
	}
	if rawMessage := fieldString(data, "error"); rawMessage != "" {
		message = rawMessage
	}
	if detail := parseRunErrorDetail(data); detail != nil {
		code = firstNonEmpty(detail.Code, code)
		message = firstNonEmpty(detail.Message, message)
	}
	if rawCode := fieldString(data, "error_code", "errorCode"); rawCode != "" {
		code = rawCode
	}
	return &RunResponse{
		RunID:      runID,
		StatusCode: statusCodeFor(status, httpStatus),
		Status:     status,
		Error: &RunErrorDetail{
			Code:    code,
			Message: message,
			Details: fieldMap(data, "metadata"),
		},
		Raw: data,
	}
}

func timeoutRunResponse(runID string, timeout time.Duration) *RunResponse {
	return &RunResponse{
		RunID:      runID,
		StatusCode: http.StatusInternalServerError,
		Status:     RunStatusTimeout,
		Error: &RunErrorDetail{
			Code:    "TIMEOUT",
			Message: fmt.Sprintf("Timeout waiting for run to complete after %s", timeout),
		},
	}
}

func parseSSE(reader io.Reader, handle func(ReceivedEvent) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	currentEvent := ""
	dataLines := make([]string, 0, 1)
	sequence := 0

	dispatch := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		dataText := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		data := make(map[string]any)
		if err := json.Unmarshal([]byte(dataText), &data); err != nil {
			if currentEvent == "error" {
				return &RunError{Message: dataText}
			}
			return nil
		}
		if boolField(data, "done") || currentEvent == "done" {
			return errSSEDone
		}
		if currentEvent == "error" || data["error"] != nil {
			return parseRunErrorMap(data, firstString(data, "run_id", "runId"))
		}
		sequence++
		eventSequence := fieldInt(data, "sequence")
		if eventSequence == 0 {
			eventSequence = sequence
		}
		event := ReceivedEvent{
			EventType:    currentEvent,
			Data:         data,
			ContentIndex: fieldInt(data, "index", "content_index", "contentIndex"),
			Sequence:     eventSequence,
			RunID:        firstString(data, "run_id", "runId"),
		}
		return handle(event)
	}

	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			err := dispatch()
			currentEvent = ""
			if errors.Is(err, errSSEDone) {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			currentEvent = value
		case "data":
			dataLines = append(dataLines, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	err := dispatch()
	if errors.Is(err, errSSEDone) {
		return nil
	}
	return err
}

func gatewayEventPayload(data map[string]any) map[string]any {
	nested, ok := data["data"].(map[string]any)
	if !ok {
		return data
	}
	if firstString(data, "event_type", "eventType", "run_id", "runId") == "" {
		return data
	}
	return nested
}

var errSSEDone = errors.New("agnt5: sse done")

func parseRunErrorMap(data map[string]any, runID string) *RunError {
	errorValue := data["error"]
	message := "Unknown error"
	code := fieldString(data, "error_code", "errorCode")
	metadata := fieldMap(data, "metadata")
	if errorMap, ok := errorValue.(map[string]any); ok {
		if value := fieldString(errorMap, "message"); value != "" {
			message = value
		}
		if value := fieldString(errorMap, "code"); value != "" {
			code = value
		}
		if metadata == nil {
			metadata = fieldMap(errorMap, "details")
		}
	} else if value := fieldString(data, "error"); value != "" {
		message = value
	} else if value := fieldString(data, "error_message", "errorMessage"); value != "" {
		message = value
	}
	if runID == "" {
		runID = firstString(data, "run_id", "runId")
	}
	return &RunError{
		Message:     message,
		RunID:       runID,
		ErrorCode:   code,
		Attempts:    fieldInt(metadata, "attempts"),
		MaxAttempts: fieldInt(metadata, "max_attempts", "maxAttempts"),
		Metadata:    metadata,
	}
}

func parseRunErrorDetail(data map[string]any) *RunErrorDetail {
	errorValue := data["error"]
	if errorValue == nil {
		return nil
	}
	if errorMap, ok := errorValue.(map[string]any); ok {
		return &RunErrorDetail{
			Code:    firstNonEmpty(firstString(errorMap, "code"), firstString(data, "error_code", "errorCode"), "UNKNOWN"),
			Message: firstNonEmpty(firstString(errorMap, "message"), "Unknown error"),
			Details: fieldMap(errorMap, "details"),
		}
	}
	return &RunErrorDetail{
		Code:    firstNonEmpty(firstString(data, "error_code", "errorCode"), "UNKNOWN"),
		Message: fmt.Sprint(errorValue),
	}
}

func decodeJSONMap(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var data map[string]any
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}
	if data == nil {
		data = make(map[string]any)
	}
	return data, nil
}

func decodeJSONValue(body []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeJSONMapOrEmpty(body []byte) map[string]any {
	data, err := decodeJSONMap(body)
	if err != nil {
		return map[string]any{}
	}
	return data
}

func hasAny(data map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := data[key]; ok {
			return true
		}
	}
	return false
}

func rawJSONValue(value any) json.RawMessage {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}

func parseRunStatus(value string) RunStatus {
	switch RunStatus(value) {
	case RunStatusEnqueued, RunStatusQueued, RunStatusStarted, RunStatusRunning,
		RunStatusCompleted, RunStatusFailed, RunStatusCancelled, RunStatusPaused,
		RunStatusAwaitingInput, RunStatusAwaitingUserInput, RunStatusTimeout:
		return RunStatus(value)
	default:
		return RunStatusUnknown
	}
}

func statusCodeFor(status RunStatus, fallback int) int {
	switch status {
	case RunStatusCompleted:
		return http.StatusOK
	case RunStatusEnqueued, RunStatusQueued, RunStatusStarted, RunStatusRunning,
		RunStatusAwaitingInput, RunStatusAwaitingUserInput, RunStatusPaused:
		return http.StatusAccepted
	case RunStatusFailed, RunStatusCancelled, RunStatusTimeout:
		return http.StatusInternalServerError
	default:
		if fallback >= http.StatusBadRequest {
			return fallback
		}
		return http.StatusAccepted
	}
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := fieldString(data, key); value != "" {
			return value
		}
	}
	return ""
}

func fieldString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := data[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			return typed
		case json.Number:
			return typed.String()
		default:
			return fmt.Sprint(typed)
		}
	}
	return ""
}

func fieldInt(data map[string]any, keys ...string) int {
	return int(fieldInt64(data, keys...))
}

func fieldInt64(data map[string]any, keys ...string) int64 {
	for _, key := range keys {
		value, ok := data[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case int:
			return int64(typed)
		case int64:
			return typed
		case float64:
			return int64(typed)
		case json.Number:
			parsed, err := typed.Int64()
			if err == nil {
				return parsed
			}
			floatValue, err := typed.Float64()
			if err == nil {
				return int64(floatValue)
			}
		case string:
			parsed, err := strconv.ParseInt(typed, 10, 64)
			if err == nil {
				return parsed
			}
		}
	}
	return 0
}

func fieldInt64Ptr(data map[string]any, keys ...string) *int64 {
	for _, key := range keys {
		if _, ok := data[key]; ok {
			value := fieldInt64(data, key)
			return &value
		}
	}
	return nil
}

func boolField(data map[string]any, keys ...string) bool {
	for _, key := range keys {
		value, ok := data[key]
		if !ok {
			continue
		}
		if typed, ok := value.(bool); ok {
			return typed
		}
	}
	return false
}

func fieldMap(data map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		value, ok := data[key]
		if !ok || value == nil {
			continue
		}
		if typed, ok := value.(map[string]any); ok {
			return typed
		}
		if typed, ok := value.(map[string]string); ok {
			out := make(map[string]any, len(typed))
			for mapKey, mapValue := range typed {
				out[mapKey] = mapValue
			}
			return out
		}
		if typed, ok := value.(string); ok {
			var parsed map[string]any
			if err := json.Unmarshal([]byte(typed), &parsed); err == nil {
				return parsed
			}
		}
	}
	return nil
}

func fieldTime(data map[string]any, keys ...string) *time.Time {
	for _, key := range keys {
		value, ok := data[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(typed, "Z", "+00:00", 1))
			if err == nil {
				return &parsed
			}
		case json.Number:
			if parsed, err := typed.Int64(); err == nil {
				return unixTimeByMagnitude(parsed)
			}
		case float64:
			return unixTimeByMagnitude(int64(typed))
		}
	}
	return nil
}

func unixTimeByMagnitude(value int64) *time.Time {
	var parsed time.Time
	switch {
	case value > 1_000_000_000_000_000:
		parsed = time.Unix(0, value)
	case value > 1_000_000_000_000:
		parsed = time.UnixMilli(value)
	default:
		parsed = time.Unix(value, 0)
	}
	return &parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
