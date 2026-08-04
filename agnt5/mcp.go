package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TransportType identifies an MCP transport.
type TransportType string

const (
	TransportTypeStdio TransportType = "stdio"
	TransportTypeSSE   TransportType = "sse"
)

// ServerConfig describes an MCP server connection.
type ServerConfig struct {
	Name      string            `json:"name,omitempty"`
	Transport TransportType     `json:"transport"`
	Command   string            `json:"command,omitempty"`
	Args      []string          `json:"args,omitempty"`
	URL       string            `json:"url,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// MCPTool describes an MCP tool.
type MCPTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

// MCPResource describes an MCP resource.
type MCPResource struct {
	URI         string `json:"uri"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// MCPCallToolResult is returned from tools/call.
type MCPCallToolResult struct {
	Content []map[string]any `json:"content,omitempty"`
	IsError bool             `json:"isError,omitempty"`
	Raw     map[string]any   `json:"-"`
}

// MCPTransport is the JSON-RPC boundary for MCP.
type MCPTransport interface {
	Request(ctx context.Context, method string, params any) (map[string]any, error)
	Close() error
}

// MCPNotificationTransport is optionally implemented by transports that can
// send JSON-RPC notifications. MCPClient uses it for the initialize lifecycle.
type MCPNotificationTransport interface {
	Notify(ctx context.Context, method string, params any) error
}

// MCPRequestError is a JSON-RPC error returned by an MCP server.
type MCPRequestError struct {
	Code    int
	Message string
	Data    any
}

func (e *MCPRequestError) Error() string {
	if e == nil {
		return "agnt5: MCP request failed"
	}
	if e.Code == 0 {
		return "agnt5: MCP error: " + e.Message
	}
	return fmt.Sprintf("agnt5: MCP error %d: %s", e.Code, e.Message)
}

type mcpInitializeAttempt struct {
	done chan struct{}
	err  error
}

// MCPClient is a small MCP JSON-RPC client.
type MCPClient struct {
	transport MCPTransport

	mu           sync.Mutex
	initialized  bool
	initializing *mcpInitializeAttempt
	closed       bool
	closeOnce    sync.Once
	closeErr     error
}

func NewMCPClient(transport MCPTransport) (*MCPClient, error) {
	if transport == nil {
		return nil, errors.New("agnt5: nil MCP transport")
	}
	return &MCPClient{transport: transport}, nil
}

// Initialize performs the MCP initialize/initialized handshake. It is
// idempotent and safe for concurrent callers. Legacy transports that do not
// implement MCPNotificationTransport retain their request-only behavior.
func (c *MCPClient) Initialize(ctx context.Context) error {
	if c == nil || c.transport == nil {
		return ErrMCPTransportClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	notifier, supportsNotifications := c.transport.(MCPNotificationTransport)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrMCPTransportClosed
	}
	if !supportsNotifications {
		c.mu.Unlock()
		return nil
	}
	if c.initialized {
		c.mu.Unlock()
		return nil
	}
	if attempt := c.initializing; attempt != nil {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &mcpInitializeAttempt{done: make(chan struct{})}
	c.initializing = attempt
	c.mu.Unlock()

	initializeResult, err := c.transport.Request(ctx, "initialize", map[string]any{
		"protocolVersion": "2025-11-25",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "agnt5-sdk",
			"version": defaultServiceVersion,
		},
	})
	if err == nil {
		protocolVersion, _ := initializeResult["protocolVersion"].(string)
		if strings.TrimSpace(protocolVersion) == "" {
			err = errors.New("agnt5: MCP initialize response is missing protocolVersion")
		}
	}
	if err == nil {
		err = notifier.Notify(ctx, "notifications/initialized", nil)
	}

	c.mu.Lock()
	if err == nil && c.closed {
		err = ErrMCPTransportClosed
	}
	if err == nil {
		c.initialized = true
	}
	attempt.err = err
	c.initializing = nil
	close(attempt.done)
	c.mu.Unlock()
	return err
}

func (c *MCPClient) ListTools(ctx context.Context) ([]MCPTool, error) {
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	resp, err := c.transport.Request(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(resp["tools"])
	var tools []MCPTool
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, err
	}
	return tools, nil
}

func (c *MCPClient) CallTool(ctx context.Context, name string, arguments map[string]any) (MCPCallToolResult, error) {
	if err := c.Initialize(ctx); err != nil {
		return MCPCallToolResult{}, err
	}
	resp, err := c.transport.Request(ctx, "tools/call", map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		return MCPCallToolResult{}, err
	}
	raw, _ := json.Marshal(resp)
	var result MCPCallToolResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return MCPCallToolResult{}, err
	}
	result.Raw = resp
	return result, nil
}

func (c *MCPClient) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.closeErr = c.transport.Close()
	})
	return c.closeErr
}

type mcpResponseEnvelope struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type mcpResponseResult struct {
	result map[string]any
	err    error
}

// mcpStreamSession owns exactly one decoder goroutine for a bidirectional MCP
// connection and correlates responses to concurrent callers by JSON-RPC ID.
type mcpStreamSession struct {
	writer  io.Writer
	decoder *json.Decoder
	closeFn func() error

	writeMu sync.Mutex
	stateMu sync.Mutex
	nextID  uint64
	pending map[string]chan mcpResponseResult
	done    chan struct{}

	shutdownOnce sync.Once
	terminalErr  error
	closeErr     error
}

func newMCPStreamSession(reader io.Reader, writer io.Writer, closeFn func() error) *mcpStreamSession {
	session := &mcpStreamSession{
		writer:  writer,
		decoder: json.NewDecoder(reader),
		closeFn: closeFn,
		pending: make(map[string]chan mcpResponseResult),
		done:    make(chan struct{}),
	}
	go session.readLoop()
	return session
}

func (s *mcpStreamSession) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if s == nil || s.writer == nil || s.decoder == nil {
		return nil, errors.New("agnt5: nil MCP stream")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	id, key, responseCh, err := s.reserveRequest()
	if err != nil {
		return nil, err
	}
	request := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	encoded, err := json.Marshal(request)
	if err != nil {
		s.removePending(key, responseCh)
		return nil, err
	}
	if err := s.writeMessage(encoded); err != nil {
		s.removePending(key, responseCh)
		terminalErr := fmt.Errorf("%w: write request: %w", ErrMCPTransportClosed, err)
		s.shutdown(terminalErr)
		return nil, terminalErr
	}

	select {
	case <-ctx.Done():
		s.removePending(key, responseCh)
		return nil, ctx.Err()
	case response := <-responseCh:
		return response.result, response.err
	}
}

func (s *mcpStreamSession) Notify(ctx context.Context, method string, params any) error {
	if s == nil || s.writer == nil || s.decoder == nil {
		return errors.New("agnt5: nil MCP stream")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.connectionError(); err != nil {
		return err
	}
	request := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		request["params"] = params
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if err := s.writeMessage(encoded); err != nil {
		terminalErr := fmt.Errorf("%w: write notification: %w", ErrMCPTransportClosed, err)
		s.shutdown(terminalErr)
		return terminalErr
	}
	return nil
}

func (s *mcpStreamSession) reserveRequest() (uint64, string, chan mcpResponseResult, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.terminalErr != nil {
		return 0, "", nil, s.terminalErr
	}
	s.nextID++
	id := s.nextID
	key := strconv.FormatUint(id, 10)
	responseCh := make(chan mcpResponseResult, 1)
	s.pending[key] = responseCh
	return id, key, responseCh, nil
}

func (s *mcpStreamSession) removePending(key string, responseCh chan mcpResponseResult) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if current, ok := s.pending[key]; ok && current == responseCh {
		delete(s.pending, key)
	}
}

func (s *mcpStreamSession) writeMessage(encoded []byte) error {
	encoded = append(encoded, '\n')
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.connectionError(); err != nil {
		return err
	}
	for len(encoded) > 0 {
		written, err := s.writer.Write(encoded)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

func (s *mcpStreamSession) readLoop() {
	for {
		var envelope mcpResponseEnvelope
		if err := s.decoder.Decode(&envelope); err != nil {
			if errors.Is(err, io.EOF) {
				s.shutdown(ErrMCPTransportClosed)
			} else {
				s.shutdown(fmt.Errorf("%w: decode response: %w", ErrMCPTransportClosed, err))
			}
			return
		}
		// Notifications and server-initiated requests do not satisfy a pending
		// client request. Client capabilities currently advertise neither roots
		// nor sampling, so server requests are intentionally ignored.
		if envelope.Method != "" || len(bytes.TrimSpace(envelope.ID)) == 0 || bytes.Equal(bytes.TrimSpace(envelope.ID), []byte("null")) {
			continue
		}
		key := string(bytes.TrimSpace(envelope.ID))
		s.stateMu.Lock()
		responseCh := s.pending[key]
		if responseCh != nil {
			delete(s.pending, key)
		}
		s.stateMu.Unlock()
		if responseCh == nil {
			// A late response for a canceled request or an unknown ID is safe to
			// discard; the single reader remains available for later responses.
			continue
		}
		result, err := decodeMCPResponse(envelope)
		responseCh <- mcpResponseResult{result: result, err: err}
	}
}

func decodeMCPResponse(envelope mcpResponseEnvelope) (map[string]any, error) {
	errorPayload := bytes.TrimSpace(envelope.Error)
	if len(errorPayload) > 0 && !bytes.Equal(errorPayload, []byte("null")) {
		var rpcError struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(errorPayload, &rpcError); err == nil && rpcError.Message != "" {
			var data any
			if len(bytes.TrimSpace(rpcError.Data)) > 0 && !bytes.Equal(bytes.TrimSpace(rpcError.Data), []byte("null")) {
				_ = json.Unmarshal(rpcError.Data, &data)
			}
			return nil, &MCPRequestError{Code: rpcError.Code, Message: rpcError.Message, Data: data}
		}
		var value any
		if err := json.Unmarshal(errorPayload, &value); err != nil {
			return nil, fmt.Errorf("agnt5: decode MCP error: %w", err)
		}
		return nil, &MCPRequestError{Message: stringify(value)}
	}
	resultPayload := bytes.TrimSpace(envelope.Result)
	if len(resultPayload) == 0 {
		return nil, errors.New("agnt5: MCP response has neither result nor error")
	}
	if bytes.Equal(resultPayload, []byte("null")) {
		return nil, nil
	}
	var result map[string]any
	if err := json.Unmarshal(resultPayload, &result); err != nil {
		return nil, fmt.Errorf("agnt5: MCP result must be an object: %w", err)
	}
	return result, nil
}

func (s *mcpStreamSession) connectionError() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.terminalErr
}

func (s *mcpStreamSession) shutdown(err error) {
	if err == nil {
		err = ErrMCPTransportClosed
	}
	s.shutdownOnce.Do(func() {
		s.stateMu.Lock()
		s.terminalErr = err
		pending := s.pending
		s.pending = make(map[string]chan mcpResponseResult)
		s.stateMu.Unlock()
		for _, responseCh := range pending {
			responseCh <- mcpResponseResult{err: err}
		}
		if s.closeFn != nil {
			s.closeErr = s.closeFn()
		}
		close(s.done)
	})
}

func (s *mcpStreamSession) Close() error {
	if s == nil {
		return nil
	}
	s.shutdown(ErrMCPTransportClosed)
	<-s.done
	return s.closeErr
}

// StreamMCPTransport sends newline-delimited JSON-RPC messages over an io pair.
type StreamMCPTransport struct {
	session *mcpStreamSession
}

func NewStreamMCPTransport(rw io.ReadWriter) *StreamMCPTransport {
	if rw == nil {
		return &StreamMCPTransport{}
	}
	var closeFn func() error
	if closer, ok := rw.(io.Closer); ok {
		closeFn = closer.Close
	}
	return &StreamMCPTransport{session: newMCPStreamSession(rw, rw, closeFn)}
}

func (t *StreamMCPTransport) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if t == nil || t.session == nil {
		return nil, errors.New("agnt5: nil MCP stream")
	}
	return t.session.Request(ctx, method, params)
}

func (t *StreamMCPTransport) Notify(ctx context.Context, method string, params any) error {
	if t == nil || t.session == nil {
		return errors.New("agnt5: nil MCP stream")
	}
	return t.session.Notify(ctx, method, params)
}

func (t *StreamMCPTransport) Close() error {
	if t == nil || t.session == nil {
		return nil
	}
	return t.session.Close()
}

// StdioMCPTransport manages an MCP server process over newline-delimited JSON-RPC.
type StdioMCPTransport struct {
	cmd     *exec.Cmd
	session *mcpStreamSession

	processDone chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func NewStdioMCPTransport(ctx context.Context, command string, args ...string) (*StdioMCPTransport, error) {
	return NewStdioMCPTransportConfig(ctx, ServerConfig{Transport: TransportTypeStdio, Command: command, Args: args})
}

func NewStdioMCPTransportConfig(ctx context.Context, config ServerConfig) (*StdioMCPTransport, error) {
	if strings.TrimSpace(config.Command) == "" {
		return nil, errors.New("agnt5: MCP stdio command is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, config.Command, config.Args...)
	cmd.Env = os.Environ()
	if len(config.Env) > 0 {
		for key, value := range config.Env {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	transport := &StdioMCPTransport{
		cmd:         cmd,
		processDone: make(chan struct{}),
	}
	transport.session = newMCPStreamSession(stdout, stdin, func() error {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil
	})
	go func() {
		processErr := cmd.Wait()
		close(transport.processDone)
		if processErr != nil {
			transport.session.shutdown(fmt.Errorf("%w: MCP process exited: %w", ErrMCPTransportClosed, processErr))
			return
		}
		transport.session.shutdown(ErrMCPTransportClosed)
	}()
	return transport, nil
}

func (t *StdioMCPTransport) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if t == nil || t.session == nil {
		return nil, errors.New("agnt5: nil MCP stdio transport")
	}
	return t.session.Request(ctx, method, params)
}

func (t *StdioMCPTransport) Notify(ctx context.Context, method string, params any) error {
	if t == nil || t.session == nil {
		return errors.New("agnt5: nil MCP stdio transport")
	}
	return t.session.Notify(ctx, method, params)
}

func (t *StdioMCPTransport) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		var sessionErr error
		if t.session != nil {
			sessionErr = t.session.Close()
		}
		if t.cmd != nil && t.cmd.Process != nil {
			select {
			case <-t.processDone:
			default:
				if err := t.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.closeErr = err
				}
				<-t.processDone
			}
		}
		t.closeErr = errors.Join(t.closeErr, sessionErr)
	})
	return t.closeErr
}

// SSEMCPTransport sends MCP JSON-RPC requests to an HTTP/SSE MCP endpoint.
// It accepts both plain JSON responses and text/event-stream responses with
// JSON payloads in data: lines.
type SSEMCPTransport struct {
	Endpoint   string
	Headers    map[string]string
	HTTPClient *http.Client

	mu        sync.Mutex
	nextID    uint64
	sessionID string
	closed    bool
}

func NewSSEMCPTransport(endpoint string, headers map[string]string) *SSEMCPTransport {
	return &SSEMCPTransport{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		Headers:    cloneStringMap(headers),
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (t *SSEMCPTransport) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if t == nil || t.Endpoint == "" {
		return nil, errors.New("agnt5: MCP SSE endpoint is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id, sessionID, err := t.requestState()
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := t.newRequest(ctx, body, sessionID)
	if err != nil {
		return nil, err
	}
	client := t.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, errors.New("agnt5: MCP SSE returned HTTP " + intString(resp.StatusCode))
	}
	t.captureSessionID(resp)
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseMCPHTTPResponse(payload, strconv.FormatUint(id, 10))
}

func (t *SSEMCPTransport) Notify(ctx context.Context, method string, params any) error {
	if t == nil || t.Endpoint == "" {
		return errors.New("agnt5: MCP SSE endpoint is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrMCPTransportClosed
	}
	sessionID := t.sessionID
	t.mu.Unlock()
	payload := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		payload["params"] = params
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := t.newRequest(ctx, body, sessionID)
	if err != nil {
		return err
	}
	client := t.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return errors.New("agnt5: MCP SSE returned HTTP " + intString(resp.StatusCode))
	}
	t.captureSessionID(resp)
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func (t *SSEMCPTransport) requestState() (uint64, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, "", ErrMCPTransportClosed
	}
	t.nextID++
	return t.nextID, t.sessionID, nil
}

func (t *SSEMCPTransport) newRequest(ctx context.Context, body []byte, sessionID string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-11-25")
	if sessionID != "" {
		req.Header.Set("MCP-Session-Id", sessionID)
	}
	for key, value := range t.Headers {
		req.Header.Set(key, value)
	}
	return req, nil
}

func (t *SSEMCPTransport) captureSessionID(resp *http.Response) {
	if resp == nil {
		return
	}
	sessionID := resp.Header.Get("MCP-Session-Id")
	if sessionID == "" {
		return
	}
	t.mu.Lock()
	t.sessionID = sessionID
	t.mu.Unlock()
}

func (t *SSEMCPTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

// StaticMCPTransport is a deterministic in-memory transport for tests.
type StaticMCPTransport struct {
	Responses map[string]map[string]any
}

func (t StaticMCPTransport) Request(_ context.Context, method string, _ any) (map[string]any, error) {
	if resp, ok := t.Responses[method]; ok {
		return resp, nil
	}
	return nil, errors.New("agnt5: MCP response not configured: " + method)
}

func (t StaticMCPTransport) Close() error { return nil }

func parseMCPHTTPResponse(payload []byte, targetID string) (map[string]any, error) {
	raw := bytes.TrimSpace(payload)
	if bytes.HasPrefix(raw, []byte("data:")) || bytes.Contains(raw, []byte("\ndata:")) {
		lines := bytes.Split(raw, []byte("\n"))
		found := false
		var legacyResponse []byte
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if bytes.Equal(data, []byte("[DONE]")) || len(data) == 0 {
				continue
			}
			var envelope mcpResponseEnvelope
			if err := json.Unmarshal(data, &envelope); err != nil {
				continue
			}
			if envelope.Method != "" {
				continue
			}
			if len(bytes.TrimSpace(envelope.ID)) == 0 {
				if len(bytes.TrimSpace(envelope.Result)) > 0 || len(bytes.TrimSpace(envelope.Error)) > 0 {
					legacyResponse = data
				}
				continue
			}
			if string(bytes.TrimSpace(envelope.ID)) != targetID {
				continue
			}
			raw = data
			found = true
			break
		}
		if !found && legacyResponse != nil {
			raw = legacyResponse
			found = true
		}
		if !found {
			return nil, fmt.Errorf("agnt5: MCP SSE response did not contain request id %s", targetID)
		}
	}
	var envelope mcpResponseEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(envelope.ID)) > 0 {
		if string(bytes.TrimSpace(envelope.ID)) != targetID {
			return nil, fmt.Errorf("agnt5: MCP response id %s does not match request id %s", bytes.TrimSpace(envelope.ID), targetID)
		}
		return decodeMCPResponse(envelope)
	}
	if len(bytes.TrimSpace(envelope.Result)) > 0 || len(bytes.TrimSpace(envelope.Error)) > 0 {
		return decodeMCPResponse(envelope)
	}
	var direct map[string]any
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, err
	}
	return direct, nil
}

func stringify(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(bytes.TrimSpace(encoded))
	}
}
