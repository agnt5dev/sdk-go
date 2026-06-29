package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
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

// MCPClient is a small MCP JSON-RPC client.
type MCPClient struct {
	transport MCPTransport
}

func NewMCPClient(transport MCPTransport) (*MCPClient, error) {
	if transport == nil {
		return nil, errors.New("agnt5: nil MCP transport")
	}
	return &MCPClient{transport: transport}, nil
}

func (c *MCPClient) ListTools(ctx context.Context) ([]MCPTool, error) {
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
	return c.transport.Close()
}

// StreamMCPTransport sends newline-delimited JSON-RPC messages over an io pair.
type StreamMCPTransport struct {
	mu     sync.Mutex
	nextID int
	rw     io.ReadWriter
}

func NewStreamMCPTransport(rw io.ReadWriter) *StreamMCPTransport {
	return &StreamMCPTransport{rw: rw}
}

func (t *StreamMCPTransport) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if t == nil || t.rw == nil {
		return nil, errors.New("agnt5: nil MCP stream")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	request := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := t.rw.Write(append(encoded, '\n')); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	var response map[string]any
	var decodeErr error
	go func() {
		defer close(done)
		decoder := json.NewDecoder(t.rw)
		decodeErr = decoder.Decode(&response)
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}
	if decodeErr != nil {
		return nil, decodeErr
	}
	if errValue, ok := response["error"]; ok {
		return nil, errors.New("agnt5: MCP error: " + stringify(errValue))
	}
	result, _ := response["result"].(map[string]any)
	return result, nil
}

func (t *StreamMCPTransport) Close() error {
	if closer, ok := t.rw.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// StdioMCPTransport manages an MCP server process over newline-delimited JSON-RPC.
type StdioMCPTransport struct {
	mu      sync.Mutex
	nextID  int
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	decoder *json.Decoder
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
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &StdioMCPTransport{cmd: cmd, stdin: stdin, stdout: stdout, decoder: json.NewDecoder(stdout)}, nil
}

func (t *StdioMCPTransport) Request(ctx context.Context, method string, params any) (map[string]any, error) {
	if t == nil || t.stdin == nil || t.decoder == nil {
		return nil, errors.New("agnt5: nil MCP stdio transport")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	request := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := t.stdin.Write(append(encoded, '\n')); err != nil {
		return nil, err
	}
	type responseEnvelope struct {
		ID     any            `json:"id"`
		Result map[string]any `json:"result"`
		Error  any            `json:"error"`
	}
	done := make(chan responseEnvelope, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			var response responseEnvelope
			if err := t.decoder.Decode(&response); err != nil {
				errCh <- err
				return
			}
			if response.ID == nil {
				continue
			}
			done <- response
			return
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case response := <-done:
		if response.Error != nil {
			return nil, errors.New("agnt5: MCP error: " + stringify(response.Error))
		}
		return response.Result, nil
	}
}

func (t *StdioMCPTransport) Close() error {
	if t == nil {
		return nil
	}
	if t.stdin != nil {
		_ = t.stdin.Close()
	}
	if t.stdout != nil {
		_ = t.stdout.Close()
	}
	if t.cmd != nil && t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
		_ = t.cmd.Wait()
	}
	return nil
}

// SSEMCPTransport sends MCP JSON-RPC requests to an HTTP/SSE MCP endpoint.
// It accepts both plain JSON responses and text/event-stream responses with
// JSON payloads in data: lines.
type SSEMCPTransport struct {
	Endpoint   string
	Headers    map[string]string
	HTTPClient *http.Client
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
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for key, value := range t.Headers {
		req.Header.Set(key, value)
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
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseMCPHTTPResponse(payload)
}

func (t *SSEMCPTransport) Close() error { return nil }

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

func parseMCPHTTPResponse(payload []byte) (map[string]any, error) {
	raw := bytes.TrimSpace(payload)
	if bytes.HasPrefix(raw, []byte("data:")) || bytes.Contains(raw, []byte("\ndata:")) {
		lines := bytes.Split(raw, []byte("\n"))
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if bytes.Equal(data, []byte("[DONE]")) || len(data) == 0 {
				continue
			}
			raw = data
			break
		}
	}
	var envelope struct {
		Result map[string]any `json:"result"`
		Error  any            `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, errors.New("agnt5: MCP error: " + stringify(envelope.Error))
	}
	if envelope.Result != nil {
		return envelope.Result, nil
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
