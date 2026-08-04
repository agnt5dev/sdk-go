package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMCPClientListAndCallTools(t *testing.T) {
	client, err := NewMCPClient(StaticMCPTransport{Responses: map[string]map[string]any{
		"tools/list": {"tools": []any{map[string]any{"name": "lookup", "description": "Lookup"}}},
		"tools/call": {"content": []any{map[string]any{"type": "text", "text": "ok"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0].Name != "lookup" {
		t.Fatalf("tools = %#v", tools)
	}
	result, err := client.CallTool(context.Background(), "lookup", map[string]any{"key": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Content) != 1 || result.Content[0]["text"] != "ok" {
		t.Fatalf("result = %#v", result)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTools(context.Background()); !errors.Is(err, ErrMCPTransportClosed) {
		t.Fatalf("list after close err = %v", err)
	}
}

func TestSSEMCPTransportParsesEventStream(t *testing.T) {
	transport := NewSSEMCPTransport("http://mcp.example", map[string]string{"X-Test": "yes"})
	var mu sync.Mutex
	var methods []string
	transport.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("X-Test") != "yes" {
			t.Fatalf("headers = %#v", r.Header)
		}
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		switch request.Method {
		case "initialize":
			body, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result": map[string]any{
					"protocolVersion": "2025-11-25",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "test", "version": "1"},
				},
			})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Type":   []string{"application/json"},
					"Mcp-Session-Id": []string{"session-1"},
				},
				Body:    io.NopCloser(strings.NewReader(string(body))),
				Request: r,
			}, nil
		case "notifications/initialized":
			if r.Header.Get("MCP-Session-Id") != "session-1" {
				t.Fatalf("notification session headers = %#v", r.Header)
			}
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    r,
			}, nil
		case "tools/list":
			if r.Header.Get("MCP-Session-Id") != "session-1" {
				t.Fatalf("request session headers = %#v", r.Header)
			}
			body, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  map[string]any{"tools": []any{map[string]any{"name": "fetch", "description": "Fetch URL"}}},
			})
			stream := "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n" +
				"data: {\"jsonrpc\":\"2.0\",\"id\":999,\"result\":{}}\n\n" +
				"data: " + string(body) + "\n\n"
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(stream)),
				Request:    r,
			}, nil
		default:
			t.Fatalf("unexpected MCP method %q", request.Method)
		}
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    r,
		}, nil
	})}

	client, err := NewMCPClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0].Name != "fetch" {
		t.Fatalf("tools = %#v", tools)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(methods, ",") != "initialize,notifications/initialized,tools/list" {
		t.Fatalf("methods = %#v", methods)
	}
}

func TestStreamMCPTransportCorrelatesConcurrentResponses(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	transport := NewStreamMCPTransport(clientConn)
	defer func() { _ = transport.Close() }()
	defer serverConn.Close()

	serverErr := make(chan error, 1)
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		requests := make([]mcpTestRequest, 2)
		for index := range requests {
			if err := decoder.Decode(&requests[index]); err != nil {
				serverErr <- err
				return
			}
		}
		if err := encoder.Encode(map[string]any{
			"jsonrpc": "2.0",
			"method":  "notifications/tools/list_changed",
		}); err != nil {
			serverErr <- err
			return
		}
		for index := len(requests) - 1; index >= 0; index-- {
			if err := encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      requests[index].ID,
				"result":  map[string]any{"method": requests[index].Method},
			}); err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
	}()

	type requestResult struct {
		method string
		value  map[string]any
		err    error
	}
	results := make(chan requestResult, 2)
	for _, method := range []string{"first", "second"} {
		method := method
		go func() {
			value, err := transport.Request(context.Background(), method, map[string]any{})
			results <- requestResult{method: method, value: value, err: err}
		}()
	}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.value["method"] != result.method {
			t.Fatalf("result for %s = %#v", result.method, result.value)
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestStreamMCPTransportCancellationDoesNotStealNextResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	transport := NewStreamMCPTransport(clientConn)
	defer func() { _ = transport.Close() }()
	defer serverConn.Close()

	firstRead := make(chan struct{})
	releaseLateResponse := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var first mcpTestRequest
		if err := decoder.Decode(&first); err != nil {
			serverErr <- err
			return
		}
		close(firstRead)
		<-releaseLateResponse
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": first.ID, "result": map[string]any{"late": true}}); err != nil {
			serverErr <- err
			return
		}
		var second mcpTestRequest
		if err := decoder.Decode(&second); err != nil {
			serverErr <- err
			return
		}
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": second.ID, "result": map[string]any{"ok": true}}); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := transport.Request(ctx, "first", nil)
		firstResult <- err
	}()
	<-firstRead
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first err = %v", err)
	}
	secondResult := make(chan struct {
		value map[string]any
		err   error
	}, 1)
	go func() {
		value, err := transport.Request(context.Background(), "second", nil)
		secondResult <- struct {
			value map[string]any
			err   error
		}{value: value, err: err}
	}()
	close(releaseLateResponse)
	result := <-secondResult
	if result.err != nil || result.value["ok"] != true {
		t.Fatalf("second result = %#v, %v", result.value, result.err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestStreamMCPTransportReturnsTypedJSONRPCError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	transport := NewStreamMCPTransport(clientConn)
	defer func() { _ = transport.Close() }()
	defer serverConn.Close()
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var request mcpTestRequest
		if decoder.Decode(&request) == nil {
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"error": map[string]any{
					"code":    -32601,
					"message": "unknown method",
					"data":    map[string]any{"method": request.Method},
				},
			})
		}
	}()
	_, err := transport.Request(context.Background(), "missing", nil)
	var requestErr *MCPRequestError
	if !errors.As(err, &requestErr) || requestErr.Code != -32601 || requestErr.Message != "unknown method" {
		t.Fatalf("err = %#v", err)
	}
}

func TestStreamMCPTransportMarshalErrorDoesNotCloseConnection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	transport := NewStreamMCPTransport(clientConn)
	defer func() { _ = transport.Close() }()
	defer serverConn.Close()

	if _, err := transport.Request(context.Background(), "invalid", map[string]any{"value": func() {}}); err == nil {
		t.Fatal("expected JSON marshal error")
	}
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var request mcpTestRequest
		if decoder.Decode(&request) == nil {
			_ = encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  map[string]any{"ok": true},
			})
		}
	}()
	result, err := transport.Request(context.Background(), "valid", nil)
	if err != nil || result["ok"] != true {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

func TestStreamMCPTransportCloseUnblocksPendingRequest(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	transport := NewStreamMCPTransport(clientConn)
	defer serverConn.Close()
	requestRead := make(chan struct{})
	go func() {
		var request mcpTestRequest
		_ = json.NewDecoder(serverConn).Decode(&request)
		close(requestRead)
	}()
	requestErr := make(chan error, 1)
	go func() {
		_, err := transport.Request(context.Background(), "wait", nil)
		requestErr <- err
	}()
	<-requestRead
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-requestErr:
		if !errors.Is(err, ErrMCPTransportClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending request was not unblocked")
	}
}

func TestMCPClientInitializesStdioServer(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewStdioMCPTransportConfig(context.Background(), ServerConfig{
		Transport: TransportTypeStdio,
		Command:   executable,
		Args:      []string{"-test.run=^TestMCPStdioHelperProcess$"},
		Env:       map[string]string{"AGNT5_MCP_HELPER": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewMCPClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0].Name != "stdio_tool" {
		t.Fatalf("tools = %#v", tools)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := client.ListTools(context.Background()); !errors.Is(err, ErrMCPTransportClosed) {
		t.Fatalf("list after close err = %v", err)
	}
}

func TestStdioMCPTransportProcessExitUnblocksRequest(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewStdioMCPTransportConfig(context.Background(), ServerConfig{
		Transport: TransportTypeStdio,
		Command:   executable,
		Args:      []string{"-test.run=^TestMCPStdioHelperProcess$"},
		Env: map[string]string{
			"AGNT5_MCP_HELPER":      "1",
			"AGNT5_MCP_HELPER_EXIT": "1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Close() }()
	client, err := NewMCPClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.ListTools(ctx)
	if !errors.Is(err, ErrMCPTransportClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestMCPClientInitializesOnceForConcurrentCallers(t *testing.T) {
	transport := &lifecycleMCPTransport{
		initializeStarted: make(chan struct{}, 2),
		releaseInitialize: make(chan struct{}),
	}
	client, err := NewMCPClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			tools, err := client.ListTools(context.Background())
			if err == nil && (len(tools) != 1 || tools[0].Name != "concurrent") {
				err = fmt.Errorf("tools = %#v", tools)
			}
			results <- err
		}()
	}
	close(start)
	<-transport.initializeStarted
	close(transport.releaseInitialize)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.initializeCalls != 1 || transport.notificationCalls != 1 || transport.listCalls != 2 {
		t.Fatalf("calls: initialize=%d notification=%d list=%d", transport.initializeCalls, transport.notificationCalls, transport.listCalls)
	}
}

func TestMCPStdioHelperProcess(t *testing.T) {
	if os.Getenv("AGNT5_MCP_HELPER") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	initialized := false
	for {
		var request mcpTestRequest
		if err := decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) {
				os.Exit(0)
			}
			os.Exit(2)
		}
		switch request.Method {
		case "initialize":
			if os.Getenv("AGNT5_MCP_HELPER_EXIT") == "1" {
				os.Exit(3)
			}
			if err := encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result": map[string]any{
					"protocolVersion": "2025-11-25",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "helper", "version": "1"},
				},
			}); err != nil {
				os.Exit(2)
			}
		case "notifications/initialized":
			initialized = true
		case "tools/list":
			if !initialized {
				os.Exit(2)
			}
			if err := encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      request.ID,
				"result":  map[string]any{"tools": []any{map[string]any{"name": "stdio_tool"}}},
			}); err != nil {
				os.Exit(2)
			}
		default:
			os.Exit(2)
		}
	}
}

type mcpTestRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

type lifecycleMCPTransport struct {
	mu sync.Mutex

	initializeStarted chan struct{}
	releaseInitialize chan struct{}
	initializeCalls   int
	notificationCalls int
	listCalls         int
}

func (t *lifecycleMCPTransport) Request(_ context.Context, method string, _ any) (map[string]any, error) {
	switch method {
	case "initialize":
		t.mu.Lock()
		t.initializeCalls++
		t.mu.Unlock()
		t.initializeStarted <- struct{}{}
		<-t.releaseInitialize
		return map[string]any{"protocolVersion": "2025-11-25"}, nil
	case "tools/list":
		t.mu.Lock()
		t.listCalls++
		t.mu.Unlock()
		return map[string]any{"tools": []any{map[string]any{"name": "concurrent"}}}, nil
	default:
		return nil, fmt.Errorf("unexpected method %q", method)
	}
}

func (t *lifecycleMCPTransport) Notify(_ context.Context, method string, _ any) error {
	if method != "notifications/initialized" {
		return fmt.Errorf("unexpected notification %q", method)
	}
	t.mu.Lock()
	t.notificationCalls++
	t.mu.Unlock()
	return nil
}

func (t *lifecycleMCPTransport) Close() error { return nil }

func TestInMemorySandboxFiles(t *testing.T) {
	sandbox := NewInMemorySandbox()
	if _, err := sandbox.WriteFile(context.Background(), "/tmp/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	read, err := sandbox.ReadFile(context.Background(), "/tmp/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(read.Content) != "hello" {
		t.Fatalf("read = %#v", read)
	}
	list, err := sandbox.ListFiles(context.Background(), "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Files) != 1 {
		t.Fatalf("list = %#v", list)
	}
}

func TestInMemorySandboxEmitsEvents(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeFunction}, nil, "")
	sandbox := NewInMemorySandbox()
	if _, err := sandbox.WriteFile(ctx, "/tmp/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := sandbox.ReadFile(ctx, "/tmp/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := sandbox.ListFiles(ctx, "/tmp"); err != nil {
		t.Fatal(err)
	}
	events := ctx.Events()
	for _, eventType := range []string{
		"sandbox.file.write.started",
		"sandbox.file.write.completed",
		"sandbox.file.read.started",
		"sandbox.file.read.completed",
		"sandbox.file.list.started",
		"sandbox.file.list.completed",
	} {
		if !hasEventType(events, eventType) {
			t.Fatalf("missing event %s in %#v", eventType, events)
		}
	}
}

func hasEventType(events []Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}
