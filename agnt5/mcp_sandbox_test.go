package agnt5

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
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
}

func TestSSEMCPTransportParsesEventStream(t *testing.T) {
	transport := NewSSEMCPTransport("http://mcp.example", map[string]string{"X-Test": "yes"})
	transport.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("X-Test") != "yes" {
			t.Fatalf("headers = %#v", r.Header)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(`data: {"result":{"tools":[{"name":"fetch","description":"Fetch URL"}]}}` + "\n\n")),
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
}

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
