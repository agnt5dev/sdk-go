package agnt5

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func TestWorkflowAndSessionProxies(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workflows/process/run" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Session-ID") != "session-1" || r.Header.Get("X-User-ID") != "user-1" {
			t.Fatalf("headers: %#v", r.Header)
		}
		_, _ = w.Write([]byte(`{"run_id":"run-1","status":"completed","output":{"ok":true}}`))
	})
	resp, err := client.Session("session-1").WithUser("user-1").Workflow("process").Run(context.Background(), map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.IsSuccess() {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestClientEval(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/eval" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		var body EvalRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Component != "greet" || len(body.Scorers) != 1 || body.Scorers[0].Name != "exact_match" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{
			"run_id":"run-1",
			"output":{"message":"hello"},
			"passed":true,
			"scores":[{"scorer":"exact_match","score":1,"passed":true}]
		}`))
	})
	resp, err := client.Eval(context.Background(), EvalRequest{
		Component: "greet",
		Input:     map[string]any{"name": "Ada"},
		Expected:  map[string]any{"message": "hello"},
		Scorers:   []EvalScorerSpec{{Name: "exact_match"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Passed || len(resp.Scores) != 1 || resp.Scores[0].Scorer != "exact_match" {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestClientChat(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agents/assistant/chat" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["message"] != "hi" || body["session_id"] != "s1" {
			t.Fatalf("body = %#v", body)
		}
		if _, ok := body["content"]; ok {
			t.Fatalf("legacy content field in body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"run-1","session_id":"s1","status":"completed","output":{"session_id":"s1","message":{"role":"assistant","content":"hello"}}}`))
	})
	resp, err := client.Chat(context.Background(), "assistant", ChatMessage{SessionID: "s1", Role: MessageRoleUser, Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.Content != "hello" {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestClientChatAcceptsDirectResponse(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session_id":"s1","message":{"role":"assistant","content":"hello"}}`))
	})
	resp, err := client.Chat(context.Background(), "assistant", ChatMessage{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SessionID != "s1" || resp.Message.Content != "hello" {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestClientResumeWorkflow(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workflows/resume/run-1" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Session-ID") != "session-1" ||
			r.Header.Get("X-User-ID") != "user-1" ||
			r.Header.Get("X-TENANT-ID") != "tenant-1" {
			t.Fatalf("headers: %#v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["user_response"] != "approve" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"run-1","status":"resumed","offset":42}`))
	})
	resp, err := client.ResumeWorkflow(context.Background(), "run-1", "approve",
		WithRunSessionID("session-1"),
		WithRunUserID("user-1"),
		WithRunTenant("tenant-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if resp.RunID != "run-1" || resp.Status != "resumed" || resp.Offset != 42 {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestClientCancelRun(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runs/run-1/cancel" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-TENANT-ID") != "tenant-1" {
			t.Fatalf("headers: %#v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["reason"] != "ui-test" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"run-1","status":"cancelled","offset":7}`))
	})
	resp, err := client.CancelRun(context.Background(), "run-1", "ui-test", WithRunTenant("tenant-1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.RunID != "run-1" || resp.Status != "cancelled" || resp.Offset != 7 {
		t.Fatalf("resp = %#v", resp)
	}
}
