package serverless

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func invoke(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodPost, InvokePath, strings.NewReader(body)))
	return r
}

func TestFunctionToolAndAgent(t *testing.T) {
	h := New(Options{ServiceName: "parity"})
	if err := RegisterFunction(h, "double", func(_ *Context, in struct {
		Value int `json:"value"`
	}) (int, error) {
		return in.Value * 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterTool(h, Tool{Name: "lookup", Description: "lookup order", Schema: map[string]any{"type": "object"}, Handler: func(_ context.Context, args map[string]any) (any, error) { return args["id"], nil }}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterAgent(h, Agent{Name: "helper", Run: func(_ *Context, input AgentInput) (AgentResult, error) {
		return AgentResult{Output: "answer:" + input.Message}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	cases := []struct{ kind, name, input, want string }{{"function", "double", `{"value":4}`, `"output":8`}, {"tool", "lookup", `{"id":"o1"}`, `"output":"o1"`}, {"agent", "helper", `{"message":"hi","session_id":"s1"}`, `"output":"answer:hi"`}}
	for _, tc := range cases {
		response := invoke(t, h, `{"component_type":"`+tc.kind+`","component_name":"`+tc.name+`","run_id":"r1","input":`+tc.input+`}`)
		if response.Code != 200 || !strings.Contains(response.Body.String(), tc.want) {
			t.Fatalf("%s response: %d %s", tc.kind, response.Code, response.Body.String())
		}
	}
	manifest := httptest.NewRecorder()
	h.ServeHTTP(manifest, httptest.NewRequest(http.MethodGet, ManifestPath, nil))
	for _, kind := range []string{"agent", "function", "tool"} {
		if !strings.Contains(manifest.Body.String(), `"component_type":"`+kind+`"`) {
			t.Fatalf("manifest missing %s: %s", kind, manifest.Body.String())
		}
	}
}

func TestSignalAndUserInputResume(t *testing.T) {
	h := New(Options{})
	_ = RegisterWorkflow(h, "approval", func(ctx *Context, _ struct{}) (string, error) {
		signal, err := WaitForSignal[string](ctx, "approved", "gate")
		if err != nil {
			return "", err
		}
		answer, err := ctx.WaitForUser(UserInput{Question: "Proceed?", Type: "approval", Skippable: true})
		return signal + ":" + answer, err
	})
	suspended := invoke(t, h, `{"component_type":"workflow","component_name":"approval","run_id":"r1","input":{}}`)
	if !strings.Contains(suspended.Body.String(), `"reason":"signal"`) {
		t.Fatal(suspended.Body.String())
	}
	userWait := invoke(t, h, `{"component_type":"workflow","component_name":"approval","run_id":"r1","input":{},"metadata":{"signal_name":"approved","waiting_step":"gate","signal_payload":"\"yes\""}}`)
	if !strings.Contains(userWait.Body.String(), `"reason":"user_input_required"`) {
		t.Fatal(userWait.Body.String())
	}
	completed := invoke(t, h, `{"component_type":"workflow","component_name":"approval","run_id":"r1","input":{},"metadata":{"signal_name":"approved","waiting_step":"gate","signal_payload":"\"yes\"","pause_index":"0","user_response":"ok"}}`)
	if !strings.Contains(completed.Body.String(), `"output":"yes:ok"`) {
		t.Fatal(completed.Body.String())
	}
}

func TestInputAndOutputReferences(t *testing.T) {
	input := []byte(`{"name":"Ada"}`)
	digest := sha256.Sum256(input)
	var uploaded []byte
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(input))), Header: make(http.Header)}, nil
		}
		uploaded, _ = io.ReadAll(r.Body)
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	h := New(Options{HTTPClient: client})
	_ = RegisterWorkflow(h, "hello", func(_ *Context, in map[string]string) (map[string]string, error) {
		return map[string]string{"message": "hello " + in["name"]}, nil
	})
	body := map[string]any{"component_type": "workflow", "component_name": "hello", "run_id": "r1", "input_ref": map[string]any{"kind": payloadRefKind, "url": "https://store.test/input", "method": "GET", "size_bytes": len(input), "sha256": hex.EncodeToString(digest[:]), "expires_at_ms": time.Now().Add(time.Minute).UnixMilli()}, "output_upload": map[string]any{"kind": payloadRefKind, "url": "https://store.test/output", "method": "PUT", "ref": "out.json", "threshold_bytes": 1, "max_bytes": 1024, "content_type": "application/json", "expires_at_ms": time.Now().Add(time.Minute).UnixMilli()}}
	raw, _ := json.Marshal(body)
	response := invoke(t, h, string(raw))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"output_ref"`) {
		t.Fatalf("response: %d %s", response.Code, response.Body.String())
	}
	if string(uploaded) != `{"message":"hello Ada"}` {
		t.Fatalf("uploaded %q", uploaded)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestAgentSessionCheckpointResume(t *testing.T) {
	h := New(Options{})
	seen := 0
	_ = RegisterAgent(h, Agent{Name: "helper", Run: func(_ *Context, input AgentInput) (AgentResult, error) {
		seen = len(input.History)
		return AgentResult{Output: "ok"}, nil
	}})
	response := invoke(t, h, `{"component_type":"agent","component_name":"helper","run_id":"r1","input":{"message":"again","session_id":"s1"},"checkpoint":{"agent_sessions":{"s1":{"messages":[{"role":"user","content":"before"},{"role":"assistant","content":"answer"}]}}}}`)
	if response.Code != 200 || seen != 2 || !strings.Contains(response.Body.String(), `"agent_sessions"`) {
		t.Fatalf("seen=%d response=%s", seen, response.Body.String())
	}
}
