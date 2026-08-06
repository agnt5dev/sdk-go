package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newHTTPTestClient(t *testing.T, handler http.HandlerFunc, opts ...ClientOption) *Client {
	t.Helper()
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			response := recorder.Result()
			response.Request = req
			return response, nil
		}),
	}
	opts = append([]ClientOption{WithHTTPClient(httpClient)}, opts...)
	client, err := NewClient("http://agnt5.test", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestClientRunPostsHeadersAndParsesResponse(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: %s", r.Method)
		}
		if r.URL.Path != "/v1/functions/greet/run" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-API-KEY") != "agnt5_sk_test" ||
			r.Header.Get("X-TENANT-ID") != "tenant-call" ||
			r.Header.Get("X-DEPLOYMENT-ID") != "dep-1" ||
			r.Header.Get("X-Session-ID") != "session-1" ||
			r.Header.Get("X-User-ID") != "user-1" ||
			r.Header.Get("Idempotency-Key") != "idem-1" {
			t.Fatalf("headers: %#v", r.Header)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["name"] != "Ada" {
			t.Fatalf("body: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"run_id":"run-1",
			"status":"completed",
			"duration_ms":12,
			"trace_id":"trace-1",
			"component":"greet",
			"output":{"message":"hello Ada"}
		}`))
	},
		WithAPIKey("agnt5_sk_test"),
		WithTenantID("tenant-default"),
		WithClientDeploymentID("dep-1"),
	)
	response, err := client.Run(context.Background(), "greet", map[string]string{"name": "Ada"},
		WithRunSessionID("session-1"),
		WithRunUserID("user-1"),
		WithRunTenant("tenant-call"),
		WithRunHeader("Idempotency-Key", "legacy-idem"),
		WithIdempotencyKey("idem-1"),
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !response.IsSuccess() || response.RunID != "run-1" || response.StatusCode != http.StatusOK {
		t.Fatalf("response: %#v", response)
	}
	var output struct {
		Message string `json:"message"`
	}
	if err := response.DecodeOutput(&output); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if output.Message != "hello Ada" {
		t.Fatalf("output: %#v", output)
	}
}

func TestClientRunAcceptsDirectOutputBody(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/functions/greet/run" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"message":"hello direct"}`))
	})
	response, err := client.Run(context.Background(), "greet", nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !response.IsSuccess() {
		t.Fatalf("response: %#v", response)
	}
	var output map[string]string
	if err := response.DecodeOutput(&output); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if output["message"] != "hello direct" {
		t.Fatalf("output: %#v", output)
	}
}

func TestClientSubmitWrapsMetadata(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workflows/process/submit" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-TENANT-ID") != "tenant-job" {
			t.Fatalf("tenant header: %q", r.Header.Get("X-TENANT-ID"))
		}
		if r.Header.Get("Idempotency-Key") != "submit-idem" {
			t.Fatalf("idempotency header: %q", r.Header.Get("Idempotency-Key"))
		}
		var body struct {
			Input map[string]string `json:"input"`
			Meta  map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Input["id"] != "42" || body.Meta["priority"] != "high" {
			t.Fatalf("body: %#v", body)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"run_id":"run-2","status":"queued","links":{"self":"/v1/status/run-2"}}`))
	})
	response, err := client.Submit(context.Background(), "process", map[string]string{"id": "42"},
		WithSubmitComponentType(ComponentTypeWorkflow),
		WithSubmitMetadata(map[string]string{"priority": "high"}),
		WithSubmitTenant("tenant-job"),
		WithSubmitIdempotencyKey("submit-idem"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if response.RunID != "run-2" || response.Status != RunStatusQueued || response.StatusCode != http.StatusAccepted {
		t.Fatalf("response: %#v", response)
	}
	if response.StatusURL() != "/v1/status/run-2" {
		t.Fatalf("status url: %q", response.StatusURL())
	}
}

func TestClientGetStatusResultAndEvents(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status/run-3":
			_, _ = w.Write([]byte(`{"run_id":"run-3","status":"completed","component_name":"greet"}`))
		case "/v1/result/run-3":
			_, _ = w.Write([]byte(`{"run_id":"run-3","status":"completed","output":{"ok":true}}`))
		case "/v1/runs/run-3/events":
			if r.Header.Get("Accept") != "application/json" {
				t.Fatalf("accept: %q", r.Header.Get("Accept"))
			}
			_, _ = w.Write([]byte(`{
				"items":[{
					"event_type":"run.completed",
					"run_id":"run-3",
					"data":{"output_data":{"ok":true}},
					"timestamp_ns":1700000000000000000,
					"metadata":{"trace_id":"trace-3"}
				}],
				"count":1
			}`))
		default:
			http.NotFound(w, r)
		}
	})
	status, err := client.GetStatus(context.Background(), "run-3")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.IsComplete() || status.Component != "greet" {
		t.Fatalf("status response: %#v", status)
	}
	result, err := client.GetResult(context.Background(), "run-3")
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	var output map[string]bool
	if err := result.DecodeOutput(&output); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if !output["ok"] {
		t.Fatalf("result output: %#v", output)
	}
	events, err := client.GetEvents(context.Background(), "run-3")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if events.Count != 1 || events.Items[0].EventType != "run.completed" {
		t.Fatalf("events: %#v", events)
	}
	if string(events.Items[0].Data) == "" {
		t.Fatal("event data was not preserved")
	}
}

func TestClientWaitForResultPollsUntilComplete(t *testing.T) {
	var statusCalls atomic.Int32
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status/run-4":
			call := statusCalls.Add(1)
			if call == 1 {
				_, _ = w.Write([]byte(`{"run_id":"run-4","status":"running"}`))
				return
			}
			_, _ = w.Write([]byte(`{"run_id":"run-4","status":"completed"}`))
		case "/v1/result/run-4":
			_, _ = w.Write([]byte(`{"run_id":"run-4","status":"completed","output":{"done":true}}`))
		default:
			http.NotFound(w, r)
		}
	})
	result, err := client.WaitForResult(context.Background(), "run-4", time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !result.IsSuccess() || statusCalls.Load() != 2 {
		t.Fatalf("result=%#v statusCalls=%d", result, statusCalls.Load())
	}
}

func TestClientStreamEventsParsesSSE(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/functions/generate/stream" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("accept: %q", r.Header.Get("Accept"))
		}
		if r.Header.Get("Idempotency-Key") != "stream-idem" {
			t.Fatalf("idempotency header: %q", r.Header.Get("Idempotency-Key"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: output.delta\n")
		_, _ = fmt.Fprint(w, "data: {\"content\":\"hel\",\"sequence\":1,\"index\":0}\n\n")
		_, _ = fmt.Fprint(w, "event: output.delta\n")
		_, _ = fmt.Fprint(w, "data: {\"delta\":\"l\",\"sequence\":2,\"index\":0}\n\n")
		_, _ = fmt.Fprint(w, "event: output.delta\n")
		_, _ = fmt.Fprint(w, "data: {\"output_data\":\"o\",\"sequence\":3,\"index\":0}\n\n")
		_, _ = fmt.Fprint(w, "event: done\n")
		_, _ = fmt.Fprint(w, "data: {\"done\":true}\n\n")
	})
	var chunks []string
	err := client.Stream(context.Background(), "generate", map[string]string{"prompt": "hi"}, func(chunk string) error {
		chunks = append(chunks, chunk)
		return nil
	}, WithRunIdempotencyKey("stream-idem"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if strings.Join(chunks, "") != "hello" {
		t.Fatalf("chunks: %#v", chunks)
	}
}

func TestClientStreamUnwrapsGatewayEventEnvelope(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: output.delta\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"output.delta\",\"run_id\":\"run-6\",\"data\":{\"content\":\"hel\",\"index\":0}}\n\n")
		_, _ = fmt.Fprint(w, "event: output.delta\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"output.delta\",\"run_id\":\"run-6\",\"data\":{\"content\":\"lo\",\"index\":0}}\n\n")
		_, _ = fmt.Fprint(w, "event: run.completed\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"run.completed\",\"run_id\":\"run-6\",\"data\":{\"output_data\":\"hello\"}}\n\n")
	})
	var chunks []string
	err := client.Stream(context.Background(), "generate", nil, func(chunk string) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if strings.Join(chunks, "") != "hello" {
		t.Fatalf("chunks: %#v", chunks)
	}
	var events []ReceivedEvent
	err = client.StreamEvents(context.Background(), "generate", nil, func(event ReceivedEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("stream events: %v", err)
	}
	if len(events) != 3 || events[0].Data["content"] != "hel" || events[0].RunID != "run-6" || events[0].Sequence != 1 {
		t.Fatalf("events: %#v", events)
	}
}

func TestClientStreamReturnsEnvelopedRunFailure(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: run.failed\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"run.failed\",\"run_id\":\"run-7\",\"data\":{\"error_message\":\"boom\",\"error_code\":\"FUNCTION_ERROR\"}}\n\n")
	})
	err := client.Stream(context.Background(), "generate", nil, func(string) error { return nil })
	var runErr *RunError
	if !errors.As(err, &runErr) || runErr.Message != "boom" || runErr.ErrorCode != "FUNCTION_ERROR" {
		t.Fatalf("run error: %#v (%v)", runErr, err)
	}
}

func TestClientStreamEventsReturnsRunError(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: error\n")
		_, _ = fmt.Fprint(w, "data: {\"run_id\":\"run-5\",\"error\":{\"code\":\"FAILED\",\"message\":\"boom\"},\"metadata\":{\"attempts\":2,\"max_attempts\":2}}\n\n")
	})
	err := client.StreamEvents(context.Background(), "generate", nil, func(ReceivedEvent) error {
		t.Fatal("handler should not be called for error event")
		return nil
	})
	var runErr *RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("expected RunError, got %T %v", err, err)
	}
	if runErr.RunID != "run-5" || runErr.ErrorCode != "FAILED" || !runErr.ExhaustedRetries() {
		t.Fatalf("run error: %#v", runErr)
	}
}

func TestNewClientRejectsInvalidAPIKey(t *testing.T) {
	_, err := NewClient("http://localhost:34183", WithAPIKey("bad"))
	if err == nil {
		t.Fatal("expected invalid api key")
	}
}

func TestNewClientUsesEnvironmentDefaults(t *testing.T) {
	t.Setenv(envGatewayURL, "localhost:34183")
	t.Setenv(envAPIKey, "agnt5_sk_env")
	t.Setenv(envTenantID, "tenant-env")
	t.Setenv(envDeploymentID, "dep-env")

	client, err := NewClient("")
	if err != nil {
		t.Fatal(err)
	}
	if client.gatewayURL.String() != "http://localhost:34183" ||
		client.apiKey != "agnt5_sk_env" ||
		client.tenantID != "tenant-env" ||
		client.deploymentID != "dep-env" {
		t.Fatalf("client: %#v", client)
	}
	if client.httpClient.Timeout != 45*time.Second {
		t.Fatalf("default timeout: %s", client.httpClient.Timeout)
	}
}

func TestNewClientHonorsTimeoutOverride(t *testing.T) {
	client, err := NewClient("http://localhost:34183", WithClientTimeout(12*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if client.httpClient.Timeout != 12*time.Second {
		t.Fatalf("timeout override: %s", client.httpClient.Timeout)
	}
}
