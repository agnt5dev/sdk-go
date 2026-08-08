package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestClientBatchSubmitsPythonCompatibleItems(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: %s", r.Method)
		}
		if r.URL.Path != "/v1/workflows/process/batch" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("X-TENANT-ID") != "tenant-batch" {
			t.Fatalf("tenant: %q", r.Header.Get("X-TENANT-ID"))
		}
		if r.Header.Get("Idempotency-Key") != "batch-idem" {
			t.Fatalf("idempotency header: %q", r.Header.Get("Idempotency-Key"))
		}
		var body struct {
			Items []struct {
				Input    map[string]int    `json:"input"`
				Index    int               `json:"index"`
				ItemID   string            `json:"item_id"`
				Metadata map[string]string `json:"metadata"`
			} `json:"items"`
			Config struct {
				MaxConcurrency       int   `json:"max_concurrency"`
				ContinueOnFailure    bool  `json:"continue_on_failure"`
				BatchTimeoutMS       int64 `json:"batch_timeout_ms"`
				DefaultItemTimeoutMS int64 `json:"default_item_timeout_ms"`
			} `json:"config"`
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Items) != 2 {
			t.Fatalf("items: %#v", body.Items)
		}
		if body.Items[0].Input["id"] != 1 || body.Items[0].Index != 0 {
			t.Fatalf("first item: %#v", body.Items[0])
		}
		if body.Items[1].Input["id"] != 2 || body.Items[1].Index != 7 ||
			body.Items[1].ItemID != "item-2" || body.Items[1].Metadata["source"] != "api" {
			t.Fatalf("second item: %#v", body.Items[1])
		}
		if body.Config.MaxConcurrency != 5 || body.Config.ContinueOnFailure ||
			body.Config.BatchTimeoutMS != 60_000 || body.Config.DefaultItemTimeoutMS != 10_000 {
			t.Fatalf("config: %#v", body.Config)
		}
		if body.Metadata["batch_group"] != "nightly" {
			t.Fatalf("metadata: %#v", body.Metadata)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{
			"batch_id":"batch-1",
			"status":"started",
			"run_ids":["run-1","run-2"],
			"total_items":2
		}`))
	})

	result, err := client.Batch(context.Background(), "process", []any{
		map[string]int{"id": 1},
		NewBatchItem(map[string]int{"id": 2},
			WithBatchItemIndex(7),
			WithBatchItemID("item-2"),
			WithBatchItemMetadata(map[string]string{"source": "api"}),
		),
	},
		WithBatchComponentType(ComponentTypeWorkflow),
		WithBatchTenant("tenant-batch"),
		WithBatchMaxConcurrency(5),
		WithBatchContinueOnFailure(false),
		WithBatchTimeoutMS(60_000),
		WithBatchDefaultItemTimeoutMS(10_000),
		WithBatchMetadata(map[string]string{"batch_group": "nightly"}),
		WithBatchIdempotencyKey("batch-idem"),
	)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if result.BatchID != "batch-1" || result.Status != BatchStatusStarted ||
		result.TotalItems != 2 || len(result.RunIDs) != 2 || result.Stats.TotalItems != 2 {
		t.Fatalf("result: %#v", result)
	}
}

func TestClientBatchCanSendRawItems(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Items []map[string]int `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Items) != 2 || body.Items[0]["id"] != 1 || body.Items[1]["id"] != 2 {
			t.Fatalf("raw items: %#v", body.Items)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"batch_id":"batch-raw","status":"started","total_items":2}`))
	})

	result, err := client.Batch(context.Background(), "process", []map[string]int{{"id": 1}, {"id": 2}}, WithBatchRawItems())
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if result.BatchID != "batch-raw" {
		t.Fatalf("result: %#v", result)
	}
}

func TestClientBatchParsesResultsAndHelpers(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{
			"batch_id":"batch-2",
			"status":"partial_failure",
			"results":[
				{"index":1,"run_id":"run-2","status":"failed","error":{"code":"BOOM","message":"boom"}},
				{"index":0,"run_id":"run-1","status":"completed","output":{"value":1},"duration_ms":12}
			],
			"stats":{"total_items":2,"completed_items":1,"failed_items":1,"duration_ms":20}
		}`))
	})

	result, err := client.Batch(context.Background(), "process", []any{map[string]int{"id": 1}})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if !result.IsPartialFailure() || len(result.FailedItems()) != 1 {
		t.Fatalf("result: %#v", result)
	}
	outputs := result.Outputs()
	if len(outputs) != 2 || string(outputs[0]) != `{"value":1}` || string(outputs[1]) != "" {
		t.Fatalf("outputs: %#v", outputs)
	}
	successful := result.SuccessfulOutputs()
	if len(successful) != 1 || string(successful[0]) != `{"value":1}` {
		t.Fatalf("successful outputs: %#v", successful)
	}
	var decoded struct {
		Value int `json:"value"`
	}
	if err := result.Results[1].DecodeOutput(&decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if decoded.Value != 1 {
		t.Fatalf("decoded: %#v", decoded)
	}
}

func TestClientGetBatchStatusSendsIncludeResults(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method: %s", r.Method)
		}
		if r.URL.Path != "/v1/batches/batch-3" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("include_results") != "false" {
			t.Fatalf("query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{
			"batch_id":"batch-3",
			"found":true,
			"status":"running",
			"stats":{"total_items":3,"pending_items":2}
		}`))
	})

	status, err := client.GetBatchStatus(context.Background(), "batch-3", false, time.Second)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.Found || !status.IsRunning() || status.Stats.PendingItems != 2 {
		t.Fatalf("status: %#v", status)
	}
}

func TestClientCancelBatchSendsReason(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method: %s", r.Method)
		}
		if r.URL.Path != "/v1/batches/batch-4" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		reason, _ := url.QueryUnescape(r.URL.Query().Get("reason"))
		if reason != "user requested" {
			t.Fatalf("reason: %q", reason)
		}
		_, _ = w.Write([]byte(`{
			"batch_id":"batch-4",
			"status":"cancelled",
			"cancelled_items":2,
			"completed_items":1
		}`))
	})

	cancelled, err := client.CancelBatch(context.Background(), "batch-4", "user requested", time.Second)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.Status != BatchStatusCancelled || cancelled.CancelledItems != 2 || cancelled.CompletedItems != 1 {
		t.Fatalf("cancelled: %#v", cancelled)
	}
}

func TestClientBatchStreamParsesSSEAndStopsAtTerminal(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: %s", r.Method)
		}
		if r.URL.Path != "/v1/functions/process/batch/stream" {
			t.Fatalf("path: %s", r.URL.Path)
		}
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("accept: %q", r.Header.Get("Accept"))
		}
		var body struct {
			Items []struct {
				Input map[string]int `json:"input"`
				Index int            `json:"index"`
			} `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Items) != 2 || body.Items[0].Index != 0 || body.Items[1].Input["id"] != 2 {
			t.Fatalf("items: %#v", body.Items)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: batch.created\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"batch.created\",\"run_id\":\"batch-5\",\"data\":{\"total_items\":2},\"metadata\":{\"batch_id\":\"batch-5\"}}\n\n")
		_, _ = fmt.Fprint(w, "event: run.completed\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"run.completed\",\"run_id\":\"run-5\",\"data\":{\"output_data\":{\"ok\":true}},\"metadata\":{\"batch_id\":\"batch-5\",\"batch_index\":\"0\"}}\n\n")
		_, _ = fmt.Fprint(w, "event: batch.completed\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"batch.completed\",\"run_id\":\"batch-5\",\"data\":{},\"metadata\":{\"batch_id\":\"batch-5\"}}\n\n")
		_, _ = fmt.Fprint(w, "event: run.completed\n")
		_, _ = fmt.Fprint(w, "data: {\"event_type\":\"run.completed\",\"run_id\":\"late\",\"data\":{},\"metadata\":{\"batch_id\":\"batch-5\"}}\n\n")
	})

	var events []BatchStreamEvent
	err := client.BatchStream(context.Background(), "process", []map[string]int{{"id": 1}, {"id": 2}}, func(event BatchStreamEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("batch stream: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("events: %#v", events)
	}
	if events[0].EventType != "batch.created" || events[0].BatchID != "batch-5" {
		t.Fatalf("first event: %#v", events[0])
	}
	if events[1].RunID != "run-5" || string(events[1].Data) != `{"output_data":{"ok":true}}` {
		t.Fatalf("run event: %#v", events[1])
	}
	if events[2].EventType != "batch.completed" {
		t.Fatalf("terminal event: %#v", events[2])
	}
}

func TestClientBatchStreamReturnsPlainTextSSEError(t *testing.T) {
	client := newHTTPTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: error\n")
		_, _ = fmt.Fprint(w, "data: stream error: tail failed\n\n")
	})

	err := client.BatchStream(context.Background(), "process", []any{map[string]int{"id": 1}}, func(BatchStreamEvent) error {
		t.Fatal("handler should not be called")
		return nil
	})
	var runErr *RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("expected RunError, got %T %v", err, err)
	}
	if runErr.Message != "stream error: tail failed" {
		t.Fatalf("run error: %#v", runErr)
	}
}
