package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func captureCoreMetrics(t *testing.T) func() []map[string]any {
	t.Helper()
	t.Setenv("AGNT5_CORE_METRICS_LOGS", "1")
	f, err := os.CreateTemp(t.TempDir(), "metrics")
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = f
	t.Cleanup(func() { os.Stderr = previous; _ = f.Close() })
	return func() []map[string]any {
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		var observations []map[string]any
		for _, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(line, "AGNT5_CORE_METRIC ") {
				continue
			}
			var observation map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "AGNT5_CORE_METRIC ")), &observation); err != nil {
				t.Fatal(err)
			}
			observations = append(observations, observation)
		}
		return observations
	}
}

type coreMetricsPullClient struct {
	pb.EngineServiceClient
	polls      int
	completion chan struct{}
	ack        chan bool
}

func (c *coreMetricsPullClient) PollJob(ctx context.Context, _ *pb.PollJobRequest, _ ...grpc.CallOption) (*pb.PollJobResponse, error) {
	c.polls++
	if c.polls > 2 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	id := fmt.Sprintf("run-%d", c.polls)
	return &pb.PollJobResponse{Job: &pb.JobAssignment{
		JobId: id, RunId: id, ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
		ComponentName: "test", InputData: []byte(`{}`), Attempt: 1, LeaseId: "lease-" + id,
	}}, nil
}

func (c *coreMetricsPullClient) CompleteJob(ctx context.Context, _ *pb.CompleteJobRequest, _ ...grpc.CallOption) (*pb.CompleteJobResponse, error) {
	c.completion <- struct{}{}
	select {
	case ack := <-c.ack:
		return &pb.CompleteJobResponse{Acknowledged: ack}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCoreMetricsSlotStaysOccupiedThroughAcknowledgment(t *testing.T) {
	observations := captureCoreMetrics(t)
	w := NewWorker("metrics", WithWorkerID("worker"), WithProjectID("project"))
	if err := RegisterFunction(w, "test", func(*Context, map[string]any) (string, error) { return "ok", nil }); err != nil {
		t.Fatal(err)
	}
	client := &coreMetricsPullClient{completion: make(chan struct{}, 2), ack: make(chan bool)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var open, active, total atomic.Uint32
	total.Store(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.runPullSlot(ctx, client, "session", pullSlotConfig{minSlots: 1, maxSlots: 1, claimTimeoutMS: 300000}, 7,
			&open, &active, &total, make(chan error, 1), make(chan pullSlotEvent, 3))
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-client.completion:
	case <-ctx.Done():
		t.Fatal("first completion did not start")
	}
	first := observations()
	var events []string
	for _, o := range first {
		events = append(events, o["event"].(string))
	}
	if !reflect.DeepEqual(events, []string{"claimed", "started", "handler_finished"}) {
		t.Fatalf("before blocked acknowledgment: %v", events)
	}
	if active.Load() != 1 {
		t.Fatal("slot released before acknowledgment")
	}
	client.ack <- true
	select {
	case <-client.completion:
	case <-ctx.Done():
		t.Fatal("second assignment did not execute")
	}
	all := observations()
	events = nil
	for _, o := range all {
		events = append(events, o["event"].(string))
	}
	want := []string{"claimed", "started", "handler_finished", "acknowledged", "released", "claimed", "started", "handler_finished"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events: %v", events)
	}
	if all[0]["slot_id"] != all[5]["slot_id"] || all[0]["execution_id"] == all[5]["execution_id"] {
		t.Fatal("slot or execution identity was lost")
	}
	cancel()
	<-done
	for _, o := range observations() {
		if o["run_id"] == "run-2" && o["event"] == "acknowledged" {
			t.Fatal("cancelled completion was acknowledged")
		}
	}
}

func TestCoreMetricsReplayDoesNotCountBusinessWork(t *testing.T) {
	observations := captureCoreMetrics(t)
	w := NewWorker("metrics")
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.Context = w.coreMetrics.claim(ctx.Context, ctx.RunID(), 1)
	body := func(*Context) (string, error) { return "value", nil }
	if _, err := Step(ctx, "load", body); err != nil {
		t.Fatal(err)
	}
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome:      pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId: writer.completeRequests[0].GetActivationId(),
		ReplayResult: inlineActivationPayload([]byte(`"value"`)),
	}
	replay := newActivationTestContext(writer)
	replay.Context = w.coreMetrics.claim(replay.Context, replay.RunID(), 1)
	if _, err := Step(replay, "load", func(*Context) (string, error) {
		t.Fatal("replay executed business work")
		return "", nil
	}); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, o := range observations() {
		if o["event"] == "business" {
			count++
			if o["outcome"] != "success" || o["elapsed_ms"].(float64) < 0 {
				t.Fatalf("business observation: %v", o)
			}
		}
	}
	if count != 1 {
		t.Fatalf("business intervals = %d, want 1", count)
	}
}

type coreMetricsActivationClient struct {
	pb.EngineServiceClient
	calls int
}

func (c *coreMetricsActivationClient) BeginActivation(context.Context, *pb.BeginActivationRequest, ...grpc.CallOption) (*pb.BeginActivationResponse, error) {
	c.calls++
	if c.calls == 1 {
		return nil, status.Error(codes.Unavailable, "retry")
	}
	return &pb.BeginActivationResponse{Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE}, nil
}

func TestCoreMetricsRPCIncludesRetryBackoffOnce(t *testing.T) {
	observations := captureCoreMetrics(t)
	w := NewWorker("metrics")
	ctx := w.coreMetrics.claim(context.Background(), "run", 1)
	client := &coreMetricsActivationClient{}
	writer := &engineEventWriter{client: client}
	started := time.Now()
	if _, err := writer.BeginActivation(ctx, &pb.BeginActivationRequest{}); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	count := 0
	for _, o := range observations() {
		if o["event"] != "activation_rpc" {
			continue
		}
		count++
		if o["operation"] != "begin" || o["outcome"] != "execute" ||
			o["elapsed_ms"].(float64) < float64(elapsed.Milliseconds())-5 {
			t.Fatalf("retry interval: %v elapsed=%v", o, elapsed)
		}
	}
	if count != 1 || client.calls != 2 {
		t.Fatalf("intervals=%d calls=%d", count, client.calls)
	}
}

func TestCoreMetricsRejectedCompletionHasNoAcknowledgment(t *testing.T) {
	observations := captureCoreMetrics(t)
	w := NewWorker("metrics")
	ctx := w.coreMetrics.claim(context.Background(), "run", 1)
	client := &coreMetricsPullClient{completion: make(chan struct{}, 1), ack: make(chan bool, 1)}
	client.ack <- false
	if err := w.completePolledJobRequestWithin(ctx, client, &pb.CompleteJobRequest{}, time.Second); err == nil {
		t.Fatal("expected rejected completion")
	}
	for _, o := range observations() {
		if o["event"] == "acknowledged" {
			t.Fatal("false acknowledgment")
		}
	}
}

func TestCoreMetricsDisabledHasNoObserver(t *testing.T) {
	t.Setenv("AGNT5_CORE_METRICS_LOGS", "0")
	w := NewWorker("metrics")
	ctx := context.Background()
	if w.coreMetrics != nil || w.coreMetrics.claim(ctx, "run", 1) != ctx {
		t.Fatal("disabled telemetry allocated execution state")
	}
}
