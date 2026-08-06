package agnt5

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestContextSleepYieldsTypedDurableSuspension(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.Metadata[durableSuspensionV1Capability] = "true"

	err := ctx.Sleep(2500*time.Millisecond, WithSleepKey("backoff"))
	var suspended *durableSleepSuspensionError
	if !errors.As(err, &suspended) || suspended.suspension == nil {
		t.Fatalf("sleep error = %#v", err)
	}
	if got := suspended.suspension; got.GetTimerKey() != "sleep:backoff" ||
		got.GetDelayMs() != 2500 || got.GetAttempt() != 1 ||
		string(got.GetFenceToken()) != "fence-1" {
		t.Fatalf("suspension = %#v", got)
	}
	if len(writer.beginRequests) != 1 ||
		writer.beginRequests[0].GetKind() != pb.ActivationKind_ACTIVATION_KIND_TIMER {
		t.Fatalf("begin requests = %#v", writer.beginRequests)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 0 {
		t.Fatalf("timer activation must remain nonterminal")
	}
}

func TestContextSleepResumesOnlyMatchingTimerActivation(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.Metadata[durableSuspensionV1Capability] = "true"
	ctx.invocation.Metadata["timer_key"] = "sleep:backoff"
	ctx.invocation.Metadata["activation_id"] = activationID(
		"project-1",
		"run-1",
		"",
		pb.ActivationKind_ACTIVATION_KIND_TIMER,
		"sleep:backoff",
	)

	if err := ctx.Sleep(time.Second, WithSleepKey("backoff")); err != nil {
		t.Fatalf("resume sleep: %v", err)
	}
	if len(writer.beginRequests) != 0 {
		t.Fatalf("resumed sleep began another activation: %#v", writer.beginRequests)
	}
}

func TestContextSleepZeroAndNegativeValidation(t *testing.T) {
	ctx := newActivationTestContext(&recordingActivationWriter{})
	ctx.invocation.Metadata[durableSuspensionV1Capability] = "true"
	if err := ctx.Sleep(0); err != nil {
		t.Fatalf("zero sleep: %v", err)
	}
	if err := ctx.Sleep(-time.Second); err == nil {
		t.Fatal("negative sleep succeeded")
	}
}

func TestDurableSleepContinuationRestoresCompletedSteps(t *testing.T) {
	continuation, err := json.Marshal(map[string]any{
		"completed_steps":         map[string]any{"step:load:0": map[string]any{"ok": true}},
		"workflow_correlation_id": "workflow-cid",
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := mergeDurableContinuation(map[string]string{
		"continuation_b64": base64.RawURLEncoding.EncodeToString(continuation),
	})
	if metadata["workflow_correlation_id"] != "workflow-cid" ||
		metadata["completed_steps"] == "" {
		t.Fatalf("resume metadata = %#v", metadata)
	}
}

func TestDispatchConvertsSleepSignalToWorkerSuspension(t *testing.T) {
	writer := &recordingActivationWriter{}
	worker := NewWorker(
		"svc",
		WithProjectID("project-1"),
		WithServiceVersion("v1"),
		WithDurableActivationArtifact("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="),
	)
	worker.checkpointWriter = writer
	if err := worker.applyProtocolNegotiation([]string{
		durableActivationV1Capability,
		durableSuspensionV1Capability,
	}, nil); err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	if err := RegisterWorkflow(worker, "workflow", func(ctx *Context, _ map[string]any) (any, error) {
		return nil, ctx.Sleep(2500*time.Millisecond, WithSleepKey("backoff"))
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	response := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		&pb.DispatchComponentRequest{
			InvocationId:  "run-1",
			ComponentName: "workflow",
			ComponentType: pb.ComponentType_COMPONENT_TYPE_WORKFLOW,
			InputData:     []byte(`{}`),
			LeaseId:       "lease-1",
			Metadata: map[string]string{
				"worker_session_id": "session-1",
			},
		},
	))
	if !response.GetSuccess() || response.GetEventType() != "workflow.paused" {
		t.Fatalf("response = %#v", response)
	}
	suspension := response.GetWorkerSuspension()
	if suspension == nil || suspension.GetTimerKey() != "sleep:backoff" ||
		suspension.GetDelayMs() != 2500 {
		t.Fatalf("worker suspension = %#v", suspension)
	}
}
