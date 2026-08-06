package agnt5

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingActivationWriter struct {
	beginRequests    []*pb.BeginActivationRequest
	completeRequests []*pb.CompleteActivationRequest
	failRequests     []*pb.FailActivationRequest
	beginResponse    *pb.BeginActivationResponse
	completeResponse *pb.CompleteActivationResponse
	failResponse     *pb.FailActivationResponse
	beginErr         error
	completeErr      error
	failErr          error
}

func (w *recordingActivationWriter) Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	return nil, errors.New("legacy checkpoint must not be used after durable_activation_v1 negotiation")
}

func (w *recordingActivationWriter) BeginActivation(_ context.Context, request *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	w.beginRequests = append(w.beginRequests, request)
	if w.beginErr != nil {
		return nil, w.beginErr
	}
	if w.beginResponse != nil {
		return w.beginResponse, nil
	}
	return &pb.BeginActivationResponse{
		Outcome:               pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE,
		ActivationId:          activationID(request.GetProjectId(), request.GetRunId(), request.GetParentActivationId(), request.GetKind(), request.GetStableKey()),
		Attempt:               1,
		FenceToken:            []byte("fence-1"),
		AcceptedJournalOffset: 11,
	}, nil
}

func (w *recordingActivationWriter) CompleteActivation(_ context.Context, request *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	w.completeRequests = append(w.completeRequests, request)
	if w.completeErr != nil {
		return nil, w.completeErr
	}
	if w.completeResponse != nil {
		return w.completeResponse, nil
	}
	return &pb.CompleteActivationResponse{
		Accepted:              true,
		ActivationId:          request.GetActivationId(),
		Attempt:               request.GetAttempt(),
		AcceptedJournalOffset: 12,
	}, nil
}

func (w *recordingActivationWriter) FailActivation(_ context.Context, request *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	w.failRequests = append(w.failRequests, request)
	if w.failErr != nil {
		return nil, w.failErr
	}
	if w.failResponse != nil {
		return w.failResponse, nil
	}
	return &pb.FailActivationResponse{
		Accepted:              true,
		ActivationId:          request.GetActivationId(),
		Attempt:               request.GetAttempt(),
		Status:                pb.ActivationStatus_ACTIVATION_STATUS_UNKNOWN_OUTCOME,
		AcceptedJournalOffset: 12,
	}, nil
}

func newActivationTestContext(writer *recordingActivationWriter) *Context {
	return newContext(context.Background(), Invocation{
		ID:            "invocation-1",
		RunID:         "run-1",
		ComponentName: "workflow",
		ComponentType: ComponentTypeWorkflow,
		LeaseID:       "lease-1",
		Metadata: map[string]string{
			durableActivationV1Capability:       "true",
			activationArtifactSHA256Metadata:    "0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg=",
			activationDefinitionVersionMetadata: "v1",
			activationDefinitionConfigMetadata:  `["object",[]]`,
			"worker_session_id":                 "session-1",
		},
	}, writer, "project-1")
}

func TestDurableActivationVectorsMatchFrozenContract(t *testing.T) {
	canonical, err := canonicalActivationValue(map[string]any{
		"name":  "alpha",
		"count": int64(2),
	})
	if err != nil {
		t.Fatalf("canonical value: %v", err)
	}
	if got, want := string(canonical), `["object",[["count",["i64","2"]],["name",["string","alpha"]]]]`; got != want {
		t.Fatalf("canonical value = %s, want %s", got, want)
	}
	inputDigest := sha256Bytes(canonical)
	if got, want := base64.StdEncoding.EncodeToString(inputDigest), "+6akLLE8ses5QeK62PHHkobScg7gWMdae1Zh105nCzM="; got != want {
		t.Fatalf("input digest = %s, want %s", got, want)
	}
	artifact, err := decodeSHA256("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg=")
	if err != nil {
		t.Fatalf("artifact digest: %v", err)
	}
	definition := activationDefinitionDigest(artifact, "workflow", "v1", []byte(`["object",[]]`))
	if got, want := base64.StdEncoding.EncodeToString(definition), "iTziD0lZ9kXRtq7RUj58/nzuTDQQtdgYp+MDNrAGVmw="; got != want {
		t.Fatalf("definition digest = %s, want %s", got, want)
	}
	if got, want := activationID("project-1", "run-1", "parent-1", pb.ActivationKind_ACTIVATION_KIND_STEP, "step/load"), "actv1_9LU0V32sQX2U3CaQSCW37t-WWSvBAe04qTWqTD6mN-w"; got != want {
		t.Fatalf("activation ID = %s, want %s", got, want)
	}
}

func TestStepExecutesOnlyAfterActivationAdmissionAndCompletionAck(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	called := false

	got, err := Step(ctx, "load", func(context.Context) (string, error) {
		called = true
		if len(writer.beginRequests) != 1 {
			t.Fatalf("user code ran before BeginActivation admission")
		}
		return "value", nil
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !called || got != "value" {
		t.Fatalf("called=%t output=%q", called, got)
	}
	if len(writer.completeRequests) != 1 || len(writer.failRequests) != 0 {
		t.Fatalf("complete=%d fail=%d", len(writer.completeRequests), len(writer.failRequests))
	}
	begin := writer.beginRequests[0]
	if begin.GetStableKey() != "step:load:0" || len(begin.GetInputDigest()) != 32 || len(begin.GetDefinitionDigest()) != 32 {
		t.Fatalf("begin request: %#v", begin)
	}
	completed := writer.completeRequests[0]
	if string(completed.GetOutput().GetInlineData()) != `"value"` || len(completed.GetOutputDigest()) != 32 {
		t.Fatalf("complete request: %#v", completed)
	}
	events := ctx.Events()
	if gotTypes := eventTypes(events); !reflect.DeepEqual(gotTypes, []string{"workflow.step.started", "workflow.step.completed"}) {
		t.Fatalf("events = %#v", gotTypes)
	}
	data := events[1].Data.(map[string]any)
	if data["activation_id"] == "" || data["accepted_journal_offset"] != uint64(12) {
		t.Fatalf("completion event data: %#v", data)
	}
}

func TestStepDoesNotReturnOrMemoizeBeforeCompletionAck(t *testing.T) {
	writer := &recordingActivationWriter{
		completeErr: newActivationError(ActivationErrorUnknownOutcome, "completion acknowledgement was lost", "", 0, context.DeadlineExceeded),
	}
	ctx := newActivationTestContext(writer)

	got, err := Step(ctx, "load", func(context.Context) (string, error) { return "value", nil })
	if !errors.Is(err, ErrActivationUnknownOutcome) || got != "" {
		t.Fatalf("output=%q error=%v", got, err)
	}
	if _, ok := ctx.completedStepPayload("step:load:0"); ok {
		t.Fatal("step was memoized before completion acknowledgement")
	}
	if gotTypes := eventTypes(ctx.Events()); !reflect.DeepEqual(gotTypes, []string{"workflow.step.started"}) {
		t.Fatalf("events = %#v", gotTypes)
	}
}

func TestStepWaitsForAcceptedFailureReceipt(t *testing.T) {
	userErr := errors.New("boom")
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)

	_, err := Step(ctx, "load", func(context.Context) (string, error) { return "", userErr })
	if !errors.Is(err, userErr) {
		t.Fatalf("step error = %v", err)
	}
	if len(writer.failRequests) != 1 || len(writer.completeRequests) != 0 {
		t.Fatalf("fail=%d complete=%d", len(writer.failRequests), len(writer.completeRequests))
	}
	failed := writer.failRequests[0]
	if failed.GetExternalOutcomeCertainty() != pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN ||
		failed.GetErrorCode() != "STEP_FAILED" {
		t.Fatalf("failure request: %#v", failed)
	}

	writer = &recordingActivationWriter{
		failErr: newActivationError(ActivationErrorUnknownOutcome, "failure acknowledgement was lost", "", 0, context.DeadlineExceeded),
	}
	ctx = newActivationTestContext(writer)
	_, err = Step(ctx, "load", func(context.Context) (string, error) { return "", userErr })
	if !errors.Is(err, ErrActivationUnknownOutcome) {
		t.Fatalf("lost failure acknowledgement error = %v", err)
	}
}

func TestStepReplaySkipsUserCode(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	activationID := activationID("project-1", "run-1", "", pb.ActivationKind_ACTIVATION_KIND_STEP, "step:load:0")
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome:               pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId:          activationID,
		Attempt:               1,
		ReplayResult:          inlineActivationPayload([]byte(`{"message":"cached"}`)),
		AcceptedJournalOffset: 42,
	}
	called := false

	got, err := Step(ctx, "load", func(context.Context) (greetOutput, error) {
		called = true
		return greetOutput{}, nil
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if called || got.Message != "cached" || len(writer.completeRequests) != 0 {
		t.Fatalf("called=%t output=%#v complete=%d", called, got, len(writer.completeRequests))
	}
}

func TestStepRefusesNonExecuteDecisions(t *testing.T) {
	tests := []struct {
		name    string
		outcome pb.BeginActivationOutcome
		modify  func(*pb.BeginActivationResponse)
		target  error
	}{
		{name: "wait", outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_WAIT, modify: func(response *pb.BeginActivationResponse) {
			response.Wait = &pb.ActivationWaitReceipt{Attempt: 1}
		}, target: ErrActivationContended},
		{name: "conflict", outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CONFLICT, modify: func(response *pb.BeginActivationResponse) {
			response.Conflict = &pb.ActivationConflictReceipt{Message: "input changed"}
		}, target: ErrNondeterministicReplay},
		{name: "cancelled", outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_CANCELLED, target: ErrActivationCancelled},
		{name: "unknown", outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_UNKNOWN_OUTCOME, modify: func(response *pb.BeginActivationResponse) {
			response.UnknownOutcome = &pb.ActivationUnknownOutcomeReceipt{ErrorCode: "EXTERNAL_EFFECT_UNKNOWN"}
		}, target: ErrActivationUnknownOutcome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := &recordingActivationWriter{}
			ctx := newActivationTestContext(writer)
			response := &pb.BeginActivationResponse{
				Outcome:      test.outcome,
				ActivationId: activationID("project-1", "run-1", "", pb.ActivationKind_ACTIVATION_KIND_STEP, "step:load:0"),
				Attempt:      1,
			}
			if test.modify != nil {
				test.modify(response)
			}
			writer.beginResponse = response
			called := false
			_, err := Step(ctx, "load", func(context.Context) (string, error) {
				called = true
				return "", nil
			})
			if !errors.Is(err, test.target) || called {
				t.Fatalf("error=%v called=%t", err, called)
			}
		})
	}
}

func TestStepWithKeyUsesExplicitStableIdentity(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	_, err := StepWithKey(ctx, "load", "item-42", func(*Context) (string, error) { return "value", nil })
	if err != nil {
		t.Fatalf("step with key: %v", err)
	}
	if got := writer.beginRequests[0].GetStableKey(); got != "step:load:item-42" {
		t.Fatalf("stable key = %q", got)
	}
}

func TestActivationRPCErrorUsesTypedRuntimeDetail(t *testing.T) {
	grpcStatus, err := status.New(codes.FailedPrecondition, "text is not the contract").WithDetails(
		&pb.ActivationErrorDetail{
			Code:         pb.ActivationErrorCode_ACTIVATION_ERROR_CODE_NONDETERMINISTIC_REPLAY,
			ActivationId: "actv1_conflict",
			Attempt:      2,
			Message:      "definition changed",
		},
	)
	if err != nil {
		t.Fatalf("status details: %v", err)
	}

	mapped := activationRPCError("BeginActivation", grpcStatus.Err())
	var activationErr *ActivationError
	if !errors.As(mapped, &activationErr) || activationErr.Code != ActivationErrorNondeterministicReplay ||
		activationErr.ActivationID != "actv1_conflict" || activationErr.Attempt != 2 {
		t.Fatalf("mapped error = %#v", mapped)
	}
}

func TestWorkerNegotiatesDurableActivationCapability(t *testing.T) {
	preferred := NewWorker("svc", WithDurableActivationArtifact("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="))
	supported, required := preferred.protocolRegistrationCapabilities()
	if !reflect.DeepEqual(supported, []string{
		durableActivationV1Capability,
		durableSuspensionV1Capability,
	}) || len(required) != 0 {
		t.Fatalf("preferred protocols: supported=%v required=%v", supported, required)
	}
	if err := preferred.applyProtocolNegotiation(nil, nil); err != nil {
		t.Fatalf("preferred old runtime: %v", err)
	}
	status := preferred.DurableActivationStatus()
	if status.Enabled || !status.Degraded || status.Reason == "" {
		t.Fatalf("preferred old-runtime status: %#v", status)
	}
	if err := preferred.applyProtocolNegotiation([]string{durableActivationV1Capability}, nil); err != nil {
		t.Fatalf("preferred v1 runtime: %v", err)
	}
	status = preferred.DurableActivationStatus()
	if !status.Enabled || status.Degraded {
		t.Fatalf("preferred v1 status: %#v", status)
	}

	requiredWorker := NewWorker(
		"svc",
		WithDurableActivationMode(DurableActivationRequired),
		WithDurableActivationArtifact("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="),
	)
	supported, required = requiredWorker.protocolRegistrationCapabilities()
	if !reflect.DeepEqual(supported, []string{
		durableActivationV1Capability,
		durableSuspensionV1Capability,
	}) ||
		!reflect.DeepEqual(required, []string{durableActivationV1Capability}) {
		t.Fatalf("required protocols: supported=%v required=%v", supported, required)
	}
	if err := requiredWorker.applyProtocolNegotiation(nil, nil); !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("required old runtime error = %v", err)
	}
	if err := requiredWorker.applyProtocolNegotiation([]string{durableActivationV1Capability}, nil); err != nil {
		t.Fatalf("required v1 runtime: %v", err)
	}

	missingIdentity := NewWorker("svc", WithDurableActivationMode(DurableActivationRequired))
	if err := missingIdentity.applyProtocolNegotiation([]string{durableActivationV1Capability}, nil); !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("required missing definition identity error = %v", err)
	}
}

func TestWorkerConfiguresDurableActivationArtifactFromEnvironment(t *testing.T) {
	artifact := "0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="
	t.Setenv(envActivationArtifactSHA256, artifact)

	worker := NewWorker("svc")
	if got := worker.Metadata()[activationArtifactSHA256Metadata]; got != artifact {
		t.Fatalf("activation artifact metadata = %q, want %q", got, artifact)
	}
}

func TestNegotiatedWorkerInvocationCarriesDefinitionIdentity(t *testing.T) {
	worker := NewWorker(
		"svc",
		WithServiceVersion("v1"),
		WithProjectID("project-1"),
		WithDurableActivationArtifact("0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg="),
	)
	if err := RegisterWorkflow(worker, "workflow", func(*Context, map[string]any) (string, error) {
		return "ok", nil
	}, WithComponentConfig(map[string]string{"model": "deterministic"})); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := worker.applyProtocolNegotiation([]string{durableActivationV1Capability}, nil); err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	component, ok := worker.registry.Get("workflow")
	if !ok {
		t.Fatal("workflow component missing")
	}
	invocation := worker.withActivationMetadata(Invocation{
		ID:            "inv-1",
		RunID:         "run-1",
		ComponentName: "workflow",
		ComponentType: ComponentTypeWorkflow,
	}, component)
	if invocation.Metadata[durableActivationV1Capability] != "true" ||
		invocation.Metadata[activationDefinitionVersionMetadata] != "v1" ||
		invocation.Metadata[activationArtifactSHA256Metadata] == "" ||
		invocation.Metadata[activationDefinitionConfigMetadata] == "" {
		t.Fatalf("activation metadata: %#v", invocation.Metadata)
	}
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}
