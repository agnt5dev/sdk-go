package agnt5

import (
	"bytes"
	"context"
	"errors"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

type activationAwareModel struct {
	response  GenerateResponse
	err       error
	calls     int
	execution ActivationExecution
}

type activationStreamingTestModel struct {
	response GenerateResponse
	err      error
	calls    int
}

func (m *activationStreamingTestModel) Generate(
	context.Context,
	GenerateRequest,
) (GenerateResponse, error) {
	return GenerateResponse{}, errors.New("streaming model fell back to Generate")
}

func (m *activationStreamingTestModel) Stream(
	_ context.Context,
	_ GenerateRequest,
	emit func(ModelStreamChunk) error,
) (GenerateResponse, error) {
	m.calls++
	if err := emit(ModelStreamChunk{Type: ModelStreamMessageDelta, Content: "partial"}); err != nil {
		return GenerateResponse{}, err
	}
	return m.response, m.err
}

func (m *activationAwareModel) Generate(ctx context.Context, _ GenerateRequest) (GenerateResponse, error) {
	m.calls++
	m.execution, _ = ActivationFromContext(ctx)
	return m.response, m.err
}

func TestModelFinalUsesDurableActivationAndRecordsUsage(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	model := &activationAwareModel{response: GenerateResponse{
		ID:      "response-1",
		Model:   "openai/gpt-test",
		Content: "provider final",
		Usage: TokenUsage{
			InputTokens:  3,
			OutputTokens: 2,
			TotalTokens:  5,
		},
		FinishReason: "stop",
	}}

	response, err := ctx.Generate(model, GenerateRequest{
		Model:    "openai/gpt-test",
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if response.Content != "provider final" || model.calls != 1 {
		t.Fatalf("response = %#v calls = %d", response, model.calls)
	}
	if len(writer.beginRequests) != 1 || len(writer.completeRequests) != 1 {
		t.Fatalf("activation writes = begin %d complete %d", len(writer.beginRequests), len(writer.completeRequests))
	}
	begin := writer.beginRequests[0]
	if begin.GetKind() != pb.ActivationKind_ACTIVATION_KIND_MODEL {
		t.Fatalf("kind = %v", begin.GetKind())
	}
	if begin.GetStableKey() != "model:openai/gpt-test:0" {
		t.Fatalf("stable key = %q", begin.GetStableKey())
	}
	if begin.GetRecoveryPolicy() != pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_UNKNOWN_OUTCOME {
		t.Fatalf("recovery policy = %v", begin.GetRecoveryPolicy())
	}
	expectedID := activationID(
		begin.GetProjectId(),
		begin.GetRunId(),
		begin.GetParentActivationId(),
		begin.GetKind(),
		begin.GetStableKey(),
	)
	if model.execution.ActivationID != expectedID || model.execution.IdempotencyKey != "agnt5:"+expectedID {
		t.Fatalf("execution authority = %#v", model.execution)
	}
	usage := writer.completeRequests[0].GetUsage()
	if usage.GetTokensIn() != 3 || usage.GetTokensOut() != 2 || usage.GetModel() != "openai/gpt-test" {
		t.Fatalf("activation usage = %#v", usage)
	}
	if _, ok := ctx.Activation(); ok {
		t.Fatal("activation authority leaked outside model call")
	}
}

func TestAcceptedModelFinalReplaysWithoutProviderCall(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	stableKey := "model:openai/gpt-test:0"
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId: activationID(
			"project-1",
			"run-1",
			"",
			pb.ActivationKind_ACTIVATION_KIND_MODEL,
			stableKey,
		),
		Attempt:               1,
		AcceptedJournalOffset: 9,
		ReplayResult: inlineActivationPayload([]byte(
			`{"id":"response-replay","model":"openai/gpt-test","content":"replayed final","usage":{"total_tokens":5},"finish_reason":"stop"}`,
		)),
	}
	model := &activationAwareModel{}

	response, err := ctx.Generate(model, GenerateRequest{
		Model:    "openai/gpt-test",
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if response.Content != "replayed final" || response.Usage.TotalTokens != 5 {
		t.Fatalf("replayed response = %#v", response)
	}
	if model.calls != 0 {
		t.Fatalf("provider calls = %d", model.calls)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 0 {
		t.Fatal("replay emitted a terminal activation write")
	}
}

func TestModelRecoveryPolicyControlsRetryableFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		policy    RecoveryPolicy
		retryable bool
	}{
		{name: "default unknown", policy: "", retryable: false},
		{name: "idempotent", policy: RecoveryPolicyIdempotentRetry, retryable: true},
		{name: "durable steps", policy: RecoveryPolicyDurableSteps, retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &recordingActivationWriter{}
			ctx := newActivationTestContext(writer)
			model := &activationAwareModel{err: errors.New("transport interrupted")}

			_, err := ctx.Generate(model, GenerateRequest{
				Model:          "openai/gpt-test",
				Messages:       []Message{{Role: MessageRoleUser, Content: "hello"}},
				RecoveryPolicy: test.policy,
			})
			if err == nil {
				t.Fatal("expected model failure")
			}
			if len(writer.failRequests) != 1 {
				t.Fatalf("failure writes = %d", len(writer.failRequests))
			}
			failure := writer.failRequests[0]
			if failure.GetRetryable() != test.retryable {
				t.Fatalf("retryable = %v, want %v", failure.GetRetryable(), test.retryable)
			}
			if failure.GetExternalOutcomeCertainty() != pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN {
				t.Fatalf("certainty = %v", failure.GetExternalOutcomeCertainty())
			}
		})
	}
}

func TestStreamingModelFinalUsesDurableActivation(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.IsStreaming = true
	model := &activationStreamingTestModel{response: GenerateResponse{
		ID:           "response-stream-1",
		Model:        "openai/gpt-test",
		Content:      "accepted final",
		FinishReason: "stop",
		Usage: TokenUsage{
			InputTokens:  3,
			OutputTokens: 2,
			TotalTokens:  5,
		},
	}}

	response, err := ctx.Generate(model, GenerateRequest{
		Model:    "openai/gpt-test",
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if response.Content != "accepted final" || model.calls != 1 {
		t.Fatalf("response = %#v calls = %d", response, model.calls)
	}
	if len(writer.completeRequests) != 1 {
		t.Fatalf("completion writes = %d", len(writer.completeRequests))
	}
	completed := writer.completeRequests[0]
	if completed.GetUsage().GetTokensIn() != 3 || completed.GetUsage().GetTokensOut() != 2 {
		t.Fatalf("usage = %#v", completed.GetUsage())
	}
	if len(completed.GetEvidence()) != 1 || completed.GetEvidence()[0].GetEvidenceType() != "model_provider_terminal_v1" {
		t.Fatalf("evidence = %#v", completed.GetEvidence())
	}
}

func TestAcceptedStreamingModelFinalReplaysWithoutProviderCall(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.IsStreaming = true
	stableKey := "model:openai/gpt-test:0"
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId: activationID(
			"project-1",
			"run-1",
			"",
			pb.ActivationKind_ACTIVATION_KIND_MODEL,
			stableKey,
		),
		Attempt:               1,
		AcceptedJournalOffset: 9,
		ReplayResult: inlineActivationPayload([]byte(
			`{"id":"response-replay","model":"openai/gpt-test","content":"replayed final","usage":{"total_tokens":5},"finish_reason":"stop"}`,
		)),
	}
	model := &activationStreamingTestModel{}

	response, err := ctx.Generate(model, GenerateRequest{
		Model:    "openai/gpt-test",
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if response.Content != "replayed final" || model.calls != 0 {
		t.Fatalf("response = %#v calls = %d", response, model.calls)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 0 {
		t.Fatal("replay emitted a terminal activation write")
	}
}

func TestInterruptedModelStreamRecordsBoundedEvidence(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.IsStreaming = true
	model := &activationStreamingTestModel{err: errors.New("provider stream interrupted")}

	_, err := ctx.Generate(model, GenerateRequest{
		Model:    "openai/gpt-test",
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err == nil || err.Error() != "provider stream interrupted" {
		t.Fatalf("error = %v", err)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 1 {
		t.Fatalf("terminal writes = complete %d fail %d", len(writer.completeRequests), len(writer.failRequests))
	}
	failure := writer.failRequests[0]
	if failure.GetErrorCode() != "MODEL_STREAM_INTERRUPTED" {
		t.Fatalf("error code = %q", failure.GetErrorCode())
	}
	if len(failure.GetEvidence()) != 1 {
		t.Fatalf("evidence = %#v", failure.GetEvidence())
	}
	evidence := failure.GetEvidence()[0]
	payload := evidence.GetPayload().GetInlineData()
	if evidence.GetEvidenceType() != "model_stream_interruption_v1" ||
		!bytes.Contains(payload, []byte(`"partial_chunks":1`)) ||
		!bytes.Contains(payload, []byte(`"partial_utf8_bytes":7`)) ||
		bytes.Contains(payload, []byte(`"partial"`)) {
		t.Fatalf("evidence payload = %s", payload)
	}
}
