package agnt5

import (
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
