package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestAgentToolUsesDurableActivationAndExposesDownstreamKey(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	var execution ActivationExecution
	tool, err := NewTool(
		"charge",
		func(handlerCtx context.Context, input map[string]any) (any, error) {
			var ok bool
			execution, ok = ActivationFromContext(handlerCtx)
			if !ok {
				t.Fatal("tool handler did not receive activation authority")
			}
			return map[string]any{"amount": input["amount"]}, nil
		},
		WithToolRecoveryPolicy(RecoveryPolicyIdempotentRetry),
	)
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}

	result, err := invokeAgentTool(ctx, tool, "provider-call-7", map[string]any{"amount": int64(42)})
	if err != nil {
		t.Fatalf("invokeAgentTool: %v", err)
	}
	if len(writer.beginRequests) != 1 || len(writer.completeRequests) != 1 {
		t.Fatalf("activation writes = begin %d complete %d", len(writer.beginRequests), len(writer.completeRequests))
	}
	begin := writer.beginRequests[0]
	if begin.GetKind() != pb.ActivationKind_ACTIVATION_KIND_TOOL {
		t.Fatalf("kind = %v", begin.GetKind())
	}
	if begin.GetStableKey() != "tool:charge:provider-call-7" {
		t.Fatalf("stable key = %q", begin.GetStableKey())
	}
	if begin.GetRecoveryPolicy() != pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_IDEMPOTENT_RETRY {
		t.Fatalf("recovery policy = %v", begin.GetRecoveryPolicy())
	}
	expectedActivationID := activationID(
		begin.GetProjectId(),
		begin.GetRunId(),
		begin.GetParentActivationId(),
		begin.GetKind(),
		begin.GetStableKey(),
	)
	if execution.ActivationID != expectedActivationID || execution.Attempt != 1 {
		t.Fatalf("execution authority = %#v", execution)
	}
	if execution.IdempotencyKey != "agnt5:"+expectedActivationID {
		t.Fatalf("idempotency key = %q", execution.IdempotencyKey)
	}
	if _, ok := ctx.Activation(); ok {
		t.Fatal("activation authority leaked outside tool handler")
	}
	if got := result.(map[string]any)["amount"]; got != int64(42) {
		t.Fatalf("result amount = %#v", got)
	}
}

func TestAgentToolRecoveryPolicyControlsRetryableFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		policy    RecoveryPolicy
		retryable bool
	}{
		{name: "ordinary", policy: RecoveryPolicyUnknownOutcome, retryable: false},
		{name: "idempotent", policy: RecoveryPolicyIdempotentRetry, retryable: true},
		{name: "durable steps", policy: RecoveryPolicyDurableSteps, retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &recordingActivationWriter{}
			ctx := newActivationTestContext(writer)
			tool, err := NewTool(
				"effect",
				func(context.Context, map[string]any) (any, error) {
					return nil, errors.New("transport interrupted")
				},
				WithToolRecoveryPolicy(test.policy),
			)
			if err != nil {
				t.Fatalf("NewTool: %v", err)
			}
			if _, err := invokeAgentTool(ctx, tool, "call-1", nil); err == nil {
				t.Fatal("expected tool error")
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

func TestAgentToolReplaysWithoutCallingHandler(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	stableKey := "tool:lookup:provider-call-1"
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId: activationID(
			"project-1",
			"run-1",
			"",
			pb.ActivationKind_ACTIVATION_KIND_TOOL,
			stableKey,
		),
		Attempt:               1,
		AcceptedJournalOffset: 9,
		ReplayResult:          inlineActivationPayload([]byte(`{"value":"cached"}`)),
	}
	tool, err := NewTool("lookup", func(context.Context, map[string]any) (any, error) {
		t.Fatal("replayed tool executed its handler")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}

	result, err := invokeAgentTool(ctx, tool, "provider-call-1", map[string]any{"key": "a"})
	if err != nil {
		t.Fatalf("invokeAgentTool: %v", err)
	}
	if result.(map[string]any)["value"] != "cached" {
		t.Fatalf("replayed result = %#v", result)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 0 {
		t.Fatal("replay emitted a terminal write")
	}
}

func TestToolDurabilityCanBeDisabledAndPoliciesAreValidated(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	tool, err := NewTool(
		"approval",
		func(context.Context, map[string]any) (any, error) { return "paused", nil },
		WithoutDurableToolActivation(),
	)
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}
	if _, err := invokeAgentTool(ctx, tool, "call-1", nil); err != nil {
		t.Fatalf("invoke disabled tool: %v", err)
	}
	if len(writer.beginRequests) != 0 {
		t.Fatalf("disabled tool began %d activations", len(writer.beginRequests))
	}

	if _, err := NewTool(
		"bad",
		func(context.Context, map[string]any) (any, error) { return nil, nil },
		WithToolRecoveryPolicy("blind_retry"),
	); err == nil {
		t.Fatal("invalid recovery policy was accepted")
	}
}

type toolCallingTestModel struct {
	calls int
}

func (m *toolCallingTestModel) Generate(context.Context, GenerateRequest) (GenerateResponse, error) {
	m.calls++
	if m.calls == 1 {
		return GenerateResponse{ToolCalls: []ToolCall{{ID: "call-1", Name: "charge", Arguments: map[string]any{"amount": int64(42)}}}}, nil
	}
	return GenerateResponse{Content: "done"}, nil
}

func TestDurableAgentToolEmitsNoToolCallLifecycle(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	tool, err := NewTool("charge", func(_ context.Context, input map[string]any) (any, error) {
		return map[string]any{"charged": input["amount"]}, nil
	})
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}
	agent, err := NewAgent("biller", WithAgentModel(&toolCallingTestModel{}), WithAgentTools(tool))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	result, err := agent.Run(ctx, AgentInput{Message: "charge 42"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Response != "done" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	var toolBegin *pb.BeginActivationRequest
	for _, begin := range writer.beginRequests {
		if begin.GetKind() == pb.ActivationKind_ACTIVATION_KIND_TOOL {
			toolBegin = begin
		}
	}
	if toolBegin == nil {
		t.Fatalf("no TOOL activation in %#v", writer.beginRequests)
	}
	if toolBegin.GetDisplayName() != "charge" {
		t.Fatalf("display name = %q", toolBegin.GetDisplayName())
	}
	var inputData map[string]any
	if err := json.Unmarshal(toolBegin.GetInputData(), &inputData); err != nil {
		t.Fatalf("input data: %v", err)
	}
	if inputData["name"] != "charge" || inputData["tool_call_id"] != "call-1" || inputData["iteration"] != float64(1) {
		t.Fatalf("input data = %#v", inputData)
	}
	if arguments, _ := inputData["arguments"].(map[string]any); arguments["amount"] != float64(42) {
		t.Fatalf("input data arguments = %#v", inputData["arguments"])
	}
	for _, event := range ctx.Events() {
		if strings.HasPrefix(event.Type, "tool_call.") || strings.HasPrefix(event.Type, "lm.") {
			t.Fatalf("decorative lifecycle %q emitted under durable activation", event.Type)
		}
	}
}

func TestDurableAgentToolFailureCarriesLatency(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	tool, err := NewTool("charge", func(context.Context, map[string]any) (any, error) {
		return nil, errors.New("declined")
	})
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}

	if _, err := invokeAgentTool(ctx, tool, "call-1", map[string]any{}); err == nil {
		t.Fatal("expected tool error")
	}
	if len(writer.failRequests) != 1 || writer.failRequests[0].GetLatencyMs() < 0 {
		t.Fatalf("fail requests = %#v", writer.failRequests)
	}
}
