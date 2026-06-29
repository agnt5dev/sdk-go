package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestToolRegistryCall(t *testing.T) {
	registry := NewToolRegistry()
	tool, err := NewTool("lookup", func(_ context.Context, input map[string]any) (any, error) {
		return map[string]any{"value": input["key"]}, nil
	}, WithToolDescription("lookup test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tool); err != nil {
		t.Fatal(err)
	}
	out, err := registry.CallTool(context.Background(), "lookup", map[string]any{"key": "user_123"})
	if err != nil {
		t.Fatal(err)
	}
	if out.(map[string]any)["value"] != "user_123" {
		t.Fatalf("out = %#v", out)
	}
}

func TestAskUserReturnsWaitingErrorAndEvents(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeWorkflow}, nil, "")
	_, err := ctx.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval})
	if !IsWaitingForUserInput(err) {
		t.Fatalf("expected waiting error, got %v", err)
	}
	var waiting *WaitingForUserInputError
	if !errors.As(err, &waiting) || waiting.Request.ID == "" {
		t.Fatalf("waiting = %#v", waiting)
	}
	events := ctx.Events()
	if len(events) != 2 || events[0].Type != "approval.requested" || events[1].Type != "workflow.paused" {
		t.Fatalf("events = %#v", events)
	}
}

func TestAskUserReturnsReplayedResponse(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-1",
		RunID:         "run-1",
		ComponentType: ComponentTypeWorkflow,
		Metadata:      map[string]string{"pause_index": "0", "user_response": "Alice"},
	}, nil, "")

	response, err := ctx.AskUser(UserInputRequest{Prompt: "Name?", Type: HITLText})
	if err != nil {
		t.Fatalf("ask user replay: %v", err)
	}
	if response != "Alice" {
		t.Fatalf("response = %q", response)
	}
	if len(ctx.Events()) != 0 {
		t.Fatalf("replayed HITL should not emit new pause events: %#v", ctx.Events())
	}
}

func TestAskUserPauseMetadataIncludesCompletedStepsForReplay(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeWorkflow}, nil, "")
	stepOut, err := Step(ctx, "alpha", func(context.Context) (map[string]string, error) {
		return map[string]string{"value": "cached"}, nil
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if stepOut["value"] != "cached" {
		t.Fatalf("step output = %#v", stepOut)
	}
	_, err = ctx.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval})
	if !IsWaitingForUserInput(err) {
		t.Fatalf("expected waiting error, got %v", err)
	}

	var completedRaw string
	for _, event := range ctx.Events() {
		if event.Type == "workflow.paused" {
			completedRaw = event.Metadata["completed_steps"]
			break
		}
	}
	if completedRaw == "" {
		t.Fatalf("pause metadata missing completed_steps: %#v", ctx.Events())
	}
	var completed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(completedRaw), &completed); err != nil {
		t.Fatalf("completed_steps json: %v", err)
	}
	if _, ok := completed["step:alpha:0"]; !ok {
		t.Fatalf("completed_steps missing alpha step: %#v", completed)
	}

	replayCtx := newContext(context.Background(), Invocation{
		ID:            "run-2",
		RunID:         "run-2",
		ComponentType: ComponentTypeWorkflow,
		Metadata:      map[string]string{"completed_steps": completedRaw},
	}, nil, "")
	replayed, err := Step(replayCtx, "alpha", func(context.Context) (map[string]string, error) {
		t.Fatal("replayed step executed instead of using completed_steps")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("replayed step: %v", err)
	}
	if replayed["value"] != "cached" {
		t.Fatalf("replayed output = %#v", replayed)
	}
}
