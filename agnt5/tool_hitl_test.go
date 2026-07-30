package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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
	ctx.setParentCorrelationID("workflow-cid")
	ctx.setLifecycleCorrelationIDs("run-cid", "workflow-cid")
	_, err := ctx.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval})
	if !IsWaitingForUserInput(err) {
		t.Fatalf("expected waiting error, got %v", err)
	}
	var waiting *WaitingForUserInputError
	if !errors.As(err, &waiting) || waiting.Request.ID == "" {
		t.Fatalf("waiting = %#v", waiting)
	}
	events := ctx.Events()
	if got := eventTypes(events); !slices.Equal(got, []string{
		"workflow.step.started",
		"approval.requested",
		"workflow.step.paused",
		"workflow.paused",
	}) {
		t.Fatalf("events = %#v", events)
	}
	stepCorrelationID := events[0].CorrelationID
	if stepCorrelationID == "" ||
		events[0].ParentCorrelationID != "workflow-cid" ||
		events[1].CorrelationID != stepCorrelationID ||
		events[2].CorrelationID != stepCorrelationID ||
		events[3].CorrelationID != "workflow-cid" ||
		events[3].ParentCorrelationID != "run-cid" {
		t.Fatalf("HITL lifecycle correlation = %#v", events)
	}
	if events[3].Metadata["workflow_correlation_id"] != "workflow-cid" ||
		events[3].Metadata["step_correlation_id"] != stepCorrelationID ||
		events[3].Metadata["step_parent_correlation_id"] != "workflow-cid" {
		t.Fatalf("pause metadata = %#v", events[3].Metadata)
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

func TestAskUserResumePreservesWorkflowAndNestedStepParentCorrelation(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "nested-run:invoke",
		RunID:         "nested-run",
		ComponentName: "nested_workflow",
		ComponentType: ComponentTypeWorkflow,
	}, nil, "")
	ctx.setParentCorrelationID("workflow-cid")
	ctx.setLifecycleCorrelationIDs("run-cid", "workflow-cid")

	nested := ctx.withParentCorrelationID("function-cid")
	_, err := nested.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval})
	if !IsWaitingForUserInput(err) {
		t.Fatalf("expected waiting error, got %v", err)
	}
	pauseEvents := ctx.Events()
	pauseMetadata := cloneStringMap(pauseEvents[len(pauseEvents)-1].Metadata)
	if pauseMetadata["workflow_correlation_id"] != "workflow-cid" ||
		pauseMetadata["step_parent_correlation_id"] != "function-cid" {
		t.Fatalf("nested pause metadata = %#v", pauseMetadata)
	}

	pauseMetadata["user_response"] = "approve"
	replay := newContext(context.Background(), Invocation{
		ID:            "nested-run:invoke",
		RunID:         "nested-run",
		ComponentName: "nested_workflow",
		ComponentType: ComponentTypeWorkflow,
		Metadata:      pauseMetadata,
	}, nil, "")
	replay.setParentCorrelationID("workflow-cid")
	replay.setLifecycleCorrelationIDs("run-cid", "workflow-cid")

	response, err := replay.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval})
	if err != nil || response != "approve" {
		t.Fatalf("resume response=%q err=%v", response, err)
	}
	resumeEvents := replay.Events()
	if got := eventTypes(resumeEvents); !slices.Equal(got, []string{
		"workflow.resumed",
		"workflow.step.completed",
	}) {
		t.Fatalf("resume events = %#v", resumeEvents)
	}
	if resumeEvents[0].CorrelationID != "workflow-cid" ||
		resumeEvents[0].ParentCorrelationID != "run-cid" ||
		resumeEvents[1].CorrelationID != pauseMetadata["step_correlation_id"] ||
		resumeEvents[1].ParentCorrelationID != "function-cid" {
		t.Fatalf("nested resume correlation = %#v", resumeEvents)
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
