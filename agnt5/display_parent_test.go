package agnt5

import (
	"context"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// AGNT5-1243: a durable model, tool or delegated-agent activation is shown under
// the agent iteration that issued it. Iterations are journal events, not
// admitted activations, so the link is a reader-only hint beside the durable
// parent, which stays exactly what it was.

func iterationStarts(events []Event) []string {
	var ids []string
	for _, event := range events {
		if event.Type == "agent.iteration.started" {
			ids = append(ids, event.CorrelationID)
		}
	}
	return ids
}

func beginsOfKind(writer *recordingActivationWriter, kind pb.ActivationKind) []*pb.BeginActivationRequest {
	var matches []*pb.BeginActivationRequest
	for _, begin := range writer.beginRequests {
		if begin.GetKind() == kind {
			matches = append(matches, begin)
		}
	}
	return matches
}

func TestDurableAgentActivationsAreShownUnderTheirIteration(t *testing.T) {
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

	if _, err := agent.Run(ctx, AgentInput{Message: "charge 42"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	iterations := iterationStarts(ctx.Events())
	if len(iterations) != 2 {
		t.Fatalf("iterations = %d, want the tool-calling one and the final one", len(iterations))
	}
	kinds := make([]pb.ActivationKind, 0, len(writer.beginRequests))
	for _, begin := range writer.beginRequests {
		kinds = append(kinds, begin.GetKind())
	}
	want := []pb.ActivationKind{
		pb.ActivationKind_ACTIVATION_KIND_MODEL,
		pb.ActivationKind_ACTIVATION_KIND_TOOL,
		pb.ActivationKind_ACTIVATION_KIND_MODEL,
	}
	if len(kinds) != len(want) {
		t.Fatalf("activation kinds = %v, want %v", kinds, want)
	}
	for i, kind := range want {
		if kinds[i] != kind {
			t.Fatalf("activation kinds = %v, want %v", kinds, want)
		}
	}
	// Iteration 1 issued the first model call and the tool call; iteration 2
	// issued the final model call.
	for i, wantParent := range []string{iterations[0], iterations[0], iterations[1]} {
		if got := writer.beginRequests[i].GetDisplayParentCorrelationId(); got != wantParent {
			t.Fatalf("begin[%d] (%v) display parent = %q, want %q", i, kinds[i], got, wantParent)
		}
	}
	// Ownership is untouched: every activation still chains to the same durable
	// parent it had before the hint existed.
	for _, begin := range writer.beginRequests {
		if begin.GetParentActivationId() != writer.beginRequests[0].GetParentActivationId() {
			t.Fatalf("parent activation drifted: %q vs %q", begin.GetParentActivationId(), writer.beginRequests[0].GetParentActivationId())
		}
	}
	if ctx.displayParentCorrelationID() != "" {
		t.Fatal("the iteration display parent leaked onto the caller's context")
	}
}

func TestWorkNestedInsideAToolIsOwnedByTheTool(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	tool, err := NewTool("charge", func(handlerCtx context.Context, _ map[string]any) (any, error) {
		inner, ok := handlerCtx.(*Context)
		if !ok {
			t.Fatalf("tool handler context = %T", handlerCtx)
		}
		return Step(inner, "ledger", func(*Context) (string, error) { return "posted", nil })
	})
	if err != nil {
		t.Fatalf("NewTool: %v", err)
	}
	agent, err := NewAgent("biller", WithAgentModel(&toolCallingTestModel{}), WithAgentTools(tool))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	if _, err := agent.Run(ctx, AgentInput{Message: "charge 42"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	tools := beginsOfKind(writer, pb.ActivationKind_ACTIVATION_KIND_TOOL)
	steps := beginsOfKind(writer, pb.ActivationKind_ACTIVATION_KIND_STEP)
	if len(tools) != 1 || len(steps) != 1 {
		t.Fatalf("tool begins = %d, step begins = %d", len(tools), len(steps))
	}
	toolID := activationID(
		tools[0].GetProjectId(),
		tools[0].GetRunId(),
		tools[0].GetParentActivationId(),
		tools[0].GetKind(),
		tools[0].GetStableKey(),
	)
	if steps[0].GetParentActivationId() != toolID {
		t.Fatalf("step parent = %q, want the tool activation %q", steps[0].GetParentActivationId(), toolID)
	}
	// The tool's own nested work hangs off the tool, not off the iteration
	// that described the tool.
	if got := steps[0].GetDisplayParentCorrelationId(); got != "" {
		t.Fatalf("nested step display parent = %q, want none", got)
	}
	if got := tools[0].GetDisplayParentCorrelationId(); got != iterationStarts(ctx.Events())[0] {
		t.Fatalf("tool display parent = %q, want iteration 1", got)
	}
}

type handoffCallingTestModel struct {
	toolName string
	calls    int
}

func (m *handoffCallingTestModel) Generate(context.Context, GenerateRequest) (GenerateResponse, error) {
	m.calls++
	if m.calls == 1 {
		return GenerateResponse{ToolCalls: []ToolCall{{ID: "call-1", Name: m.toolName, Arguments: map[string]any{"message": "investigate"}}}}, nil
	}
	return GenerateResponse{Content: "relayed"}, nil
}

func TestHandoffIsShownUnderTheCallingIterationAndItsWorkUnderItsOwn(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	target, err := NewAgent("researcher", WithAgentModel(&activationAwareModel{response: GenerateResponse{Content: "handled"}}))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewAgent(
		"router",
		WithAgentModel(&handoffCallingTestModel{toolName: handoffToolName(target.Name)}),
		WithAgentHandoffs(Handoff{Agent: target, JoinPolicy: ChildJoinPolicyRequired}),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := source.Run(ctx, AgentInput{Message: "start"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Response != "handled" {
		t.Fatalf("result = %#v", result)
	}

	iterations := iterationStarts(ctx.Events())
	if len(iterations) != 2 {
		t.Fatalf("iterations = %d, want the router's and the researcher's", len(iterations))
	}
	children := beginsOfKind(writer, pb.ActivationKind_ACTIVATION_KIND_CHILD)
	if len(children) != 1 {
		t.Fatalf("child begins = %d", len(children))
	}
	if got := children[0].GetDisplayParentCorrelationId(); got != iterations[0] {
		t.Fatalf("child display parent = %q, want the router iteration %q", got, iterations[0])
	}
	childID := activationID(
		children[0].GetProjectId(),
		children[0].GetRunId(),
		children[0].GetParentActivationId(),
		children[0].GetKind(),
		children[0].GetStableKey(),
	)
	var nested []*pb.BeginActivationRequest
	for _, begin := range beginsOfKind(writer, pb.ActivationKind_ACTIVATION_KIND_MODEL) {
		if begin.GetParentActivationId() == childID {
			nested = append(nested, begin)
		}
	}
	if len(nested) != 1 {
		t.Fatalf("model activations owned by the child = %d, want 1", len(nested))
	}
	// The delegated agent's model call belongs to its own iteration, never to
	// the iteration that delegated to it.
	if got := nested[0].GetDisplayParentCorrelationId(); got != iterations[1] {
		t.Fatalf("nested model display parent = %q, want the researcher iteration %q (router's is %q)", got, iterations[1], iterations[0])
	}
}

func TestActivationsOutsideAnAgentCarryNoDisplayParent(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	if _, err := Step(ctx, "load", func(*Context) (string, error) { return "value", nil }); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := writer.beginRequests[0].GetDisplayParentCorrelationId(); got != "" {
		t.Fatalf("display parent = %q, want none", got)
	}
}
