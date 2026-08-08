package agnt5

import (
	"encoding/json"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestDelegatedAgentUsesStableNestedChildActivation(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	targetModel := &activationAwareModel{response: GenerateResponse{Content: "handled"}}
	target, err := NewAgent("researcher", WithAgentModel(targetModel))
	if err != nil {
		t.Fatal(err)
	}

	result, err := runDelegatedChild(
		ctx,
		target,
		AgentInput{Message: "investigate"},
		ChildJoinPolicyRequired,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "handled" {
		t.Fatalf("result = %#v", result)
	}
	if len(writer.beginRequests) < 2 {
		t.Fatalf("begin requests = %d, want child plus nested model", len(writer.beginRequests))
	}
	child := writer.beginRequests[0]
	if child.GetKind() != pb.ActivationKind_ACTIVATION_KIND_CHILD {
		t.Fatalf("child kind = %v", child.GetKind())
	}
	if child.GetStableKey() != "child:researcher:0" {
		t.Fatalf("stable key = %q", child.GetStableKey())
	}
	link := child.GetChild()
	if link == nil || link.GetChildKey() != child.GetStableKey() || link.GetChildRunId() == "" || link.GetChildSessionId() == "" {
		t.Fatalf("child linkage = %#v", link)
	}
	if link.GetJoinPolicy() != pb.ChildJoinPolicy_CHILD_JOIN_POLICY_REQUIRED {
		t.Fatalf("join policy = %v", link.GetJoinPolicy())
	}
	if string(link.GetChildDefinitionDigest()) != string(child.GetDefinitionDigest()) {
		t.Fatal("child definition digest must bind the activation definition")
	}
	model := writer.beginRequests[1]
	childID := activationID(
		child.GetProjectId(),
		child.GetRunId(),
		child.GetParentActivationId(),
		child.GetKind(),
		child.GetStableKey(),
	)
	if model.GetKind() != pb.ActivationKind_ACTIVATION_KIND_MODEL || model.GetParentActivationId() != childID {
		t.Fatalf("nested model = %#v", model)
	}
	modelID := activationID(
		model.GetProjectId(),
		model.GetRunId(),
		model.GetParentActivationId(),
		model.GetKind(),
		model.GetStableKey(),
	)
	if targetModel.execution.ActivationID != modelID {
		t.Fatalf("model execution = %#v", targetModel.execution)
	}
}

func TestDelegatedAgentReplaysAcceptedChildWithoutModelCall(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	model := &activationAwareModel{response: GenerateResponse{Content: "must not run"}}
	target, err := NewAgent("researcher-replay", WithAgentModel(model))
	if err != nil {
		t.Fatal(err)
	}
	replayed := AgentResult{AgentName: target.Name, Response: "accepted"}
	payload, err := json.Marshal(replayed)
	if err != nil {
		t.Fatal(err)
	}
	stableKey := "child:researcher-replay:0"
	expectedID := activationID(
		ctx.projectID,
		ctx.RunID(),
		parentActivationID(ctx),
		pb.ActivationKind_ACTIVATION_KIND_CHILD,
		stableKey,
	)
	writer.beginResponse = &pb.BeginActivationResponse{
		Outcome:      pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY,
		ActivationId: expectedID,
		Attempt:      1,
		ReplayResult: inlineActivationPayload(payload),
	}

	result, err := runDelegatedChild(
		ctx,
		target,
		AgentInput{Message: "investigate"},
		ChildJoinPolicyRequired,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "accepted" || model.calls != 0 {
		t.Fatalf("result = %#v model calls = %d", result, model.calls)
	}
	if len(writer.completeRequests) != 0 || len(writer.failRequests) != 0 {
		t.Fatalf("replay wrote complete=%d fail=%d", len(writer.completeRequests), len(writer.failRequests))
	}
}
