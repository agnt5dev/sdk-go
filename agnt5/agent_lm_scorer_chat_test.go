package agnt5

import (
	"context"
	"testing"
)

func TestStaticModelGenerate(t *testing.T) {
	resp, err := StaticModel{}.Generate(context.Background(), GenerateRequest{
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content != "hello" {
		t.Fatalf("resp = %#v", resp)
	}
}

func TestAgentRunEmitsEvents(t *testing.T) {
	agent, err := NewAgent("assistant", WithAgentModel(StaticModel{Content: "done"}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeAgent}, nil, "")
	result, err := agent.Run(ctx, AgentInput{Message: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "done" {
		t.Fatalf("result = %#v", result)
	}
	events := ctx.Events()
	types := eventTypes(events)
	if len(types) != 6 ||
		types[0] != "agent.started" ||
		types[1] != "agent.iteration.started" ||
		types[2] != "lm.started" ||
		types[3] != "lm.completed" ||
		types[4] != "agent.iteration.completed" ||
		types[5] != "agent.completed" {
		t.Fatalf("events = %#v", events)
	}
}

func TestAgentForwardsPromptCache(t *testing.T) {
	model := &recordingModel{response: GenerateResponse{Content: "done"}}
	agent, err := NewAgent(
		"assistant",
		WithAgentModel(model),
		WithAgentPromptCache(PromptCacheWithTTL("1h")),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = agent.Run(nil, AgentInput{Message: "work"})
	if err != nil {
		t.Fatal(err)
	}

	if model.request.Cache == nil || !model.request.Cache.Enabled || model.request.Cache.TTL != "1h" {
		t.Fatalf("request = %#v", model.request)
	}
}

func TestAgentRunsToolLoop(t *testing.T) {
	lookup, err := NewTool("lookup", func(_ context.Context, input map[string]any) (any, error) {
		return map[string]any{"name": "Alice", "key": input["key"]}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	model := &ScriptedModel{Responses: []GenerateResponse{
		{ToolCalls: []ToolCall{{ID: "call-1", Name: "lookup", Arguments: map[string]any{"key": "user_123"}}}},
		{Content: "Alice is user_123"},
	}}
	agent, err := NewAgent("assistant", WithAgentModel(model), WithAgentTools(lookup), WithAgentMaxTurns(3))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeAgent}, nil, "")
	result, err := agent.Run(ctx, AgentInput{Message: "lookup user_123"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "Alice is user_123" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(result.ToolCallDetails) != 1 || result.ToolCallDetails[0].Name != "lookup" {
		t.Fatalf("tool call details = %#v", result.ToolCallDetails)
	}
	var sawToolMessage bool
	for _, message := range result.Messages {
		if message.Role == MessageRoleTool && message.ToolCallID == "call-1" {
			sawToolMessage = true
		}
	}
	if !sawToolMessage {
		t.Fatalf("messages missing tool result: %#v", result.Messages)
	}
	types := eventTypes(ctx.Events())
	if !containsEventType(types, "tool_call.started") || !containsEventType(types, "tool_call.completed") {
		t.Fatalf("events = %#v", types)
	}
}

type recordingModel struct {
	request  GenerateRequest
	response GenerateResponse
}

func (m *recordingModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	m.request = request
	return m.response, nil
}

func TestAgentHandoff(t *testing.T) {
	specialist, err := NewAgent("specialist", WithAgentModel(StaticModel{Content: "specialist handled it"}))
	if err != nil {
		t.Fatal(err)
	}
	handoff, err := NewHandoff(specialist, WithHandoffMetadata(map[string]any{"tier": "specialist"}))
	if err != nil {
		t.Fatal(err)
	}
	routerModel := &ScriptedModel{Responses: []GenerateResponse{{
		ToolCalls: []ToolCall{{ID: "call-1", Name: "transfer_to_specialist", Arguments: map[string]any{"message": "please handle"}}},
	}}}
	router, err := NewAgent("router", WithAgentModel(routerModel), WithAgentHandoffs(handoff), WithAgentMaxTurns(2))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeAgent}, nil, "")
	result, err := router.Run(ctx, AgentInput{Message: "route this"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "specialist handled it" || result.HandoffTo != "specialist" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.HandoffMetadata["tier"] != "specialist" {
		t.Fatalf("handoff metadata = %#v", result.HandoffMetadata)
	}
}

func TestAgentManagerRunsRegisteredAgent(t *testing.T) {
	agent, err := NewAgent("managed", WithAgentModel(StaticModel{Content: "managed response"}))
	if err != nil {
		t.Fatal(err)
	}
	manager := NewAgentManager(nil)
	if err := manager.Register(agent); err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeAgent}, nil, "")
	result, err := manager.Run(ctx, "managed", AgentInput{Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "managed response" {
		t.Fatalf("result = %#v", result)
	}
	if len(manager.List()) != 1 {
		t.Fatalf("manager list = %#v", manager.List())
	}
}

func TestScorerRegistry(t *testing.T) {
	registry := NewScorerRegistry()
	if err := registry.Register(ExactMatchScorer()); err != nil {
		t.Fatal(err)
	}
	result, err := registry.Run(context.Background(), "exact_match", ScorerRequest{Output: "x", Expected: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passed || result.Score != 1 {
		t.Fatalf("result = %#v", result)
	}
}

func TestChatBotStoresConversation(t *testing.T) {
	agent, err := NewAgent("assistant", WithAgentModel(StaticModel{Content: "reply"}))
	if err != nil {
		t.Fatal(err)
	}
	bot, err := NewChatBot("chat", agent)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", Metadata: map[string]string{"session_id": "s1"}}, nil, "")
	resp, err := bot.Handle(ctx, ChatMessage{SessionID: "s1", Role: MessageRoleUser, Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.Content != "reply" {
		t.Fatalf("resp = %#v", resp)
	}
	messages, err := ctx.Memory().Conversation().Messages(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("messages = %#v", messages)
	}
}

func containsEventType(types []string, want string) bool {
	for _, got := range types {
		if got == want {
			return true
		}
	}
	return false
}
