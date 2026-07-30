package agnt5

import (
	"context"
	"strings"
	"testing"
)

type testStreamingModel struct {
	responses []GenerateResponse
	chunks    [][]ModelStreamChunk
	call      int
}

func (m *testStreamingModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	if m.call >= len(m.responses) {
		return StaticModel{}.Generate(context.Background(), request)
	}
	response := m.responses[m.call]
	m.call++
	return response, nil
}

func (m *testStreamingModel) Stream(
	_ context.Context,
	request GenerateRequest,
	emit func(ModelStreamChunk) error,
) (GenerateResponse, error) {
	if m.call >= len(m.responses) {
		return StaticModel{}.Generate(context.Background(), request)
	}
	for _, chunk := range m.chunks[m.call] {
		if err := emit(chunk); err != nil {
			return GenerateResponse{}, err
		}
	}
	response := m.responses[m.call]
	m.call++
	return response, nil
}

func TestAgentStreamsModelTextWhenInvocationStreams(t *testing.T) {
	model := &testStreamingModel{
		responses: []GenerateResponse{{Content: "Hello from agent"}},
		chunks: [][]ModelStreamChunk{{
			{Type: ModelStreamMessageStart},
			{Type: ModelStreamMessageDelta, Content: "Hello"},
			{Type: ModelStreamMessageDelta, Content: " from"},
			{Type: ModelStreamMessageDelta, Content: " agent"},
			{Type: ModelStreamMessageStop},
		}},
	}
	agent, err := NewAgent("assistant", WithAgentModel(model))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-stream",
		RunID:         "run-stream",
		ComponentType: ComponentTypeAgent,
		IsStreaming:   true,
	}, nil, "")
	var streamed []Event
	ctx.setStreamEmitter(func(event Event) error {
		streamed = append(streamed, event)
		return nil
	})

	result, err := agent.Run(ctx, AgentInput{Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "Hello from agent" {
		t.Fatalf("result = %#v", result)
	}
	if got := eventTypes(streamed); !equalStrings(got, []string{
		"lm.message.start",
		"lm.message.delta",
		"lm.message.delta",
		"lm.message.delta",
		"lm.message.stop",
	}) {
		t.Fatalf("streamed events = %#v", got)
	}
}

func TestAgentStreamsToolArgumentsBeforeExecutingTool(t *testing.T) {
	lookup, err := NewTool("lookup", func(_ context.Context, input map[string]any) (any, error) {
		return map[string]any{"name": "Alice", "key": input["key"]}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	model := &testStreamingModel{
		responses: []GenerateResponse{
			{ToolCalls: []ToolCall{{
				ID:        "call-lookup",
				Name:      "lookup",
				Arguments: map[string]any{"key": "user_123"},
			}}},
			{Content: "Alice is an admin"},
		},
		chunks: [][]ModelStreamChunk{
			{
				{Type: ModelStreamToolCallStart, ToolCallID: "call-lookup", ToolName: "lookup"},
				{Type: ModelStreamToolCallDelta, ArgumentsDelta: `{"key":`},
				{Type: ModelStreamToolCallDelta, ArgumentsDelta: `"user_123"}`},
				{
					Type:       ModelStreamToolCallStop,
					ToolCallID: "call-lookup",
					ToolName:   "lookup",
					Arguments:  map[string]any{"key": "user_123"},
				},
			},
			{
				{Type: ModelStreamThinkingStart},
				{Type: ModelStreamThinkingDelta, Content: "Checking the lookup result"},
				{Type: ModelStreamThinkingStop},
				{Type: ModelStreamMessageStart},
				{Type: ModelStreamMessageDelta, Content: "Alice"},
				{Type: ModelStreamMessageDelta, Content: " is an admin"},
				{Type: ModelStreamMessageStop},
			},
		},
	}
	agent, err := NewAgent(
		"assistant",
		WithAgentModel(model),
		WithAgentTools(lookup),
		WithAgentMaxTurns(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-stream",
		RunID:         "run-stream",
		ComponentType: ComponentTypeAgent,
		IsStreaming:   true,
	}, nil, "")
	var streamed []Event
	ctx.setStreamEmitter(func(event Event) error {
		streamed = append(streamed, event)
		return nil
	})

	result, err := agent.Run(ctx, AgentInput{Message: "lookup user_123"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "Alice is an admin" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	streamedTypes := eventTypes(streamed)
	if !containsOrderedStrings(streamedTypes, []string{
		"lm.tool_call.start",
		"lm.tool_call.delta",
		"lm.tool_call.delta",
		"lm.tool_call.stop",
		"lm.thinking.start",
		"lm.thinking.delta",
		"lm.thinking.stop",
		"lm.message.start",
		"lm.message.delta",
		"lm.message.delta",
		"lm.message.stop",
	}) {
		t.Fatalf("streamed events = %#v", streamedTypes)
	}
	durableTypes := eventTypes(ctx.Events())
	if !containsOrderedStrings(durableTypes, []string{
		"tool_call.started",
		"tool_call.completed",
		"agent.completed",
	}) {
		t.Fatalf("durable events = %#v", durableTypes)
	}
}

func TestStreamingAgentEmitsDurableBoundariesInModelStreamOrder(t *testing.T) {
	model := &testStreamingModel{
		responses: []GenerateResponse{{Content: "Hello from agent"}},
		chunks: [][]ModelStreamChunk{{
			{Type: ModelStreamThinkingStart},
			{Type: ModelStreamThinkingDelta, Content: "Checking the request"},
			{Type: ModelStreamThinkingStop},
			{Type: ModelStreamMessageStart},
			{Type: ModelStreamMessageDelta, Content: "Hello"},
			{Type: ModelStreamMessageDelta, Content: " from agent"},
			{Type: ModelStreamMessageStop},
		}},
	}
	agent, err := NewAgent("assistant", WithAgentModel(model))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-stream",
		RunID:         "run-stream",
		ComponentType: ComponentTypeAgent,
		IsStreaming:   true,
	}, nil, "")
	var emitted []Event
	ctx.setStreamEmitter(func(event Event) error {
		emitted = append(emitted, event)
		return nil
	})
	ctx.setCheckpointEmitter(func(event Event) error {
		emitted = append(emitted, event)
		return nil
	})

	if _, err := agent.Run(ctx, AgentInput{Message: "hello"}); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(eventTypes(emitted), []string{
		"agent.started",
		"agent.iteration.started",
		"lm.started",
		"lm.thinking.start",
		"lm.thinking.delta",
		"lm.thinking.stop",
		"lm.message.start",
		"lm.message.delta",
		"lm.message.delta",
		"lm.message.stop",
		"lm.completed",
		"agent.iteration.completed",
		"agent.completed",
	}) {
		t.Fatalf("emitted events = %#v", eventTypes(emitted))
	}
	agentStarted := requireSingleEvent(t, emitted, "agent.started")
	agentCompleted := requireSingleEvent(t, emitted, "agent.completed")
	iterationStarted := requireSingleEvent(t, emitted, "agent.iteration.started")
	iterationCompleted := requireSingleEvent(t, emitted, "agent.iteration.completed")
	lmStarted := requireSingleEvent(t, emitted, "lm.started")
	lmCompleted := requireSingleEvent(t, emitted, "lm.completed")
	assertLifecyclePair(t, agentStarted, agentCompleted, "")
	assertLifecyclePair(t, iterationStarted, iterationCompleted, agentStarted.CorrelationID)
	assertLifecyclePair(t, lmStarted, lmCompleted, iterationStarted.CorrelationID)
	for _, event := range emitted {
		if strings.HasPrefix(event.Type, "lm.thinking.") || strings.HasPrefix(event.Type, "lm.message.") {
			if event.CorrelationID != lmStarted.CorrelationID ||
				event.ParentCorrelationID != iterationStarted.CorrelationID {
				t.Fatalf("%s stream correlation = %#v", event.Type, event)
			}
			assertCanonicalLifecycleFields(t, event, "language_model", "lm")
		}
	}
}

func TestWorkerManagedAgentSuppressesDuplicateComponentLifecycle(t *testing.T) {
	model := &testStreamingModel{
		responses: []GenerateResponse{{Content: "done"}},
		chunks:    [][]ModelStreamChunk{{}},
	}
	agent, err := NewAgent("assistant", WithAgentModel(model))
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-managed",
		RunID:         "run-managed",
		ComponentType: ComponentTypeAgent,
	}, nil, "")
	ctx.setManagedAgent("assistant")
	ctx.setParentCorrelationID("agent-component-cid")

	if _, err := agent.Run(ctx, AgentInput{Message: "hello"}); err != nil {
		t.Fatal(err)
	}
	types := eventTypes(ctx.Events())
	if containsString(types, "agent.started") || containsString(types, "agent.completed") {
		t.Fatalf("worker-managed lifecycle was duplicated: %#v", types)
	}
	if !containsOrderedStrings(types, []string{
		"agent.iteration.started",
		"lm.started",
		"lm.completed",
		"agent.iteration.completed",
	}) {
		t.Fatalf("agent internals were not emitted: %#v", types)
	}
	iterationStarted := requireSingleEvent(t, ctx.Events(), "agent.iteration.started")
	iterationCompleted := requireSingleEvent(t, ctx.Events(), "agent.iteration.completed")
	lmStarted := requireSingleEvent(t, ctx.Events(), "lm.started")
	lmCompleted := requireSingleEvent(t, ctx.Events(), "lm.completed")
	assertLifecyclePair(t, iterationStarted, iterationCompleted, "agent-component-cid")
	assertLifecyclePair(t, lmStarted, lmCompleted, iterationStarted.CorrelationID)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func containsOrderedStrings(haystack, needles []string) bool {
	index := 0
	for _, value := range haystack {
		if index < len(needles) && value == needles[index] {
			index++
		}
	}
	return index == len(needles)
}
