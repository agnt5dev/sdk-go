package agnt5

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestTaskAgentLifecycleFormsNestedCorrelationGraph(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-deep-research",
		RunID:         "run-deep-research",
		ComponentName: "deep_research_workflow",
		ComponentType: ComponentTypeWorkflow,
	}, nil, "")
	ctx.setParentCorrelationID("workflow-cid")

	agent, err := NewAgent(
		"ScopingAgent",
		WithAgentModel(StaticModel{Model: "gpt-4o-mini", Content: "plan"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Task(ctx, "plan_research", "coffee", func(taskCtx *Context, topic string) (AgentResult, error) {
		return agent.Run(taskCtx, AgentInput{Message: topic})
	}); err != nil {
		t.Fatal(err)
	}

	events := ctx.Events()
	stepStarted := requireSingleEvent(t, events, "workflow.step.started")
	functionStarted := requireSingleEvent(t, events, "function.started")
	functionCompleted := requireSingleEvent(t, events, "function.completed")
	agentStarted := requireSingleEvent(t, events, "agent.started")
	agentCompleted := requireSingleEvent(t, events, "agent.completed")
	iterationStarted := requireSingleEvent(t, events, "agent.iteration.started")
	iterationCompleted := requireSingleEvent(t, events, "agent.iteration.completed")
	lmStarted := requireSingleEvent(t, events, "lm.started")
	lmCompleted := requireSingleEvent(t, events, "lm.completed")

	assertLifecyclePair(t, functionStarted, functionCompleted, stepStarted.CorrelationID)
	assertLifecyclePair(t, agentStarted, agentCompleted, functionStarted.CorrelationID)
	assertLifecyclePair(t, iterationStarted, iterationCompleted, agentStarted.CorrelationID)
	assertLifecyclePair(t, lmStarted, lmCompleted, iterationStarted.CorrelationID)

	assertCanonicalLifecycleFields(t, agentStarted, "ScopingAgent", "agent")
	assertCanonicalLifecycleFields(t, agentCompleted, "ScopingAgent", "agent")
	assertCanonicalLifecycleFields(t, iterationStarted, "ScopingAgent", "agent")
	assertCanonicalLifecycleFields(t, iterationCompleted, "ScopingAgent", "agent")
	assertCanonicalLifecycleFields(t, lmStarted, "gpt-4o-mini", "lm")
	assertCanonicalLifecycleFields(t, lmCompleted, "gpt-4o-mini", "lm")
}

func TestParallelTaskAgentCorrelationScopesAreIsolated(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{
		ID:            "run-parallel-agents",
		RunID:         "run-parallel-agents",
		ComponentName: "parallel_workflow",
		ComponentType: ComponentTypeWorkflow,
	}, nil, "")
	ctx.setParentCorrelationID("workflow-cid")

	const taskCount = 4
	var wg sync.WaitGroup
	for index := 0; index < taskCount; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			taskName := fmt.Sprintf("task_%d", index)
			agentName := fmt.Sprintf("Agent%d", index)
			modelName := fmt.Sprintf("model-%d", index)
			agent, err := NewAgent(
				agentName,
				WithAgentModel(StaticModel{Model: modelName, Content: "done"}),
			)
			if err != nil {
				t.Errorf("new agent %d: %v", index, err)
				return
			}
			if _, err := Task(ctx, taskName, index, func(taskCtx *Context, _ int) (AgentResult, error) {
				return agent.Run(taskCtx, AgentInput{Message: "work"})
			}); err != nil {
				t.Errorf("task %d: %v", index, err)
			}
		}()
	}
	wg.Wait()

	events := ctx.Events()
	functionCIDs := make(map[string]string, taskCount)
	for _, event := range events {
		if event.Type != "function.started" {
			continue
		}
		functionCIDs[eventDataString(event, "name")] = event.CorrelationID
	}
	if len(functionCIDs) != taskCount {
		t.Fatalf("function correlations = %#v", functionCIDs)
	}

	for index := 0; index < taskCount; index++ {
		taskName := fmt.Sprintf("task_%d", index)
		agentName := fmt.Sprintf("Agent%d", index)
		modelName := fmt.Sprintf("model-%d", index)
		functionCID := functionCIDs[taskName]
		agentStarted := requireNamedEvent(t, events, "agent.started", agentName)
		agentCompleted := requireNamedEvent(t, events, "agent.completed", agentName)
		iterationStarted := requireNamedEvent(t, events, "agent.iteration.started", agentName)
		iterationCompleted := requireNamedEvent(t, events, "agent.iteration.completed", agentName)
		lmStarted := requireNamedEvent(t, events, "lm.started", modelName)
		lmCompleted := requireNamedEvent(t, events, "lm.completed", modelName)

		assertLifecyclePair(t, agentStarted, agentCompleted, functionCID)
		assertLifecyclePair(t, iterationStarted, iterationCompleted, agentStarted.CorrelationID)
		assertLifecyclePair(t, lmStarted, lmCompleted, iterationStarted.CorrelationID)
	}
}

func TestBuiltInModelIdentityIncludesProvider(t *testing.T) {
	model := NewOpenAIModel(OpenAIConfig{Model: "gpt-4o-mini"})
	name, provider := languageModelIdentity(model, GenerateRequest{})
	if name != "openai/gpt-4o-mini" || provider != "openai" {
		t.Fatalf("model identity = %q, %q", name, provider)
	}
}

func assertLifecyclePair(t *testing.T, started Event, terminal Event, parentCorrelationID string) {
	t.Helper()
	if started.CorrelationID == "" || terminal.CorrelationID != started.CorrelationID {
		t.Fatalf("%s/%s correlation mismatch: %#v / %#v", started.Type, terminal.Type, started, terminal)
	}
	if started.ParentCorrelationID != parentCorrelationID || terminal.ParentCorrelationID != parentCorrelationID {
		t.Fatalf(
			"%s/%s parent mismatch: got %q/%q, want %q",
			started.Type,
			terminal.Type,
			started.ParentCorrelationID,
			terminal.ParentCorrelationID,
			parentCorrelationID,
		)
	}
}

func assertCanonicalLifecycleFields(t *testing.T, event Event, name string, componentType string) {
	t.Helper()
	data, ok := event.Data.(map[string]any)
	if !ok {
		t.Fatalf("%s data = %#v", event.Type, event.Data)
	}
	if data["name"] != name ||
		data["component_type"] != componentType ||
		data["correlation_id"] != event.CorrelationID ||
		data["parent_correlation_id"] != event.ParentCorrelationID {
		t.Fatalf("%s canonical data = %#v", event.Type, data)
	}
	if eventID, _ := data["event_id"].(string); eventID == "" {
		t.Fatalf("%s missing event_id: %#v", event.Type, data)
	}
	if event.Metadata["name"] != name ||
		event.Metadata["component_type"] != componentType ||
		event.Metadata["cid"] != event.CorrelationID ||
		event.Metadata["pcid"] != event.ParentCorrelationID {
		t.Fatalf("%s canonical metadata = %#v", event.Type, event.Metadata)
	}
}

func requireSingleEvent(t *testing.T, events []Event, eventType string) Event {
	t.Helper()
	var matches []Event
	for _, event := range events {
		if event.Type == eventType {
			matches = append(matches, event)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("%s events = %#v", eventType, matches)
	}
	return matches[0]
}

func requireNamedEvent(t *testing.T, events []Event, eventType string, name string) Event {
	t.Helper()
	for _, event := range events {
		if event.Type == eventType && eventDataString(event, "name") == name {
			return event
		}
	}
	t.Fatalf("missing %s event named %q", eventType, name)
	return Event{}
}

func eventDataString(event Event, key string) string {
	data, _ := event.Data.(map[string]any)
	value, _ := data[key].(string)
	return value
}
