package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type greetInput struct {
	Name string `json:"name" description:"Person to greet"`
}

type greetOutput struct {
	Message string `json:"message"`
}

func TestRegisterFunctionAndInvoke(t *testing.T) {
	worker := NewWorker("test-worker")
	err := RegisterFunction(worker, "greet", func(ctx *Context, in greetInput) (greetOutput, error) {
		if got := ctx.Metadata("request_id"); got != "req-1" {
			t.Fatalf("metadata not propagated: %q", got)
		}
		ctx.Logger().Info("greeting", "name", in.Name)
		return greetOutput{Message: "hello " + in.Name}, nil
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	result, err := worker.invoke(context.Background(), Invocation{
		ID:            "inv-1",
		RunID:         "run-1",
		ComponentName: "greet",
		ComponentType: ComponentTypeFunction,
		Input:         []byte(`{"name":"Ada"}`),
		Metadata:      map[string]string{"request_id": "req-1"},
		LeaseID:       "lease-1",
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.LeaseID != "lease-1" {
		t.Fatalf("lease id not echoed: %q", result.LeaseID)
	}
	var out greetOutput
	if err := json.Unmarshal(result.Output, &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if out.Message != "hello Ada" {
		t.Fatalf("unexpected output: %#v", out)
	}
	if len(result.Events) != 1 || result.Events[0].Type != EventTypeLogInfo {
		t.Fatalf("expected one info log event, got %#v", result.Events)
	}
}

func TestRegisterWorkflow(t *testing.T) {
	worker := NewWorker("test-worker")
	err := RegisterWorkflow(worker, "wf", func(ctx *Context, in greetInput) (greetOutput, error) {
		message, err := Step(ctx, "build_message", func(context.Context) (string, error) {
			return "hello " + in.Name, nil
		})
		if err != nil {
			return greetOutput{}, err
		}
		return greetOutput{Message: message}, nil
	})
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	component, ok := worker.Registry().Get("wf")
	if !ok {
		t.Fatal("workflow not registered")
	}
	if component.Type != ComponentTypeWorkflow {
		t.Fatalf("unexpected component type: %s", component.Type)
	}

	result, err := worker.invoke(context.Background(), Invocation{
		ID:            "inv-1",
		RunID:         "run-1",
		ComponentName: "wf",
		ComponentType: ComponentTypeWorkflow,
		Input:         []byte(`{"name":"Grace"}`),
	})
	if err != nil {
		t.Fatalf("invoke workflow: %v", err)
	}
	var out greetOutput
	if err := json.Unmarshal(result.Output, &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if out.Message != "hello Grace" {
		t.Fatalf("unexpected workflow output: %#v", out)
	}
	if got := eventTypes(result.Events); len(got) != 2 || got[0] != "workflow.step.started" || got[1] != "workflow.step.completed" {
		t.Fatalf("unexpected workflow events: %#v", got)
	}
}

func TestRegisterDuplicate(t *testing.T) {
	worker := NewWorker("test-worker")
	handler := func(*Context, greetInput) (greetOutput, error) {
		return greetOutput{}, nil
	}
	if err := RegisterFunction(worker, "greet", handler); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := RegisterFunction(worker, "greet", handler); !errors.Is(err, ErrDuplicateComponent) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestComponentInfosAreSortedAndCopied(t *testing.T) {
	worker := NewWorker("test-worker")
	handler := func(*Context, greetInput) (greetOutput, error) {
		return greetOutput{}, nil
	}
	if err := RegisterFunction(worker, "zeta", handler, WithRetry(3, 100, 1000)); err != nil {
		t.Fatalf("register zeta: %v", err)
	}
	if err := RegisterWorkflow(worker, "alpha", handler, WithComponentMetadata(map[string]string{"owner": "sdk"})); err != nil {
		t.Fatalf("register alpha: %v", err)
	}

	infos := worker.Components()
	if len(infos) != 2 {
		t.Fatalf("expected two infos, got %#v", infos)
	}
	if infos[0].Name != "alpha" || infos[0].Type != ComponentTypeWorkflow {
		t.Fatalf("first info should be alpha workflow, got %#v", infos[0])
	}
	if infos[1].Name != "zeta" || infos[1].Config["max_attempts"] != "3" {
		t.Fatalf("second info should be zeta with retry config, got %#v", infos[1])
	}
	inputProperties, ok := infos[1].InputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("function input schema properties missing: %#v", infos[1].InputSchema)
	}
	nameSchema, ok := inputProperties["name"].(map[string]any)
	if !ok || nameSchema["type"] != "string" || nameSchema["description"] != "Person to greet" {
		t.Fatalf("unexpected name schema: %#v", inputProperties["name"])
	}
	infos[0].Metadata["owner"] = "mutated"
	if worker.Components()[0].Metadata["owner"] != "sdk" {
		t.Fatal("component info metadata was not defensively copied")
	}
	inputProperties["name"].(map[string]any)["type"] = "mutated"
	freshProperties := worker.Components()[1].InputSchema["properties"].(map[string]any)
	if freshProperties["name"].(map[string]any)["type"] != "string" {
		t.Fatal("component info input schema was not defensively copied")
	}
}

func TestTypedRegistrationIncludesProtoSchemas(t *testing.T) {
	worker := NewWorker("test-worker")
	if err := RegisterFunction(worker, "greet", func(*Context, greetInput) (greetOutput, error) {
		return greetOutput{}, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	proto := protoComponentInfo(worker.Components()[0])
	if proto.InputSchema == nil || proto.OutputSchema == nil {
		t.Fatalf("typed schemas missing from proto: %#v", proto)
	}
	name := proto.InputSchema.Properties["name"]
	if name == nil || name.Type != "string" {
		t.Fatalf("input name schema missing: %#v", proto.InputSchema)
	}
	if name.GetDescription() != "Person to greet" {
		t.Fatalf("input description = %q", name.GetDescription())
	}
	if got := proto.InputSchema.Required; len(got) != 1 || got[0] != "name" {
		t.Fatalf("required fields = %#v", got)
	}
}

func TestToolRegistrationIncludesExplicitInputSchema(t *testing.T) {
	worker := NewWorker("test-worker")
	tool, err := NewTool(
		"schema-tool",
		func(context.Context, map[string]any) (any, error) { return nil, nil },
		WithToolSchema(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"location": map[string]any{
					"type":        "string",
					"description": "City and state to search",
				},
			},
			"required": []string{"location"},
		}),
	)
	if err != nil {
		t.Fatalf("new tool: %v", err)
	}
	if err := RegisterTool(worker, tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}

	proto := protoComponentInfo(worker.Components()[0])
	location := proto.InputSchema.GetProperties()["location"]
	if location.GetDescription() != "City and state to search" {
		t.Fatalf("tool input schema missing: %#v", proto.InputSchema)
	}
}

func TestRegisterRejectsNilWorker(t *testing.T) {
	err := RegisterFunction(nil, "greet", func(*Context, greetInput) (greetOutput, error) {
		return greetOutput{}, nil
	})
	if !errors.Is(err, ErrNilWorker) {
		t.Fatalf("expected nil worker error, got %v", err)
	}
}

func TestInvokeMissingComponent(t *testing.T) {
	worker := NewWorker("test-worker")
	_, err := worker.invoke(context.Background(), Invocation{ComponentName: "missing"})
	if !errors.Is(err, ErrComponentNotFound) {
		t.Fatalf("expected component not found, got %v", err)
	}
}

func TestInvokeRejectsBadJSON(t *testing.T) {
	worker := NewWorker("test-worker")
	if err := RegisterFunction(worker, "greet", func(*Context, greetInput) (greetOutput, error) {
		return greetOutput{}, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := worker.invoke(context.Background(), Invocation{
		ComponentName: "greet",
		ComponentType: ComponentTypeFunction,
		Input:         []byte(`{"name":`),
	})
	if err == nil {
		t.Fatal("expected JSON error")
	}
}

func TestInvokeConvertsHandlerPanicToError(t *testing.T) {
	worker := NewWorker("test-worker")
	if err := RegisterFunction(worker, "panic", func(ctx *Context, _ greetInput) (greetOutput, error) {
		ctx.Logger().Info("before panic")
		panic("boom")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	result, err := worker.invoke(context.Background(), Invocation{
		ID:            "inv-panic",
		RunID:         "run-panic",
		ComponentName: "panic",
		ComponentType: ComponentTypeFunction,
		Input:         []byte(`{"name":"Ada"}`),
		LeaseID:       "lease-panic",
	})
	if err == nil || !strings.Contains(err.Error(), "handler panic: boom") {
		t.Fatalf("expected panic error, got %v", err)
	}
	if result.LeaseID != "lease-panic" {
		t.Fatalf("lease id not preserved: %q", result.LeaseID)
	}
	if len(result.Events) != 1 || result.Events[0].Type != EventTypeLogInfo {
		t.Fatalf("panic events not preserved: %#v", result.Events)
	}
}

func eventTypes(events []Event) []string {
	types := make([]string, len(events))
	for i, event := range events {
		types[i] = event.Type
	}
	return types
}
