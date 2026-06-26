package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type greetInput struct {
	Name string `json:"name"`
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
	infos[0].Metadata["owner"] = "mutated"
	if worker.Components()[0].Metadata["owner"] != "sdk" {
		t.Fatal("component info metadata was not defensively copied")
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
