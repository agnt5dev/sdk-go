package agnt5

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type chargeInput struct {
	OrderID string
	Lines   []int
}

// retryRegistry registers "charge" with a retry policy and "plain" without one.
func retryRegistry(t *testing.T) *Registry {
	t.Helper()
	worker := NewWorker("task-retry-test")
	noop := func(*Context, chargeInput) (int, error) { return 0, nil }
	if err := RegisterFunction(worker, "charge", noop, WithRetry(3, 0, 0), WithBackoff("constant", 1)); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunction(worker, "plain", noop); err != nil {
		t.Fatal(err)
	}
	return worker.registry
}

func TestTaskAppliesRegisteredRetryPolicy(t *testing.T) {
	j := newDeclaredJournal()
	ctx := declaredContext(j, "run-1")
	ctx.shared.registry = retryRegistry(t)

	var attempts []int
	var seen [][]int
	input := chargeInput{OrderID: "ORD-1", Lines: []int{1, 2}}
	got, err := Task(ctx, "charge", input, func(ctx *Context, in chargeInput) (int, error) {
		attempts = append(attempts, ctx.Attempt())
		seen = append(seen, append([]int(nil), in.Lines...))
		in.Lines[0] = 99
		if len(attempts) < 3 {
			return 0, errors.New("declined")
		}
		return 42, nil
	})
	if err != nil || got != 42 {
		t.Fatalf("got=%d err=%v", got, err)
	}
	if !reflect.DeepEqual(attempts, []int{0, 1, 2}) {
		t.Fatalf("attempts=%v", attempts)
	}
	for _, lines := range seen {
		if !reflect.DeepEqual(lines, []int{1, 2}) {
			t.Fatalf("an attempt saw input changed by an earlier one: %v", seen)
		}
	}
	if !reflect.DeepEqual(input.Lines, []int{1, 2}) {
		t.Fatalf("caller's input was changed: %v", input.Lines)
	}
	if !reflect.DeepEqual(j.calls, []string{"begin", "fail", "begin", "fail", "begin", "complete"}) {
		t.Fatalf("journal calls=%v", j.calls)
	}
	if !j.failures[0].GetRetryable() || !j.failures[1].GetRetryable() {
		t.Fatal("failed attempts with budget left must be reported as retryable")
	}
}

func TestTaskRetryBudgetExhaustionReturnsLastError(t *testing.T) {
	j := newDeclaredJournal()
	ctx := declaredContext(j, "run-1")
	ctx.shared.registry = retryRegistry(t)

	calls := 0
	declined := errors.New("declined")
	_, err := Task(ctx, "charge", chargeInput{}, func(*Context, chargeInput) (int, error) {
		calls++
		return 0, declined
	})
	if !errors.Is(err, declined) || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	if last := j.failures[len(j.failures)-1]; last.GetRetryable() {
		t.Fatal("the final attempt must not be reported as retryable")
	}
}

func TestTaskWithoutRegisteredPolicyRunsOnce(t *testing.T) {
	for _, name := range []string{"plain", "not-registered"} {
		j := newDeclaredJournal()
		ctx := declaredContext(j, "run-1")
		ctx.shared.registry = retryRegistry(t)

		calls := 0
		_, err := Task(ctx, name, chargeInput{}, func(*Context, chargeInput) (int, error) {
			calls++
			return 0, errors.New("boom")
		})
		if err == nil || calls != 1 {
			t.Fatalf("%s: calls=%d err=%v", name, calls, err)
		}
		if !reflect.DeepEqual(j.calls, []string{"begin", "fail"}) || j.failures[0].GetRetryable() {
			t.Fatalf("%s: journal calls=%v", name, j.calls)
		}
	}
}

func TestTaskAppliesRetryPolicyWithoutDurableActivations(t *testing.T) {
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentName: "research", ComponentType: ComponentTypeWorkflow}, nil, "")
	ctx.shared.registry = retryRegistry(t)

	calls := 0
	got, err := Task(ctx, "charge", chargeInput{}, func(*Context, chargeInput) (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("declined")
		}
		return 7, nil
	})
	if err != nil || got != 7 || calls != 3 {
		t.Fatalf("got=%d calls=%d err=%v", got, calls, err)
	}
}
