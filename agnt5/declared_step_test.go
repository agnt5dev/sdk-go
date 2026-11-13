package agnt5

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

type declaredJournal struct {
	mu          sync.Mutex
	calls       []string
	begins      []*pb.BeginActivationRequest
	completions []*pb.CompleteActivationRequest
	failures    []*pb.FailActivationRequest
	committed   map[string][]byte
	attempts    map[string]uint32
	blockPhase  string
	blockRun    string
	entered     chan struct{}
	release     chan struct{}
	loseAck     bool
	failErr     error
}

func newDeclaredJournal() *declaredJournal {
	return &declaredJournal{committed: map[string][]byte{}, attempts: map[string]uint32{}, entered: make(chan struct{}, 1), release: make(chan struct{})}
}
func (j *declaredJournal) wait(ctx context.Context, phase, run string) error {
	if j.blockPhase == phase && j.blockRun == run {
		select {
		case j.entered <- struct{}{}:
		default:
		}
		select {
		case <-j.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}
func (j *declaredJournal) Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	return nil, errors.New("declared step emitted a legacy checkpoint")
}
func (j *declaredJournal) BeginActivation(ctx context.Context, r *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	j.mu.Lock()
	j.calls = append(j.calls, "begin")
	j.begins = append(j.begins, r)
	j.mu.Unlock()
	if err := j.wait(ctx, "begin", r.GetRunId()); err != nil {
		return nil, err
	}
	id := activationID(r.GetProjectId(), r.GetRunId(), r.GetParentActivationId(), r.GetKind(), r.GetStableKey())
	j.mu.Lock()
	defer j.mu.Unlock()
	if data, ok := j.committed[id]; ok {
		return &pb.BeginActivationResponse{Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY, ActivationId: id, Attempt: j.attempts[id], ReplayResult: inlineActivationPayload(data), AcceptedJournalOffset: 2}, nil
	}
	j.attempts[id]++
	return &pb.BeginActivationResponse{Outcome: pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE, ActivationId: id, Attempt: j.attempts[id], FenceToken: []byte("fence"), AcceptedJournalOffset: 1}, nil
}
func (j *declaredJournal) CompleteActivation(ctx context.Context, r *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	j.mu.Lock()
	j.calls = append(j.calls, "complete")
	j.completions = append(j.completions, r)
	j.mu.Unlock()
	if err := j.wait(ctx, "complete", r.GetRunId()); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	data, err := inlineActivationBytes(r.GetOutput())
	if err != nil {
		return nil, err
	}
	j.committed[r.GetActivationId()] = data
	if j.loseAck {
		return nil, ErrActivationUnknownOutcome
	}
	return &pb.CompleteActivationResponse{Accepted: true, ActivationId: r.GetActivationId(), Attempt: r.GetAttempt(), AcceptedJournalOffset: 2}, nil
}
func (j *declaredJournal) FailActivation(ctx context.Context, r *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	j.mu.Lock()
	j.calls = append(j.calls, "fail")
	j.failures = append(j.failures, r)
	j.mu.Unlock()
	if err := j.wait(ctx, "fail", r.GetRunId()); err != nil {
		return nil, err
	}
	if j.failErr != nil {
		return nil, j.failErr
	}
	status := pb.ActivationStatus_ACTIVATION_STATUS_FAILED
	if r.GetRetryable() {
		status = pb.ActivationStatus_ACTIVATION_STATUS_RETRY_READY
	}
	return &pb.FailActivationResponse{Accepted: true, ActivationId: r.GetActivationId(), Attempt: r.GetAttempt(), Status: status, AcceptedJournalOffset: 2}, nil
}
func declaredContext(j *declaredJournal, run string) *Context {
	return newContext(context.Background(), Invocation{ID: run, RunID: run, ComponentName: "research", ComponentType: ComponentTypeWorkflow, LeaseID: "lease-1", Metadata: map[string]string{
		durableActivationV1Capability: "true", activationArtifactSHA256Metadata: "0lJSBAIElTtKmSY0S/XeONW7020B5x6yW0xopTX5kkg=", activationDefinitionVersionMetadata: "v1", activationDefinitionConfigMetadata: `["object",[]]`, "worker_session_id": "session-1", "dispatch_mode": "pull", pullCompletionLifecycleV1Capability: "true",
	}}, j, "project-1")
}
func waitDeclared(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatal("step did not reach expected boundary")
	}
}

func TestDeclaredStepUsesOnlyTwoACKs(t *testing.T) {
	j := newDeclaredJournal()
	ctx := declaredContext(j, "run-1")
	work, err := NewStep("double", func(scope *Context, n int) (int, error) {
		active, ok := scope.Activation()
		if !ok || scope.Attempt() != 0 || active.IdempotencyKey == "" {
			return 0, errors.New("missing activation authority")
		}
		return n * 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := work.Execute(ctx, 3, "three")
	if err != nil || got != 6 {
		t.Fatalf("output=%d err=%v", got, err)
	}
	if !reflect.DeepEqual(j.calls, []string{"begin", "complete"}) {
		t.Fatal(j.calls)
	}
	if j.begins[0].GetStableKey() != "step:double:three" {
		t.Fatal(j.begins[0])
	}
	for _, e := range ctx.Events() {
		if e.Type == "function.started" || e.Type == "function.completed" {
			t.Fatal("extra function checkpoint", e)
		}
	}
}
func TestDeclaredStepWaitsForACKs(t *testing.T) {
	for _, phase := range []string{"begin", "complete"} {
		t.Run(phase, func(t *testing.T) {
			j := newDeclaredJournal()
			j.blockPhase = phase
			j.blockRun = "run-1"
			ctx := declaredContext(j, "run-1")
			executed := make(chan struct{}, 1)
			returned := make(chan struct{})
			done := make(chan error, 1)
			work, _ := NewStep("work", func(*Context, int) (int, error) { executed <- struct{}{}; return 7, nil })
			go func() {
				_, err := work.Execute(ctx, 0, "work")
				if err == nil {
					close(returned)
				}
				done <- err
			}()
			waitDeclared(t, j.entered)
			select {
			case <-returned:
				t.Fatal("returned before ACK")
			default:
			}
			if _, ok := ctx.completedStepPayload("step:work:work"); ok {
				t.Fatal("memoized before ACK")
			}
			if phase == "begin" {
				select {
				case <-executed:
					t.Fatal("handler ran before admission")
				default:
				}
			}
			close(j.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDeclaredStepLostACKReplaysCommittedResult(t *testing.T) {
	j := newDeclaredJournal()
	j.loseAck = true
	calls := 0
	work, _ := NewStep("write", func(*Context, string) (string, error) { calls++; return "saved", nil })
	ctx := declaredContext(j, "run-1")
	_, err := work.Execute(ctx, "input", "same")
	if !errors.Is(err, ErrActivationUnknownOutcome) {
		t.Fatal(err)
	}
	if _, ok := ctx.completedStepPayload("step:write:same"); ok {
		t.Fatal("memoized unknown write")
	}
	j.loseAck = false
	got, err := work.Execute(declaredContext(j, "run-1"), "input", "same")
	if err != nil || got != "saved" || calls != 1 {
		t.Fatalf("output=%q calls=%d err=%v", got, calls, err)
	}
	if !reflect.DeepEqual(j.calls, []string{"begin", "complete", "begin"}) {
		t.Fatal(j.calls)
	}
}
func TestDeclaredStepOtherRunsProgressDuringACK(t *testing.T) {
	for _, phase := range []string{"begin", "complete"} {
		t.Run(phase, func(t *testing.T) {
			j := newDeclaredJournal()
			j.blockPhase = phase
			j.blockRun = "run-1"
			work, _ := NewStep("work", func(ctx *Context, _ int) (string, error) { return ctx.RunID(), nil })
			done := make(chan error, 1)
			go func() { _, err := work.Execute(declaredContext(j, "run-1"), 0, "same"); done <- err }()
			waitDeclared(t, j.entered)
			got, err := work.Execute(declaredContext(j, "run-2"), 0, "same")
			if err != nil || got != "run-2" {
				t.Fatalf("got=%q err=%v", got, err)
			}
			close(j.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDeclaredStepConcurrentRuns(t *testing.T) {
	for _, count := range []int{10, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			j := newDeclaredJournal()
			var group sync.WaitGroup
			errs := make(chan error, count)
			work, _ := NewStep("work", func(ctx *Context, _ int) (string, error) { return ctx.RunID(), nil })
			for i := 0; i < count; i++ {
				group.Go(func() {
					run := fmt.Sprintf("run-%d", i)
					got, err := work.Execute(declaredContext(j, run), 0, "same")
					if err == nil && got != run {
						err = fmt.Errorf("output=%q want=%q", got, run)
					}
					errs <- err
				})
			}
			group.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(j.committed) != count || len(j.begins) != count || len(j.completions) != count {
				t.Fatalf("committed=%d begins=%d completes=%d", len(j.committed), len(j.begins), len(j.completions))
			}
		})
	}
}
func TestDeclaredStepRetriesFreshInput(t *testing.T) {
	j := newDeclaredJournal()
	var attempts []int
	var seen [][]int
	work, _ := NewStep("retry", func(ctx *Context, values []int) (int, error) {
		attempts = append(attempts, ctx.Attempt())
		seen = append(seen, append([]int(nil), values...))
		values[0] = 99
		if len(attempts) < 3 {
			return 0, errors.New("retry")
		}
		return values[1], nil
	}, StepConfig{MaxAttempts: 3})
	values := []int{1, 2}
	got, err := work.Execute(declaredContext(j, "run-1"), values, "same")
	if err != nil || got != 2 || !reflect.DeepEqual(attempts, []int{0, 1, 2}) || !reflect.DeepEqual(values, []int{1, 2}) {
		t.Fatalf("output=%d attempts=%v input=%v err=%v", got, attempts, values, err)
	}
	for _, value := range seen {
		if !reflect.DeepEqual(value, []int{1, 2}) {
			t.Fatal(seen)
		}
	}
	if !reflect.DeepEqual(j.calls, []string{"begin", "fail", "begin", "fail", "begin", "complete"}) {
		t.Fatal(j.calls)
	}
}
func TestDeclaredStepRequiresDurability(t *testing.T) {
	work, _ := NewStep("work", func(*Context, int) (int, error) { t.Fatal("ran without durability"); return 0, nil })
	ctx := newContext(context.Background(), Invocation{ID: "local"}, nil, "")
	_, err := work.Execute(ctx, 1, "key")
	if !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatal(err)
	}
}
func TestDeclaredStepCancellationDoesNotMemoize(t *testing.T) {
	for _, phase := range []string{"begin", "complete"} {
		t.Run(phase, func(t *testing.T) {
			j := newDeclaredJournal()
			j.blockPhase = phase
			j.blockRun = "run-1"
			ctx := declaredContext(j, "run-1")
			parent, cancel := context.WithCancel(ctx.Context)
			defer cancel()
			ctx.Context = parent
			work, _ := NewStep("cancel", func(*Context, int) (int, error) { return 1, nil })
			done := make(chan error, 1)
			go func() { _, err := work.Execute(ctx, 1, "same"); done <- err }()
			waitDeclared(t, j.entered)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation blocked")
			}
			if _, ok := ctx.completedStepPayload("step:cancel:same"); ok {
				t.Fatal("memoized cancellation")
			}
		})
	}
}

func TestDeclaredStepWaitsForFailureACK(t *testing.T) {
	j := newDeclaredJournal()
	j.blockPhase = "fail"
	j.blockRun = "run-1"
	original := errors.New("handler failed")
	work, _ := NewStep("fail", func(*Context, int) (int, error) { return 0, original })
	done := make(chan error, 1)
	ctx := declaredContext(j, "run-1")
	go func() { _, err := work.Execute(ctx, 0, "same"); done <- err }()
	waitDeclared(t, j.entered)
	select {
	case err := <-done:
		t.Fatalf("returned before failure ACK: %v", err)
	default:
	}
	close(j.release)
	if err := <-done; !errors.Is(err, original) {
		t.Fatal(err)
	}
	if _, ok := ctx.completedStepPayload("step:fail:same"); ok {
		t.Fatal("memoized failure")
	}
}

func TestDeclaredStepUncertainFailureStopsRetries(t *testing.T) {
	j := newDeclaredJournal()
	j.failErr = ErrActivationUnknownOutcome
	attempts := 0
	work, _ := NewStep("fail", func(*Context, int) (int, error) { attempts++; return 0, errors.New("failed") }, StepConfig{MaxAttempts: 3})
	_, err := work.Execute(declaredContext(j, "run-1"), 0, "same")
	if !errors.Is(err, ErrActivationUnknownOutcome) || attempts != 1 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if !reflect.DeepEqual(j.calls, []string{"begin", "fail"}) {
		t.Fatal(j.calls)
	}
}

func TestDeclaredStepRetryBudgetAndHandlerTimeout(t *testing.T) {
	t.Run("budget", func(t *testing.T) {
		j := newDeclaredJournal()
		original := errors.New("failed")
		work, _ := NewStep("fail", func(*Context, int) (int, error) { return 0, original }, StepConfig{MaxAttempts: 2})
		_, err := work.Execute(declaredContext(j, "run-1"), 0, "same")
		if !errors.Is(err, original) || len(j.failures) != 2 || !j.failures[0].GetRetryable() || j.failures[1].GetRetryable() {
			t.Fatalf("failures=%v err=%v", j.failures, err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		j := newDeclaredJournal()
		work, _ := NewStep("timeout", func(ctx *Context, _ int) (int, error) { <-ctx.Done(); return 0, ctx.Err() }, StepConfig{Timeout: time.Millisecond})
		_, err := work.Execute(declaredContext(j, "run-1"), 0, "same")
		if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(j.calls, []string{"begin", "fail"}) {
			t.Fatalf("calls=%v err=%v", j.calls, err)
		}
	})
}

func TestDeclaredStepNestedParentIdentity(t *testing.T) {
	j := newDeclaredJournal()
	child, _ := NewStep("child", func(*Context, int) (int, error) { return 7, nil })
	parent, _ := NewStep("parent", func(ctx *Context, _ int) (int, error) { return child.Execute(ctx, 0, "child") })
	ctx := declaredContext(j, "run-1")
	if _, err := parent.Execute(ctx, 1, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Execute(ctx, 2, "two"); err != nil {
		t.Fatal(err)
	}
	want := activationID("project-1", "run-1", "", pb.ActivationKind_ACTIVATION_KIND_STEP, j.begins[0].GetStableKey())
	if j.begins[1].GetParentActivationId() != want || j.begins[3].GetParentActivationId() == want {
		t.Fatal("nested step lost its parent")
	}
}

func TestDeclaredStepRejectsInvalidConfiguration(t *testing.T) {
	handler := func(*Context, int) (int, error) { return 1, nil }
	for _, config := range []StepConfig{{MaxAttempts: -1}, {Timeout: -1}, {InitialInterval: -1}, {Backoff: "invalid"}, {Multiplier: -1}} {
		if _, err := NewStep("bad", handler, config); err == nil {
			t.Fatal("accepted invalid policy", config)
		}
	}
	work, _ := NewStep("key", handler)
	if _, err := work.Execute(declaredContext(newDeclaredJournal(), "run-1"), 0, ""); err == nil {
		t.Fatal("accepted empty key")
	}
}

func TestDeclaredStepRemainsInFlightDuringRetryBackoff(t *testing.T) {
	j := newDeclaredJournal()
	ctx := declaredContext(j, "run-1")
	var scope *Context
	work, _ := NewStep("retry", func(c *Context, _ int) (int, error) {
		if c.Attempt() == 0 {
			scope = c
			return 0, errors.New("retry")
		}
		return 7, nil
	}, StepConfig{MaxAttempts: 2, InitialInterval: 100 * time.Millisecond})
	done := make(chan error, 1)
	go func() { _, err := work.Execute(ctx, 0, "same"); done <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		j.mu.Lock()
		failures := len(j.failures)
		begins := len(j.begins)
		j.mu.Unlock()
		if failures == 1 && begins == 1 {
			ctx.shared.activity.mu.Lock()
			active := len(ctx.shared.activity.active)
			ctx.shared.activity.mu.Unlock()
			// The admitted attempt has settled; only the retry reservation remains.
			if active == 1 {
				count, _ := ctx.shared.activity.snapshot(nil)
				if count != 1 {
					t.Fatalf("retry backoff lost in-flight reservation: %d", count)
				}
				own, _ := ctx.shared.activity.snapshot(scope.enclosingActivations)
				if own != 0 {
					t.Fatalf("step would wait on itself: %d", own)
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("did not observe retry backoff")
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count, _ := ctx.shared.activity.snapshot(nil); count != 0 {
		t.Fatalf("reservation leaked: %d", count)
	}
}

func TestDeclaredStepSleepDoesNotWaitOnItsOwnReservation(t *testing.T) {
	writer := &recordingActivationWriter{}
	ctx := newActivationTestContext(writer)
	ctx.invocation.Metadata[durableSuspensionV1Capability] = "true"
	work, _ := NewStep("sleep", func(scope *Context, _ int) (int, error) { return 0, scope.Sleep(time.Minute, WithSleepKey("backoff")) })
	err := within(t, 2*time.Second, "declared step sleep", func() error { _, err := work.Execute(ctx, 0, "same"); return err })
	var suspension *durableSleepSuspensionError
	if !errors.As(err, &suspension) {
		t.Fatalf("got %v", err)
	}
	if len(writer.failRequests) != 0 {
		t.Fatal("recorded a suspension as handler failure")
	}
}

func TestDeclaredStepKeepsRunLeaseAttemptSeparateFromHandlerAttempt(t *testing.T) {
	j := newDeclaredJournal()
	ctx := declaredContext(j, "run-1")
	ctx.invocation.Attempt = 7
	ctx.invocation.Metadata["worker_id"] = "worker-1"
	work, _ := NewStep("state", func(scope *Context, _ int) (uint32, error) {
		if scope.Attempt() != 0 {
			return 0, fmt.Errorf("handler attempt=%d, want zero", scope.Attempt())
		}
		authority, err := stateWriteAuthorityFromContext(scope, StateScopeRun, scope.RunID())
		return authority.attempt, err
	})
	got, err := work.Execute(ctx, 0, "state")
	if err != nil || got != 7 || ctx.Attempt() != 7 {
		t.Fatalf("state lease attempt=%d root=%d err=%v", got, ctx.Attempt(), err)
	}
}
