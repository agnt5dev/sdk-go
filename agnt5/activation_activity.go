package agnt5

import (
	"context"
	"crypto/sha256"
	"slices"
	"sync"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// suspendQuietPeriod is how long a run must have nothing in flight before a
// durable wait gives up the lease. A branch moving from one step to its next
// is briefly at zero.
const suspendQuietPeriod = 200 * time.Millisecond

// activationActivity counts the activations of one invocation that have not
// settled. Suspending releases the run lease, and a step running under a
// released lease can never finish, so durable waits read this before they give
// the lease up.
type activationActivity struct {
	mu        sync.Mutex
	beginning int
	active    map[string]struct{}
	// pausing holds activations blocked until a pause in them returns. They
	// cannot settle first, so nothing should wait for them.
	pausing map[string]int
	changed chan struct{}
}

func newActivationActivity() *activationActivity {
	return &activationActivity{
		active:  make(map[string]struct{}),
		pausing: make(map[string]int),
		changed: make(chan struct{}),
	}
}

// snapshot returns how many activations can still finish, leaving out own and
// any that are pausing, and a channel closed at the next change.
func (a *activationActivity) snapshot(own []string) (int, <-chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	count := a.beginning
	for id := range a.active {
		if a.pausing[id] == 0 && !slices.Contains(own, id) {
			count++
		}
	}
	return count, a.changed
}

func (a *activationActivity) beginStarted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.beginning++
}

func (a *activationActivity) beginFinished(activationID string, executing bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.beginning--
	if executing {
		a.active[activationID] = struct{}{}
	}
	a.notifyLocked()
}

func (a *activationActivity) settled(activationID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, activationID)
	a.notifyLocked()
}

// settledIfAdmitted ends the count only for a call the runtime admitted. Any other
// answer can name an activation still running in another call, which must keep
// counting.
func (a *activationActivity) settledIfAdmitted(begin *pb.BeginActivationResponse) {
	if begin.GetOutcome() == pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE {
		a.settled(begin.GetActivationId())
	}
}

func (a *activationActivity) notifyLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

// waitToPause blocks until only pausing activations are left. The caller's own
// activations count as pausing for every other branch too, so two branches
// pausing together wait for running work and not for each other.
func (a *activationActivity) waitToPause(ctx context.Context, own []string) error {
	a.setPausing(own, 1)
	defer a.setPausing(own, -1)
	_, _, err := a.waitForOthers(ctx, own, time.Time{})
	return err
}

func (a *activationActivity) setPausing(ids []string, delta int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, id := range ids {
		a.pausing[id] += delta
		if a.pausing[id] == 0 {
			delete(a.pausing, id)
		}
	}
	a.notifyLocked()
}

// waitForOthers blocks while activations other than own can still finish and
// the deadline has not passed. A zero deadline waits for as long as they run.
// It reports the time left, zero once the deadline has passed, and whether it
// waited at all, since a caller that did not wait still has its whole delay.
func (a *activationActivity) waitForOthers(ctx context.Context, own []string, deadline time.Time) (time.Duration, bool, error) {
	left := func() time.Duration {
		if deadline.IsZero() {
			return time.Hour
		}
		return max(time.Until(deadline), 0)
	}
	held, waited := false, false
	for {
		remaining := left()
		if remaining == 0 {
			return 0, true, nil
		}
		count, changed := a.snapshot(own)
		var wait time.Duration
		switch {
		case count > 0:
			held, waited = true, true
			wait = remaining
		case held:
			held = false
			wait = min(suspendQuietPeriod, remaining)
			changed = nil
		default:
			return remaining, waited, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-changed:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, waited, ctx.Err()
		}
		timer.Stop()
		if changed == nil {
			if count, _ := a.snapshot(own); count > 0 {
				held = true
				continue
			}
			return left(), waited, nil
		}
	}
}

// trackingActivationWriter records which activations are unsettled as they
// pass through the writer, so every kind of activation counts the same way.
type trackingActivationWriter struct {
	stepActivationWriter
	activity *activationActivity
}

func (w *trackingActivationWriter) BeginActivation(ctx context.Context, request *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	w.activity.beginStarted()
	response, err := w.stepActivationWriter.BeginActivation(ctx, request)
	executing := err == nil && response.GetOutcome() == pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE
	w.activity.beginFinished(response.GetActivationId(), executing)
	return response, err
}

func (w *trackingActivationWriter) CompleteActivation(ctx context.Context, request *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	defer w.activity.settled(request.GetActivationId())
	return w.stepActivationWriter.CompleteActivation(ctx, request)
}

func (w *trackingActivationWriter) FailActivation(ctx context.Context, request *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	defer w.activity.settled(request.GetActivationId())
	return w.stepActivationWriter.FailActivation(ctx, request)
}

// completeTimerInProcess settles a timer whose whole delay ran under the lease.
func (c *Context) completeTimerInProcess(timerKey string, begin *pb.BeginActivationResponse, delayMS int64) error {
	payload := []byte("null")
	outputDigest := sha256.Sum256(payload)
	completed, err := c.activationWriter.CompleteActivation(c, &pb.CompleteActivationRequest{
		ProjectId:    c.projectID,
		RunId:        c.RunID(),
		ActivationId: begin.GetActivationId(),
		Attempt:      begin.GetAttempt(),
		FenceToken:   cloneBytes(begin.GetFenceToken()),
		Output:       inlineActivationPayload(payload),
		OutputDigest: outputDigest[:],
		Usage:        &pb.ActivationUsage{LatencyMs: delayMS},
	})
	if err != nil {
		return err
	}
	if !completed.GetAccepted() || completed.GetActivationId() != begin.GetActivationId() || completed.GetAttempt() != begin.GetAttempt() {
		return newActivationError(ActivationErrorUnknownOutcome, "runtime returned an invalid completion receipt", begin.GetActivationId(), begin.GetAttempt(), nil)
	}
	c.recordCompletedStep(timerKey, payload)
	return nil
}
