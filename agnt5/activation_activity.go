package agnt5

import (
	"context"
	"crypto/sha256"
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
	changed   chan struct{}
}

func newActivationActivity() *activationActivity {
	return &activationActivity{
		active:  make(map[string]struct{}),
		changed: make(chan struct{}),
	}
}

// snapshot returns the unsettled count and a channel closed at the next change.
func (a *activationActivity) snapshot() (int, <-chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.beginning + len(a.active), a.changed
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

func (a *activationActivity) notifyLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

// waitForOthers blocks while more than own activations are unsettled and the
// deadline has not passed. A zero deadline waits for as long as they run. It
// reports the time left, zero once the deadline has passed, and whether it
// waited at all, since a caller that did not wait still has its whole delay.
func (a *activationActivity) waitForOthers(ctx context.Context, own int, deadline time.Time) (time.Duration, bool, error) {
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
		count, changed := a.snapshot()
		var wait time.Duration
		switch {
		case count > own:
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
			if count, _ := a.snapshot(); count > own {
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
