package agnt5

import (
	"context"
	"os"
	"strings"
	"sync"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// pullCompletionLifecycleV1Capability lets a non-streaming pull job carry its
// lifecycle records inside CompleteJob instead of appending them one RPC at a
// time. It is always optional; the runtime never requires it. See
// docs/contracts/pull-completion-lifecycle-v1.md in the platform repository.
const pullCompletionLifecycleV1Capability = "pull_completion_lifecycle_v1"

// envPullCompletionLifecycle set to "disabled" stops advertising the capability.
const envPullCompletionLifecycle = "AGNT5_PULL_COMPLETION_LIFECYCLE"

// Caps mirrored from the runtime contract. A bundle over either bound is
// appended inline before CompleteJob instead.
const (
	pullLifecycleMaxRecords = 32
	pullLifecycleMaxBytes   = 256 * 1024
)

func pullCompletionLifecycleAdvertised() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv(envPullCompletionLifecycle)), "disabled")
}

// lifecycleFold buffers one run's lifecycle journal events while the handler
// runs so the run's CompleteJob can carry them.
type lifecycleFold struct {
	mu      sync.Mutex
	flushMu sync.Mutex
	events  []journalEvent
}

func (f *lifecycleFold) push(events ...journalEvent) {
	f.mu.Lock()
	f.events = append(f.events, events...)
	f.mu.Unlock()
}

func (f *lifecycleFold) take() []journalEvent {
	f.mu.Lock()
	events := f.events
	f.events = nil
	f.mu.Unlock()
	return events
}

func (w *Worker) pullCompletionLifecycleEnabled() bool {
	w.protocolMu.RLock()
	defer w.protocolMu.RUnlock()
	return w.pullLifecycleOn
}

// beginLifecycleFold starts buffering lifecycle events for runID. Events for
// the run written through writeEvent/writeEvents are held until
// endLifecycleFold returns them or flushLifecycleFold appends them inline.
func (w *Worker) beginLifecycleFold(runID string) {
	if runID == "" {
		return
	}
	w.foldMu.Lock()
	if w.lifecycleFolds == nil {
		w.lifecycleFolds = make(map[string]*lifecycleFold)
	}
	w.lifecycleFolds[runID] = &lifecycleFold{}
	w.foldMu.Unlock()
}

func (w *Worker) lifecycleFoldFor(runID string) *lifecycleFold {
	if w == nil || runID == "" {
		return nil
	}
	w.foldMu.Lock()
	defer w.foldMu.Unlock()
	return w.lifecycleFolds[runID]
}

// endLifecycleFold stops buffering for runID and returns everything held, in
// emission order. Nil when the run was never folded.
func (w *Worker) endLifecycleFold(runID string) []journalEvent {
	if runID == "" {
		return nil
	}
	w.foldMu.Lock()
	fold := w.lifecycleFolds[runID]
	delete(w.lifecycleFolds, runID)
	w.foldMu.Unlock()
	if fold == nil {
		return nil
	}
	return fold.take()
}

// flushLifecycleFold appends everything currently held for runID through the
// ordinary event writer and keeps the fold open for later events. Every
// durable write for the run (step checkpoint, activation) calls this first so
// journal order still reflects execution order.
func (w *Worker) flushLifecycleFold(ctx context.Context, runID string) error {
	fold := w.lifecycleFoldFor(runID)
	if fold == nil {
		return nil
	}
	fold.flushMu.Lock()
	defer fold.flushMu.Unlock()
	return w.writeEventsDirect(ctx, fold.take())
}

// foldEvents routes events whose run is being folded into its buffer and
// returns the rest in their original order.
func (w *Worker) foldEvents(events []journalEvent) []journalEvent {
	if w == nil || len(events) == 0 {
		return events
	}
	w.foldMu.Lock()
	if len(w.lifecycleFolds) == 0 {
		w.foldMu.Unlock()
		return events
	}
	passthrough := events[:0:0]
	for _, event := range events {
		if fold := w.lifecycleFolds[event.RunID]; fold != nil {
			fold.push(event)
			continue
		}
		passthrough = append(passthrough, event)
	}
	w.foldMu.Unlock()
	return passthrough
}

// lifecycleRecordsFromEvents converts held events into CompleteJob lifecycle
// records. ok is false when the bundle exceeds the contract caps or an event
// cannot be converted; the caller then appends the events inline.
func lifecycleRecordsFromEvents(events []journalEvent) (records []*pb.Record, ok bool) {
	if len(events) == 0 || len(events) > pullLifecycleMaxRecords {
		return nil, false
	}
	records = make([]*pb.Record, 0, len(events))
	total := 0
	for _, event := range events {
		record, err := recordFromJournalEvent(event)
		if err != nil {
			return nil, false
		}
		total += len(record.GetData())
		for key, value := range record.GetMetadata() {
			total += len(key) + len(value)
		}
		if total > pullLifecycleMaxBytes {
			return nil, false
		}
		records = append(records, record)
	}
	return records, true
}

// foldingCheckpointWriter flushes a run's held lifecycle events before every
// durable checkpoint so the checkpoint never lands below them in the journal.
type foldingCheckpointWriter struct {
	worker *Worker
	runID  string
	inner  stepCheckpointWriter
}

func (f *foldingCheckpointWriter) withLifecycleFlush(ctx context.Context, operation func() error) error {
	fold := f.worker.lifecycleFoldFor(f.runID)
	if fold == nil {
		return operation()
	}
	fold.flushMu.Lock()
	defer fold.flushMu.Unlock()

	if runCtx, ok := ctx.(*Context); ok {
		events := runCtx.takeEvents()
		if len(events) > 0 {
			_, err := f.worker.flushInvocationEvents(
				ctx,
				runCtx.invocation,
				events,
				f.worker.invocationMetadata(runCtx.invocation),
				runCtx.componentCorrelationID(),
			)
			if err != nil {
				runCtx.prependEvents(events)
				return err
			}
		}
	}
	if err := f.worker.writeEventsDirect(ctx, fold.take()); err != nil {
		return err
	}
	return operation()
}

func (f *foldingCheckpointWriter) Checkpoint(ctx context.Context, req *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	var response *pb.CheckpointResponse
	err := f.withLifecycleFlush(ctx, func() (err error) {
		response, err = f.inner.Checkpoint(ctx, req)
		return err
	})
	return response, err
}

// foldingActivationWriter is the activation-capable variant. It exists as a
// separate type so a context only gains an activation writer when the inner
// writer actually supports durable activations.
type foldingActivationWriter struct {
	foldingCheckpointWriter
	activations stepActivationWriter
}

func (f *foldingActivationWriter) BeginActivation(ctx context.Context, req *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	var response *pb.BeginActivationResponse
	err := f.withLifecycleFlush(ctx, func() (err error) {
		response, err = f.activations.BeginActivation(ctx, req)
		return err
	})
	return response, err
}

func (f *foldingActivationWriter) CompleteActivation(ctx context.Context, req *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	var response *pb.CompleteActivationResponse
	err := f.withLifecycleFlush(ctx, func() (err error) {
		response, err = f.activations.CompleteActivation(ctx, req)
		return err
	})
	return response, err
}

func (f *foldingActivationWriter) FailActivation(ctx context.Context, req *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	var response *pb.FailActivationResponse
	err := f.withLifecycleFlush(ctx, func() (err error) {
		response, err = f.activations.FailActivation(ctx, req)
		return err
	})
	return response, err
}

// foldingCheckpointWriterFor wraps the worker's checkpoint writer for a run
// that is being folded; other runs get the writer unchanged.
func (w *Worker) foldingCheckpointWriterFor(runID string) stepCheckpointWriter {
	if w.checkpointWriter == nil || w.lifecycleFoldFor(runID) == nil {
		return w.checkpointWriter
	}
	base := foldingCheckpointWriter{worker: w, runID: runID, inner: w.checkpointWriter}
	if activations, ok := w.checkpointWriter.(stepActivationWriter); ok {
		return &foldingActivationWriter{foldingCheckpointWriter: base, activations: activations}
	}
	return &base
}
