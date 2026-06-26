package agnt5

import (
	"context"
	"sync"
)

// Context is passed to Go component handlers.
type Context struct {
	context.Context

	invocation Invocation
	eventsMu   sync.Mutex
	events     []Event
	logger     *Logger
	projectID  string

	stepMu           sync.Mutex
	stepCounts       map[string]int
	checkpointWriter stepCheckpointWriter
}

func newContext(parent context.Context, inv Invocation, checkpointWriter stepCheckpointWriter, projectID string) *Context {
	if parent == nil {
		parent = context.Background()
	}
	ctx := &Context{
		Context:          parent,
		invocation:       inv,
		projectID:        projectID,
		stepCounts:       make(map[string]int),
		checkpointWriter: checkpointWriter,
	}
	ctx.logger = &Logger{ctx: ctx}
	return ctx
}

// InvocationID returns the runtime invocation ID.
func (c *Context) InvocationID() string {
	return c.invocation.ID
}

// RunID returns the run ID associated with this invocation.
func (c *Context) RunID() string {
	if c.invocation.RunID != "" {
		return c.invocation.RunID
	}
	return c.invocation.ID
}

// ComponentName returns the component being executed.
func (c *Context) ComponentName() string {
	return c.invocation.ComponentName
}

// ComponentType returns the component type being executed.
func (c *Context) ComponentType() ComponentType {
	return c.invocation.ComponentType
}

// Attempt returns the zero-based retry attempt number.
func (c *Context) Attempt() int {
	return c.invocation.Attempt
}

// Metadata returns a single metadata value.
func (c *Context) Metadata(key string) string {
	return c.invocation.Metadata[key]
}

// MetadataMap returns a defensive copy of invocation metadata.
func (c *Context) MetadataMap() map[string]string {
	return cloneStringMap(c.invocation.Metadata)
}

// LeaseID returns the current dispatch lease ID, if any.
func (c *Context) LeaseID() string {
	return c.invocation.LeaseID
}

// IsStreaming reports whether this run has an active streaming listener.
func (c *Context) IsStreaming() bool {
	return c.invocation.IsStreaming
}

// Logger returns the run-scoped logger.
func (c *Context) Logger() *Logger {
	return c.logger
}

// Emit records an event produced by the handler. Transport delivery lands in a
// later implementation slice.
func (c *Context) Emit(event Event) error {
	if event.RunID == "" {
		event.RunID = c.RunID()
	}
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	c.events = append(c.events, event)
	return nil
}

// Output emits an output.delta event.
func (c *Context) Output(delta string) {
	_ = c.Emit(Event{
		Type: EventTypeOutputDelta,
		Data: map[string]any{"delta": delta},
	})
}

// Events returns a defensive copy of events emitted during invocation.
func (c *Context) Events() []Event {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

func (c *Context) nextStepKey(name string) string {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	idx := c.stepCounts[name]
	c.stepCounts[name] = idx + 1
	return "step:" + name + ":" + intString(idx)
}
