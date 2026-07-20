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
	streamEmit func(Event) error
	logger     *Logger
	projectID  string

	stateMu    sync.Mutex
	stateStore StateStore
	state      *StateManager
	memory     *MemoryAccessor
	sandbox    SandboxRunner

	stepMu           sync.Mutex
	stepCounts       map[string]int
	completedSteps   map[string][]byte
	checkpointWriter stepCheckpointWriter

	hitlMu        sync.Mutex
	pauseIndex    int
	userResponses map[int]*string
}

func newContext(parent context.Context, inv Invocation, checkpointWriter stepCheckpointWriter, projectID string, stores ...StateStore) *Context {
	if parent == nil {
		parent = context.Background()
	}
	var stateStore StateStore
	if len(stores) > 0 {
		stateStore = stores[0]
	}
	ctx := &Context{
		Context:          parent,
		invocation:       inv,
		projectID:        projectID,
		stateStore:       stateStore,
		stepCounts:       make(map[string]int),
		completedSteps:   make(map[string][]byte),
		userResponses:    make(map[int]*string),
		checkpointWriter: checkpointWriter,
	}
	ctx.loadReplayMetadata(inv.Metadata)
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

// State returns a run-scoped state accessor. The default implementation is an
// in-memory adapter so handlers can use the API before a runtime-backed adapter
// is configured.
func (c *Context) State() *StateManager {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.state == nil {
		c.state = NewStateManager(c.stateStore, StateScopeRun, c.RunID())
	}
	return c.state
}

// Memory returns a session/user-aware memory accessor backed by State.
func (c *Context) Memory() *MemoryAccessor {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.memory == nil {
		c.memory = NewMemoryAccessor(c.stateStore, MemoryContext{
			RunID:     c.RunID(),
			SessionID: c.Metadata("session_id"),
			UserID:    c.Metadata("user_id"),
		})
	}
	return c.memory
}

// SetSandbox attaches a sandbox runner to the context.
func (c *Context) SetSandbox(sandbox SandboxRunner) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.sandbox = sandbox
}

// Sandbox returns the context sandbox runner, if one was attached.
func (c *Context) Sandbox() SandboxRunner {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.sandbox
}

// Emit records an event produced by the handler. Transport delivery lands in a
// later implementation slice.
func (c *Context) Emit(event Event) error {
	if event.RunID == "" {
		event.RunID = c.RunID()
	}
	if c.IsStreaming() && IsSSEOnlyEventType(event.Type) && c.streamEmit != nil {
		if err := c.streamEmit(event); err == nil {
			return nil
		}
	}
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	c.events = append(c.events, event)
	return nil
}

func (c *Context) setStreamEmitter(emitter func(Event) error) {
	c.streamEmit = emitter
}

// Output emits an output.delta event.
func (c *Context) Output(delta string) {
	_ = c.Emit(Event{
		Type: EventTypeOutputDelta,
		Data: map[string]any{"content": delta},
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
