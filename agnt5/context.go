package agnt5

import (
	"context"
	"sync"
)

type contextKey uint8

const stateAuthorityContextKey contextKey = iota

// Context is passed to Go component handlers.
type Context struct {
	context.Context

	invocation   Invocation
	shared       *contextShared
	logger       *Logger
	projectID    string
	parentCID    string
	runCID       string
	componentCID string

	stateStore       StateStore
	checkpointWriter stepCheckpointWriter
}

type contextShared struct {
	eventsMu       sync.Mutex
	events         []Event
	streamEmit     func(Event) error
	checkpointEmit func(Event) error
	managedAgent   string

	stateMu sync.Mutex
	state   *StateManager
	memory  *MemoryAccessor
	sandbox SandboxRunner

	stepMu         sync.Mutex
	stepCounts     map[string]int
	completedSteps map[string][]byte

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
		Context:    parent,
		invocation: inv,
		shared: &contextShared{
			stepCounts:     make(map[string]int),
			completedSteps: make(map[string][]byte),
			userResponses:  make(map[int]*string),
		},
		projectID:        projectID,
		runCID:           runCorrelationIDFromRunID(inv.RunID),
		stateStore:       stateStore,
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

// Value preserves the embedded context chain while making active dispatch
// authority available to runtime-backed state stores, including through
// context.WithCancel/WithTimeout wrappers derived from this Context.
func (c *Context) Value(key any) any {
	if key == stateAuthorityContextKey {
		return c
	}
	return c.Context.Value(key)
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
	c.shared.stateMu.Lock()
	defer c.shared.stateMu.Unlock()
	if c.shared.state == nil {
		c.shared.state = NewStateManager(c.stateStore, StateScopeRun, c.RunID())
	}
	return c.shared.state
}

// Memory returns a session/user-aware memory accessor backed by State.
func (c *Context) Memory() *MemoryAccessor {
	c.shared.stateMu.Lock()
	defer c.shared.stateMu.Unlock()
	if c.shared.memory == nil {
		c.shared.memory = NewMemoryAccessor(c.stateStore, MemoryContext{
			RunID:     c.RunID(),
			SessionID: c.Metadata("session_id"),
			UserID:    c.Metadata("user_id"),
		})
	}
	return c.shared.memory
}

// SetSandbox attaches a sandbox runner to the context.
func (c *Context) SetSandbox(sandbox SandboxRunner) {
	c.shared.stateMu.Lock()
	defer c.shared.stateMu.Unlock()
	c.shared.sandbox = sandbox
}

// Sandbox returns the context sandbox runner, if one was attached.
func (c *Context) Sandbox() SandboxRunner {
	c.shared.stateMu.Lock()
	defer c.shared.stateMu.Unlock()
	return c.shared.sandbox
}

// Emit delivers streaming events immediately when transport emitters are
// available; otherwise it buffers the event for the invocation flush.
func (c *Context) Emit(event Event) error {
	if event.RunID == "" {
		event.RunID = c.RunID()
	}
	if event.CorrelationID == "" {
		event.CorrelationID = newCorrelationID("event")
	}
	if event.ParentCorrelationID == "" {
		event.ParentCorrelationID = c.parentCorrelationID()
	}
	if c.IsStreaming() {
		if IsSSEOnlyEventType(event.Type) && c.shared.streamEmit != nil {
			if err := c.shared.streamEmit(event); err == nil {
				return nil
			}
		}
		if !IsSSEOnlyEventType(event.Type) && c.shared.checkpointEmit != nil {
			if err := c.shared.checkpointEmit(event); err == nil {
				return nil
			}
		}
	}
	c.shared.eventsMu.Lock()
	defer c.shared.eventsMu.Unlock()
	c.shared.events = append(c.shared.events, event)
	return nil
}

func (c *Context) setStreamEmitter(emitter func(Event) error) {
	c.shared.streamEmit = emitter
}

func (c *Context) setCheckpointEmitter(emitter func(Event) error) {
	c.shared.checkpointEmit = emitter
}

func (c *Context) setManagedAgent(name string) {
	c.shared.managedAgent = name
}

func (c *Context) setParentCorrelationID(correlationID string) {
	c.parentCID = correlationID
}

func (c *Context) setLifecycleCorrelationIDs(runCorrelationID, componentCorrelationID string) {
	if runCorrelationID != "" {
		c.runCID = runCorrelationID
	}
	if componentCorrelationID != "" {
		c.componentCID = componentCorrelationID
	}
}

func (c *Context) runCorrelationID() string {
	if c.runCID != "" {
		return c.runCID
	}
	return runCorrelationIDFromRunID(c.RunID())
}

func (c *Context) componentCorrelationID() string {
	if c.componentCID != "" {
		return c.componentCID
	}
	return c.parentCID
}

func (c *Context) parentCorrelationID() string {
	return c.parentCID
}

func (c *Context) withParentCorrelationID(correlationID string) *Context {
	child := &Context{
		Context:          c.Context,
		invocation:       c.invocation,
		shared:           c.shared,
		projectID:        c.projectID,
		parentCID:        correlationID,
		runCID:           c.runCID,
		componentCID:     c.componentCID,
		stateStore:       c.stateStore,
		checkpointWriter: c.checkpointWriter,
	}
	child.logger = &Logger{ctx: child}
	return child
}

func (c *Context) managesAgent(name string) bool {
	return c.shared.managedAgent != "" && c.shared.managedAgent == name
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
	c.shared.eventsMu.Lock()
	defer c.shared.eventsMu.Unlock()
	out := make([]Event, len(c.shared.events))
	copy(out, c.shared.events)
	return out
}

func (c *Context) nextStepKey(name string) string {
	c.shared.stepMu.Lock()
	defer c.shared.stepMu.Unlock()
	idx := c.shared.stepCounts[name]
	c.shared.stepCounts[name] = idx + 1
	return "step:" + name + ":" + intString(idx)
}
