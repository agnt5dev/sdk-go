package agnt5

import (
	"context"
	"time"
)

// MemoryScope identifies a memory namespace.
type MemoryScope string

const (
	MemoryScopeRun     MemoryScope = "run"
	MemoryScopeSession MemoryScope = "session"
	MemoryScopeUser    MemoryScope = "user"
	MemoryScopeGlobal  MemoryScope = "global"
)

// MemoryContext carries scope identifiers for memory access.
type MemoryContext struct {
	RunID     string
	SessionID string
	UserID    string
}

// MemoryMessage is one conversation memory message.
type MemoryMessage struct {
	Role      string         `json:"role"`
	Content   string         `json:"content"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// MemoryResult is returned from memory search APIs.
type MemoryResult struct {
	Key      string         `json:"key"`
	Value    any            `json:"value"`
	Score    float64        `json:"score,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// MemoryAccessor provides scoped memory helpers.
type MemoryAccessor struct {
	store StateStore
	ctx   MemoryContext
}

var defaultMemoryStore StateStore = defaultStateStore

// NewMemoryAccessor constructs a memory accessor.
func NewMemoryAccessor(store StateStore, ctx MemoryContext) *MemoryAccessor {
	if store == nil {
		store = defaultMemoryStore
	}
	return &MemoryAccessor{store: store, ctx: ctx}
}

// KV returns key/value memory for a scope.
func (m *MemoryAccessor) KV(scope MemoryScope) *KVMemory {
	return &KVMemory{state: NewStateManager(m.store, stateScopeFromMemory(scope), m.namespace(scope))}
}

// Working returns session-scoped working memory.
func (m *MemoryAccessor) Working() *WorkingMemory {
	return &WorkingMemory{kv: m.KV(MemoryScopeSession)}
}

// Conversation returns session-scoped conversation memory.
func (m *MemoryAccessor) Conversation() *ConversationMemory {
	return &ConversationMemory{kv: m.KV(MemoryScopeSession)}
}

func (m *MemoryAccessor) namespace(scope MemoryScope) string {
	switch scope {
	case MemoryScopeGlobal:
		return "global"
	case MemoryScopeUser:
		if m.ctx.UserID != "" {
			return m.ctx.UserID
		}
	case MemoryScopeSession:
		if m.ctx.SessionID != "" {
			return m.ctx.SessionID
		}
	}
	return m.ctx.RunID
}

func stateScopeFromMemory(scope MemoryScope) StateScope {
	switch scope {
	case MemoryScopeGlobal:
		return StateScopeGlobal
	case MemoryScopeUser:
		return StateScopeUser
	case MemoryScopeSession:
		return StateScopeSession
	default:
		return StateScopeRun
	}
}

// KVMemory stores arbitrary values by key.
type KVMemory struct {
	state *StateManager
}

func (m *KVMemory) Get(ctx context.Context, key string) (any, error) {
	return m.state.Get(ctx, key)
}

func (m *KVMemory) Set(ctx context.Context, key string, value any) error {
	return m.state.Set(ctx, key, value)
}

func (m *KVMemory) Delete(ctx context.Context, key string) error {
	return m.state.Delete(ctx, key)
}

func (m *KVMemory) List(ctx context.Context) (map[string]any, error) {
	return m.state.List(ctx)
}

// WorkingMemory stores a single session working-memory document.
type WorkingMemory struct {
	kv *KVMemory
}

func (m *WorkingMemory) Get(ctx context.Context) (string, error) {
	value, err := m.kv.Get(ctx, "working")
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", ErrStateNotFound
	}
	return text, nil
}

func (m *WorkingMemory) Set(ctx context.Context, value string) error {
	return m.kv.Set(ctx, "working", value)
}

// ConversationMemory stores append-only chat messages.
type ConversationMemory struct {
	kv *KVMemory
}

func (m *ConversationMemory) Append(ctx context.Context, message MemoryMessage) error {
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	value, err := m.kv.Get(ctx, "conversation")
	messages, _ := value.([]MemoryMessage)
	if err != nil && err != ErrStateNotFound {
		return err
	}
	messages = append(messages, message)
	return m.kv.Set(ctx, "conversation", messages)
}

func (m *ConversationMemory) Messages(ctx context.Context) ([]MemoryMessage, error) {
	value, err := m.kv.Get(ctx, "conversation")
	if err == ErrStateNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	messages, ok := value.([]MemoryMessage)
	if !ok {
		return nil, nil
	}
	out := make([]MemoryMessage, len(messages))
	copy(out, messages)
	return out, nil
}
