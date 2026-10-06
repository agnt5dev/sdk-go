package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestStateManagerScopesValues(t *testing.T) {
	store := NewInMemoryStateStore()
	runState := NewStateManager(store, StateScopeRun, "run-1")
	userState := runState.Scope(StateScopeUser, "user-1")
	if err := runState.Set(context.Background(), "key", "run-value"); err != nil {
		t.Fatal(err)
	}
	if err := userState.Set(context.Background(), "key", "user-value"); err != nil {
		t.Fatal(err)
	}
	got, err := runState.GetString(context.Background(), "key")
	if err != nil || got != "run-value" {
		t.Fatalf("run state = %q %v", got, err)
	}
	got, err = userState.GetString(context.Background(), "key")
	if err != nil || got != "user-value" {
		t.Fatalf("user state = %q %v", got, err)
	}
}

func TestMemoryAccessorConversation(t *testing.T) {
	store := NewInMemoryStateStore()
	memory := NewMemoryAccessor(store, MemoryContext{RunID: "run-1", SessionID: "session-1"})
	if err := memory.Working().Set(context.Background(), "notes"); err != nil {
		t.Fatal(err)
	}
	nextRun := NewMemoryAccessor(store, MemoryContext{RunID: "run-2", SessionID: "session-1"})
	got, err := nextRun.Working().Get(context.Background())
	if err != nil || got != "notes" {
		t.Fatalf("working memory = %q %v", got, err)
	}
	conversation := memory.Conversation()
	if err := conversation.Append(context.Background(), MemoryMessage{Role: "user", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	messages, err := nextRun.Conversation().Messages(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Content != "hello" || messages[0].CreatedAt.IsZero() {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestConversationMemoryHandlesJSONDecodedState(t *testing.T) {
	store := &jsonRoundTripStateStore{delegate: NewInMemoryStateStore()}
	memory := NewMemoryAccessor(store, MemoryContext{RunID: "run-1", SessionID: "session-1"})
	conversation := memory.Conversation()
	for _, message := range []MemoryMessage{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "hi"},
	} {
		if err := conversation.Append(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := conversation.Messages(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Content != "hello" || messages[1].Content != "hi" {
		t.Fatalf("messages = %#v", messages)
	}
}

type jsonRoundTripStateStore struct {
	delegate *InMemoryStateStore
}

func (s *jsonRoundTripStateStore) Get(ctx context.Context, scope StateScope, namespace, key string) (any, bool, error) {
	return s.delegate.Get(ctx, scope, namespace, key)
}

func (s *jsonRoundTripStateStore) Set(ctx context.Context, scope StateScope, namespace, key string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return err
	}
	return s.delegate.Set(ctx, scope, namespace, key, decoded)
}

func (s *jsonRoundTripStateStore) Delete(ctx context.Context, scope StateScope, namespace, key string) error {
	return s.delegate.Delete(ctx, scope, namespace, key)
}

func (s *jsonRoundTripStateStore) List(ctx context.Context, scope StateScope, namespace string) (map[string]any, error) {
	return s.delegate.List(ctx, scope, namespace)
}

func TestWorkingMemoryMissing(t *testing.T) {
	memory := NewMemoryAccessor(NewInMemoryStateStore(), MemoryContext{RunID: "run-1", SessionID: "session-1"})
	_, err := memory.Working().Get(context.Background())
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestMemoryRejectsMissingScopeIdentityBeforeStorage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		scope   MemoryScope
		wantErr error
		ctx     MemoryContext
	}{
		{"user", MemoryScopeUser, ErrMemoryUserIDRequired, MemoryContext{RunID: "run-1", SessionID: "session-1"}},
		{"session", MemoryScopeSession, ErrMemorySessionIDRequired, MemoryContext{RunID: "run-1", UserID: "user-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operations := map[string]func(*MemoryAccessor) error{
				"get": func(m *MemoryAccessor) error {
					_, err := m.KV(tc.scope).Get(context.Background(), "key")
					return err
				},
				"set": func(m *MemoryAccessor) error {
					return m.KV(tc.scope).Set(context.Background(), "key", "value")
				},
				"delete": func(m *MemoryAccessor) error {
					return m.KV(tc.scope).Delete(context.Background(), "key")
				},
				"list": func(m *MemoryAccessor) error {
					_, err := m.KV(tc.scope).List(context.Background())
					return err
				},
			}
			if tc.scope == MemoryScopeSession {
				operations["working_get"] = func(m *MemoryAccessor) error {
					_, err := m.Working().Get(context.Background())
					return err
				}
				operations["working_set"] = func(m *MemoryAccessor) error {
					return m.Working().Set(context.Background(), "notes")
				}
				operations["conversation_append"] = func(m *MemoryAccessor) error {
					return m.Conversation().Append(context.Background(), MemoryMessage{Role: "user", Content: "hello"})
				}
				operations["conversation_messages"] = func(m *MemoryAccessor) error {
					_, err := m.Conversation().Messages(context.Background())
					return err
				}
			}
			for name, operation := range operations {
				t.Run(name, func(t *testing.T) {
					delegate := NewInMemoryStateStore()
					if err := delegate.Set(context.Background(), stateScopeFromMemory(tc.scope), tc.ctx.RunID, "key", "legacy fallback value"); err != nil {
						t.Fatal(err)
					}
					store := &countingMemoryStore{StateStore: delegate}
					err := operation(NewMemoryAccessor(store, tc.ctx))
					if !errors.Is(err, tc.wantErr) {
						t.Errorf("expected %v, got %v", tc.wantErr, err)
					}
					if store.calls != 0 {
						t.Errorf("accessed storage %d times with missing identity", store.calls)
					}
				})
			}
		})
	}
}

func TestContextMemorySharesOnlyTheSelectedScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scope  MemoryScope
		shared bool
		idKey  string
	}{
		{"user", MemoryScopeUser, true, "user_id"},
		{"session", MemoryScopeSession, true, "session_id"},
		{"run", MemoryScopeRun, false, ""},
		{"global", MemoryScopeGlobal, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewInMemoryStateStore()
			newMemory := func(runID, identity string) *KVMemory {
				metadata := map[string]string{}
				if tc.idKey != "" {
					metadata[tc.idKey] = identity
				}
				ctx := newContext(context.Background(), Invocation{RunID: runID, Metadata: metadata}, nil, "", store)
				return ctx.Memory().KV(tc.scope)
			}
			if err := newMemory("run-1", "identity-1").Set(context.Background(), "key", "value"); err != nil {
				t.Fatal(err)
			}
			value, err := newMemory("run-2", "identity-1").Get(context.Background(), "key")
			if tc.shared {
				if err != nil || value != "value" {
					t.Fatalf("shared value = %v, %v", value, err)
				}
			} else if !errors.Is(err, ErrStateNotFound) {
				t.Fatalf("different run should have no value, got %v, %v", value, err)
			}
			if tc.idKey != "" {
				_, err := newMemory("run-1", "identity-2").Get(context.Background(), "key")
				if !errors.Is(err, ErrStateNotFound) {
					t.Fatalf("different identity should have no value, got %v", err)
				}
			}
		})
	}
}

type countingMemoryStore struct {
	StateStore
	calls int
}

func (s *countingMemoryStore) Get(ctx context.Context, scope StateScope, namespace, key string) (any, bool, error) {
	s.calls++
	return s.StateStore.Get(ctx, scope, namespace, key)
}

func (s *countingMemoryStore) Set(ctx context.Context, scope StateScope, namespace, key string, value any) error {
	s.calls++
	return s.StateStore.Set(ctx, scope, namespace, key, value)
}

func (s *countingMemoryStore) Delete(ctx context.Context, scope StateScope, namespace, key string) error {
	s.calls++
	return s.StateStore.Delete(ctx, scope, namespace, key)
}

func (s *countingMemoryStore) List(ctx context.Context, scope StateScope, namespace string) (map[string]any, error) {
	s.calls++
	return s.StateStore.List(ctx, scope, namespace)
}
