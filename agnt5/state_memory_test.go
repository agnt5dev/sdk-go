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
	got, err := memory.Working().Get(context.Background())
	if err != nil || got != "notes" {
		t.Fatalf("working memory = %q %v", got, err)
	}
	conversation := memory.Conversation()
	if err := conversation.Append(context.Background(), MemoryMessage{Role: "user", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	messages, err := conversation.Messages(context.Background())
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
	memory := NewMemoryAccessor(NewInMemoryStateStore(), MemoryContext{RunID: "run-1"})
	_, err := memory.Working().Get(context.Background())
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("err = %v", err)
	}
}
