package agnt5

import (
	"context"
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

func TestWorkingMemoryMissing(t *testing.T) {
	memory := NewMemoryAccessor(NewInMemoryStateStore(), MemoryContext{RunID: "run-1"})
	_, err := memory.Working().Get(context.Background())
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("err = %v", err)
	}
}
