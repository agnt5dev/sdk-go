package agnt5

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

// StateScope identifies the namespace for a state value.
type StateScope string

const (
	StateScopeRun     StateScope = "run"
	StateScopeSession StateScope = "session"
	StateScopeUser    StateScope = "user"
	StateScopeGlobal  StateScope = "global"
)

// ErrStateNotFound is returned when a state key does not exist.
var ErrStateNotFound = errors.New("agnt5: state not found")

// StateStore is the pluggable storage boundary for StateManager.
type StateStore interface {
	Get(ctx context.Context, scope StateScope, namespace, key string) (any, bool, error)
	Set(ctx context.Context, scope StateScope, namespace, key string, value any) error
	Delete(ctx context.Context, scope StateScope, namespace, key string) error
	List(ctx context.Context, scope StateScope, namespace string) (map[string]any, error)
}

// StateManager provides scoped key/value state access.
type StateManager struct {
	store     StateStore
	scope     StateScope
	namespace string
}

var defaultStateStore StateStore = NewInMemoryStateStore()

// NewStateManager constructs a scoped state manager.
func NewStateManager(store StateStore, scope StateScope, namespace string) *StateManager {
	if store == nil {
		store = defaultStateStore
	}
	if scope == "" {
		scope = StateScopeRun
	}
	return &StateManager{store: store, scope: scope, namespace: namespace}
}

// Scope returns a manager for another scope and namespace.
func (s *StateManager) Scope(scope StateScope, namespace string) *StateManager {
	if s == nil {
		return NewStateManager(nil, scope, namespace)
	}
	return NewStateManager(s.store, scope, namespace)
}

// Get returns a state value by key.
func (s *StateManager) Get(ctx context.Context, key string) (any, error) {
	value, ok, err := s.store.Get(ctx, s.scope, s.namespace, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrStateNotFound
	}
	return value, nil
}

// GetString returns a string state value by key.
func (s *StateManager) GetString(ctx context.Context, key string) (string, error) {
	value, err := s.Get(ctx, key)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", errors.New("agnt5: state value is not a string")
	}
	return text, nil
}

// Set stores a state value.
func (s *StateManager) Set(ctx context.Context, key string, value any) error {
	return s.store.Set(ctx, s.scope, s.namespace, key, value)
}

// Delete removes a state value.
func (s *StateManager) Delete(ctx context.Context, key string) error {
	return s.store.Delete(ctx, s.scope, s.namespace, key)
}

// List returns all values in this scope.
func (s *StateManager) List(ctx context.Context) (map[string]any, error) {
	return s.store.List(ctx, s.scope, s.namespace)
}

// InMemoryStateStore is a process-local StateStore implementation.
type InMemoryStateStore struct {
	mu     sync.RWMutex
	values map[string]any
}

// NewInMemoryStateStore creates an empty in-memory state store.
func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{values: make(map[string]any)}
}

func stateStoreKey(scope StateScope, namespace, key string) string {
	return string(scope) + "\x00" + namespace + "\x00" + key
}

func statePrefix(scope StateScope, namespace string) string {
	return string(scope) + "\x00" + namespace + "\x00"
}

func (s *InMemoryStateStore) Get(_ context.Context, scope StateScope, namespace, key string) (any, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[stateStoreKey(scope, namespace, key)]
	return value, ok, nil
}

func (s *InMemoryStateStore) Set(_ context.Context, scope StateScope, namespace, key string, value any) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("agnt5: state key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[stateStoreKey(scope, namespace, key)] = value
	return nil
}

func (s *InMemoryStateStore) Delete(_ context.Context, scope StateScope, namespace, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, stateStoreKey(scope, namespace, key))
	return nil
}

func (s *InMemoryStateStore) List(_ context.Context, scope StateScope, namespace string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := statePrefix(scope, namespace)
	keys := make([]string, 0)
	for rawKey := range s.values {
		if strings.HasPrefix(rawKey, prefix) {
			keys = append(keys, strings.TrimPrefix(rawKey, prefix))
		}
	}
	sort.Strings(keys)
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		out[key] = s.values[prefix+key]
	}
	return out, nil
}
