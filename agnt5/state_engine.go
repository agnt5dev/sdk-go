package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

const (
	stateEntityType = "state"
	stateEntityKey  = "kv"
)

type engineStateStore struct {
	client    pb.EngineServiceClient
	projectID string
	mu        sync.Mutex
	cache     map[string]engineStateCacheEntry
}

type engineStateCacheEntry struct {
	values  map[string]any
	version int64
	expires time.Time
}

type stateWriteAuthority struct {
	runID           string
	workerID        string
	workerSessionID string
	leaseID         string
	attempt         uint32
	operationID     string
}

const engineStateReadYourWriteTTL = 500 * time.Millisecond

func newEngineStateStore(client pb.EngineServiceClient, projectID string) StateStore {
	if client == nil {
		return nil
	}
	return &engineStateStore{client: client, projectID: projectID, cache: make(map[string]engineStateCacheEntry)}
}

func (s *engineStateStore) Get(ctx context.Context, scope StateScope, namespace, key string) (any, bool, error) {
	values, _, err := s.load(ctx, scope, namespace)
	if err != nil {
		return nil, false, err
	}
	value, ok := values[key]
	return value, ok, nil
}

func (s *engineStateStore) Set(ctx context.Context, scope StateScope, namespace, key string, value any) error {
	if key == "" {
		return errors.New("agnt5: state key is required")
	}
	return s.update(ctx, scope, namespace, func(values map[string]any) {
		values[key] = value
	})
}

func (s *engineStateStore) Delete(ctx context.Context, scope StateScope, namespace, key string) error {
	return s.update(ctx, scope, namespace, func(values map[string]any) {
		delete(values, key)
	})
}

func (s *engineStateStore) List(ctx context.Context, scope StateScope, namespace string) (map[string]any, error) {
	values, _, err := s.load(ctx, scope, namespace)
	if err != nil {
		return nil, err
	}
	return cloneAnyMap(values), nil
}

func (s *engineStateStore) update(ctx context.Context, scope StateScope, namespace string, mutate func(map[string]any)) error {
	authority, err := stateWriteAuthorityFromContext(ctx, scope, namespace)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		values, version, err := s.load(ctx, scope, namespace)
		if err != nil {
			return err
		}
		mutate(values)
		payload, err := json.Marshal(values)
		if err != nil {
			return err
		}
		request := &pb.PutEntityStateRequest{
			ProjectId:       s.projectID,
			EntityType:      stateEntityType,
			EntityKey:       stateEntityKey,
			Scope:           string(scope),
			ScopeId:         namespace,
			StateJson:       payload,
			ExpectedVersion: version,
		}
		if authority.runID != "" {
			request.RunId = authority.runID
			request.WorkerId = authority.workerID
			request.WorkerSessionId = authority.workerSessionID
			request.LeaseId = authority.leaseID
			request.Attempt = &authority.attempt
			request.OperationId = authority.operationID
		}
		resp, err := s.client.PutEntityState(ctx, request)
		if err == nil {
			s.storeCached(scope, namespace, values, resp.GetNewVersion())
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 25 * time.Millisecond):
		}
	}
	return lastErr
}

func stateWriteAuthorityFromContext(ctx context.Context, scope StateScope, namespace string) (stateWriteAuthority, error) {
	if scope != StateScopeRun || ctx == nil {
		return stateWriteAuthority{}, nil
	}
	runCtx, _ := ctx.Value(stateAuthorityContextKey).(*Context)
	if runCtx == nil {
		return stateWriteAuthority{}, nil
	}
	if namespace == "" || runCtx.RunID() != namespace {
		return stateWriteAuthority{}, errors.New("agnt5: run-scoped state namespace does not match the active run")
	}
	workerID := runCtx.Metadata("worker_id")
	workerSessionID := runCtx.Metadata("worker_session_id")
	if workerID == "" || workerSessionID == "" || runCtx.LeaseID() == "" {
		return stateWriteAuthority{}, errors.New("agnt5: run-scoped state write is missing parked-poll authority")
	}
	if runCtx.Attempt() < 0 {
		return stateWriteAuthority{}, errors.New("agnt5: run-scoped state write has a negative attempt")
	}
	return stateWriteAuthority{
		runID:           runCtx.RunID(),
		workerID:        workerID,
		workerSessionID: workerSessionID,
		leaseID:         runCtx.LeaseID(),
		attempt:         uint32(runCtx.Attempt()),
		operationID:     newCorrelationID("state"),
	}, nil
}

func (s *engineStateStore) load(ctx context.Context, scope StateScope, namespace string) (map[string]any, int64, error) {
	if s == nil || s.client == nil {
		return nil, 0, errors.New("agnt5: nil engine state store")
	}
	if s.projectID == "" {
		return nil, 0, errors.New("agnt5: project id is required for runtime-backed state")
	}
	if values, version, ok := s.loadCached(scope, namespace); ok {
		return values, version, nil
	}
	resp, err := s.client.GetEntityState(ctx, &pb.GetEntityStateRequest{
		ProjectId:  s.projectID,
		EntityType: stateEntityType,
		EntityKey:  stateEntityKey,
		Scope:      string(scope),
		ScopeId:    namespace,
	})
	if err != nil {
		return nil, 0, err
	}
	if !resp.GetFound() || len(resp.GetStateJson()) == 0 {
		return map[string]any{}, resp.GetVersion(), nil
	}
	var values map[string]any
	if err := json.Unmarshal(resp.GetStateJson(), &values); err != nil {
		return nil, 0, err
	}
	if values == nil {
		values = map[string]any{}
	}
	return values, resp.GetVersion(), nil
}

func (s *engineStateStore) loadCached(scope StateScope, namespace string) (map[string]any, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[stateStoreKey(scope, namespace, "")]
	if !ok || time.Now().After(entry.expires) {
		delete(s.cache, stateStoreKey(scope, namespace, ""))
		return nil, 0, false
	}
	return cloneAnyMap(entry.values), entry.version, true
}

func (s *engineStateStore) storeCached(scope StateScope, namespace string, values map[string]any, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[stateStoreKey(scope, namespace, "")] = engineStateCacheEntry{
		values: cloneAnyMap(values), version: version, expires: time.Now().Add(engineStateReadYourWriteTTL),
	}
}
