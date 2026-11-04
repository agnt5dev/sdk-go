package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	causal    bool
}

type engineStateCacheEntry struct {
	stateJSON []byte
	version   int64
	expires   time.Time
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

// Keep read/write version floors for one invocation, sharing only transport.
// Completed workflows must not leave their state in a worker-wide cache.
func (s *engineStateStore) forInvocation() StateStore {
	return &engineStateStore{client: s.client, projectID: s.projectID,
		cache: make(map[string]engineStateCacheEntry), causal: true}
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
	var request *pb.PutEntityStateRequest
	var values map[string]any
	ambiguous := false
	for attempt := 0; attempt < 3; attempt++ {
		if request == nil {
			var version int64
			values, version, err = s.load(ctx, scope, namespace)
			if err != nil {
				return err
			}
			mutate(values)
			payload, err := json.Marshal(values)
			if err != nil {
				return err
			}
			request = &pb.PutEntityStateRequest{
				ProjectId: s.projectID, EntityType: stateEntityType, EntityKey: stateEntityKey,
				Scope: string(scope), ScopeId: namespace, StateJson: payload, ExpectedVersion: version,
			}
			if authority.runID != "" {
				request.RunId = authority.runID
				request.WorkerId = authority.workerID
				request.WorkerSessionId = authority.workerSessionID
				request.LeaseId = authority.leaseID
				request.Attempt = &authority.attempt
				request.OperationId = authority.operationID
			}
		}
		resp, err := s.client.PutEntityState(ctx, request)
		if err == nil {
			s.storeCached(scope, namespace, request.StateJson, resp.GetNewVersion())
			return nil
		}
		lastErr = err
		if !ambiguous && status.Code(err) == codes.FailedPrecondition &&
			strings.HasPrefix(status.Convert(err).Message(), "version conflict:") {
			// This attempt definitely wrote nothing. Refresh before rebasing.
			s.expireCached(scope, namespace)
			request = nil
		} else {
			// An accepted response may have been lost. Retry the same operation,
			// payload, expected version and fence even if later routing fails.
			ambiguous = true
		}
		if attempt == 2 {
			break
		}
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
	// State authority belongs to the run lease, independently of a nested
	// handler's activation retry attempt.
	if runCtx.invocation.Attempt < 0 {
		return stateWriteAuthority{}, errors.New("agnt5: run-scoped state write has a negative attempt")
	}
	return stateWriteAuthority{
		runID:           runCtx.RunID(),
		workerID:        workerID,
		workerSessionID: workerSessionID,
		leaseID:         runCtx.LeaseID(),
		attempt:         uint32(runCtx.invocation.Attempt),
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
	if values, version, ok, err := s.loadCached(scope, namespace); ok || err != nil {
		return values, version, err
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
	if s.causal {
		// Expiry asks the projection for fresh state; it cannot revoke evidence
		// of a newer state already acknowledged or observed in this invocation.
		s.mu.Lock()
		entry, ok := s.cache[stateStoreKey(scope, namespace, "")]
		s.mu.Unlock()
		if ok && entry.version > resp.GetVersion() {
			s.storeCached(scope, namespace, entry.stateJSON, entry.version)
			values, err := decodeStateValues(entry.stateJSON)
			return values, entry.version, err
		}
	}
	payload := resp.GetStateJson()
	if !resp.GetFound() || len(resp.GetStateJson()) == 0 {
		payload = []byte(`{}`)
	}
	values, err := decodeStateValues(payload)
	if err != nil {
		return nil, 0, err
	}
	if s.causal {
		s.storeCached(scope, namespace, payload, resp.GetVersion())
	}
	return values, resp.GetVersion(), nil
}

func decodeStateValues(payload []byte) (map[string]any, error) {
	var values map[string]any
	if err := json.Unmarshal(payload, &values); err != nil {
		return nil, err
	}
	if values == nil {
		values = map[string]any{}
	}
	return values, nil
}

func (s *engineStateStore) expireCached(scope StateScope, namespace string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := stateStoreKey(scope, namespace, "")
	if entry, ok := s.cache[key]; ok {
		entry.expires = time.Time{}
		s.cache[key] = entry
	}
}

func (s *engineStateStore) loadCached(scope StateScope, namespace string) (map[string]any, int64, bool, error) {
	s.mu.Lock()
	entry, ok := s.cache[stateStoreKey(scope, namespace, "")]
	if !ok || time.Now().After(entry.expires) {
		if !s.causal {
			delete(s.cache, stateStoreKey(scope, namespace, ""))
		}
		s.mu.Unlock()
		return nil, 0, false, nil
	}
	s.mu.Unlock()
	values, err := decodeStateValues(entry.stateJSON)
	return values, entry.version, true, err
}

func (s *engineStateStore) storeCached(scope StateScope, namespace string, payload []byte, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.cache[stateStoreKey(scope, namespace, "")]; ok && current.version > version {
		return
	}
	s.cache[stateStoreKey(scope, namespace, "")] = engineStateCacheEntry{
		stateJSON: cloneBytes(payload), version: version, expires: time.Now().Add(engineStateReadYourWriteTTL),
	}
}
