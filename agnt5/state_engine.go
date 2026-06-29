package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
)

const (
	stateEntityType = "state"
	stateEntityKey  = "kv"
)

type engineStateStore struct {
	client    pb.EngineServiceClient
	projectID string
}

func newEngineStateStore(client pb.EngineServiceClient, projectID string) StateStore {
	if client == nil {
		return nil
	}
	return &engineStateStore{client: client, projectID: projectID}
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
		_, err = s.client.PutEntityState(ctx, &pb.PutEntityStateRequest{
			ProjectId:       s.projectID,
			EntityType:      stateEntityType,
			EntityKey:       stateEntityKey,
			Scope:           string(scope),
			ScopeId:         namespace,
			StateJson:       payload,
			ExpectedVersion: version,
		})
		if err == nil {
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

func (s *engineStateStore) load(ctx context.Context, scope StateScope, namespace string) (map[string]any, int64, error) {
	if s == nil || s.client == nil {
		return nil, 0, errors.New("agnt5: nil engine state store")
	}
	if s.projectID == "" {
		return nil, 0, errors.New("agnt5: project id is required for runtime-backed state")
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
