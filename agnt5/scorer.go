package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ScorerScope identifies the artifact a scorer evaluates.
type ScorerScope string

const (
	ScorerScopeItem     ScorerScope = "item"
	ScorerScopeRun      ScorerScope = "run"
	ScorerScopeTrace    ScorerScope = "trace"
	ScorerScopeSpan     ScorerScope = "span"
	ScorerScopeSession  ScorerScope = "session"
	ScorerScopeFleetRun ScorerScope = "fleet_run"

	builtInComponentSource = "agnt5_builtin"
)

// ScorerRequest is passed to scorer handlers.
type ScorerRequest struct {
	Input            any              `json:"input,omitempty"`
	Output           any              `json:"output,omitempty"`
	Expected         any              `json:"expected,omitempty"`
	Trace            []TraceEvent     `json:"trace,omitempty"`
	Config           map[string]any   `json:"config,omitempty"`
	PeerScores       []map[string]any `json:"peer_scores,omitempty"`
	TraceEvalContext any              `json:"trace_eval_context,omitempty"`
	State            map[string]any   `json:"state,omitempty"`
	States           map[string]any   `json:"states,omitempty"`
	StateSnapshots   map[string]any   `json:"state_snapshots,omitempty"`
	Metadata         map[string]any   `json:"metadata,omitempty"`
	// Events is retained for compatibility with early Go SDK scorer handlers.
	// New code should use Trace, which matches the cross-SDK scorer contract.
	Events []RunEvent `json:"events,omitempty"`
}

// ScorerResult is returned by scorer handlers.
type ScorerResult struct {
	Score       float64        `json:"score"`
	Passed      bool           `json:"passed"`
	Explanation string         `json:"explanation,omitempty"`
	Label       string         `json:"label,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// ScorerHandler executes a scorer.
type ScorerHandler func(context.Context, ScorerRequest) (ScorerResult, error)

// ScorerContext is the run-scoped context passed to worker scorer handlers.
type ScorerContext = Context

// ScorerConfig describes a registered scorer.
type ScorerConfig struct {
	Name        string
	Description string
	Handler     ScorerHandler
	Metadata    map[string]any
	Scope       ScorerScope
	IsAsync     bool
	DependsOn   []string
	InputSchema map[string]any
}

// ScorerRegistry stores scorers by name.
type ScorerRegistry struct {
	mu       sync.RWMutex
	scorers  map[string]ScorerConfig
	builtins map[string]ScorerConfig
}

func NewScorerRegistry() *ScorerRegistry {
	r := &ScorerRegistry{
		scorers:  make(map[string]ScorerConfig),
		builtins: make(map[string]ScorerConfig),
	}
	for _, config := range builtInScorerConfigs() {
		r.builtins[config.Name] = cloneScorerConfig(config)
	}
	return r
}

var defaultScorerRegistry = NewScorerRegistry()

func DefaultScorerRegistry() *ScorerRegistry {
	return defaultScorerRegistry
}

func (r *ScorerRegistry) Register(config ScorerConfig) error {
	if strings.TrimSpace(config.Name) == "" {
		return ErrInvalidComponentName
	}
	if config.Handler == nil {
		return ErrNilHandler
	}
	if config.Scope == "" {
		config.Scope = ScorerScopeItem
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, reserved := r.builtins[config.Name]; reserved {
		return &ScorerNameCollisionError{Name: config.Name, BuiltIn: true}
	}
	if _, exists := r.scorers[config.Name]; exists {
		return &ScorerNameCollisionError{Name: config.Name}
	}
	r.scorers[config.Name] = cloneScorerConfig(config)
	return nil
}

func (r *ScorerRegistry) Get(name string) (ScorerConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	config, ok := r.builtins[name]
	if !ok {
		config, ok = r.scorers[name]
	}
	if !ok {
		return ScorerConfig{}, false
	}
	return cloneScorerConfig(config), true
}

func (r *ScorerRegistry) List() []ScorerConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.builtins)+len(r.scorers))
	for name := range r.builtins {
		names = append(names, name)
	}
	for name := range r.scorers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ScorerConfig, 0, len(names))
	for _, name := range names {
		config, ok := r.builtins[name]
		if !ok {
			config = r.scorers[name]
		}
		out = append(out, cloneScorerConfig(config))
	}
	return out
}

func (r *ScorerRegistry) Run(ctx context.Context, name string, request ScorerRequest) (ScorerResult, error) {
	config, ok := r.Get(name)
	if !ok {
		return ScorerResult{}, &ScorerNotFoundError{Name: name}
	}
	bound, bindingMetadata, err := applyScorerFieldBindings(request)
	if err != nil {
		return scorerConfigError(name + " field binding error: " + err.Error()), nil
	}
	result, err := config.Handler(ctx, bound)
	if err != nil {
		return ScorerResult{}, err
	}
	result = normalizeScorerResult(result)
	if len(bindingMetadata) > 0 {
		result.Metadata = mergeAnyMaps(result.Metadata, bindingMetadata)
	}
	return result, nil
}

func (r *ScorerRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scorers = make(map[string]ScorerConfig)
}

// RegisterScorer registers a scorer as a worker component.
func RegisterScorer(w *Worker, config ScorerConfig, opts ...ComponentOption) error {
	if w == nil {
		return ErrNilWorker
	}
	if err := DefaultScorerRegistry().Register(config); err != nil {
		return err
	}
	if config.Scope == "" {
		config.Scope = ScorerScopeItem
	}
	opts = append([]ComponentOption{WithComponentConfig(map[string]string{"scope": string(config.Scope)})}, opts...)
	err := RegisterRaw(w, config.Name, ComponentTypeScorer, func(ctx *Context, input []byte) ([]byte, error) {
		var req ScorerRequest
		if len(input) > 0 {
			if err := json.Unmarshal(input, &req); err != nil {
				return nil, err
			}
		}
		out, err := DefaultScorerRegistry().Run(ctx, config.Name, req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	if err != nil {
		DefaultScorerRegistry().removeCustom(config.Name)
	}
	return err
}

func (r *ScorerRegistry) removeCustom(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.scorers, name)
}

// ExactMatchScorer returns a deterministic exact-match scorer.
func ExactMatchScorer() ScorerConfig {
	return cloneScorerConfig(mustBuiltInScorer("exact_match"))
}

// ContainsScorer returns a string containment scorer.
func ContainsScorer() ScorerConfig {
	return cloneScorerConfig(mustBuiltInScorer("contains"))
}

func cloneScorerConfig(config ScorerConfig) ScorerConfig {
	config.Metadata = cloneAnyMap(config.Metadata)
	config.DependsOn = append([]string(nil), config.DependsOn...)
	config.InputSchema = cloneSchemaMap(config.InputSchema)
	return config
}

func builtInScorerComponentInfos() []ComponentInfo {
	configs := DefaultScorerRegistry().List()
	out := make([]ComponentInfo, 0, len(BuiltInDeterministicScorerNames)+len(BuiltInJudgeScorerNames))
	for _, config := range configs {
		if !isBuiltInScorerName(config.Name) {
			continue
		}
		out = append(out, ComponentInfo{
			Name:        config.Name,
			Type:        ComponentTypeScorer,
			InputSchema: map[string]any{"type": "object"},
			OutputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"score":  map[string]any{"type": "number"},
					"passed": map[string]any{"type": "boolean"},
				},
			},
			Config: map[string]string{"scope": string(config.Scope)},
			Metadata: map[string]string{
				"source":      builtInComponentSource,
				"agnt5.async": fmt.Sprintf("%t", config.IsAsync),
			},
		})
	}
	return out
}

func isBuiltInScorerName(name string) bool {
	for _, candidate := range BuiltInDeterministicScorerNames {
		if name == candidate {
			return true
		}
	}
	for _, candidate := range BuiltInJudgeScorerNames {
		if name == candidate {
			return true
		}
	}
	return false
}

func (w *Worker) invokeBuiltInScorer(ctx context.Context, inv Invocation) (InvocationResult, bool, error) {
	if !isBuiltInScorerName(inv.ComponentName) {
		return InvocationResult{}, false, nil
	}
	if inv.ComponentType != "" && inv.ComponentType != ComponentTypeScorer {
		return InvocationResult{}, true, ErrComponentNotFound
	}
	var request ScorerRequest
	if len(inv.Input) > 0 {
		if err := json.Unmarshal(inv.Input, &request); err != nil {
			result := scorerInputError("Invalid scorer input JSON: " + err.Error())
			encoded, marshalErr := json.Marshal(result)
			return InvocationResult{Output: encoded, LeaseID: inv.LeaseID}, true, marshalErr
		}
	}
	runCtx := newContext(ctx, inv, w.checkpointWriter, canonicalProjectID(w.invocationMetadata(inv)), w.stateStore)
	result, err := DefaultScorerRegistry().Run(runCtx, inv.ComponentName, request)
	if err != nil {
		return InvocationResult{LeaseID: inv.LeaseID, Events: runCtx.Events()}, true, err
	}
	encoded, err := json.Marshal(result)
	return InvocationResult{Output: encoded, LeaseID: inv.LeaseID, Events: runCtx.Events()}, true, err
}
