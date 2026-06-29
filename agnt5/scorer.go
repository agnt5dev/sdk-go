package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// ScorerRequest is passed to scorer handlers.
type ScorerRequest struct {
	Input    any            `json:"input,omitempty"`
	Output   any            `json:"output,omitempty"`
	Expected any            `json:"expected,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Events   []RunEvent     `json:"events,omitempty"`
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

// ScorerConfig describes a registered scorer.
type ScorerConfig struct {
	Name        string
	Description string
	Handler     ScorerHandler
	Metadata    map[string]any
}

// ScorerRegistry stores scorers by name.
type ScorerRegistry struct {
	mu      sync.RWMutex
	scorers map[string]ScorerConfig
}

func NewScorerRegistry() *ScorerRegistry {
	return &ScorerRegistry{scorers: make(map[string]ScorerConfig)}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.scorers[config.Name]; exists {
		return ErrDuplicateComponent
	}
	config.Metadata = cloneAnyMap(config.Metadata)
	r.scorers[config.Name] = config
	return nil
}

func (r *ScorerRegistry) Get(name string) (ScorerConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	config, ok := r.scorers[name]
	if !ok {
		return ScorerConfig{}, false
	}
	config.Metadata = cloneAnyMap(config.Metadata)
	return config, true
}

func (r *ScorerRegistry) List() []ScorerConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.scorers))
	for name := range r.scorers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ScorerConfig, 0, len(names))
	for _, name := range names {
		out = append(out, r.scorers[name])
	}
	return out
}

func (r *ScorerRegistry) Run(ctx context.Context, name string, request ScorerRequest) (ScorerResult, error) {
	config, ok := r.Get(name)
	if !ok {
		return ScorerResult{}, errors.New("agnt5: scorer not found: " + name)
	}
	return config.Handler(ctx, request)
}

func (r *ScorerRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scorers = make(map[string]ScorerConfig)
}

// RegisterScorer registers a scorer as a worker component.
func RegisterScorer(w *Worker, config ScorerConfig, opts ...ComponentOption) error {
	if err := DefaultScorerRegistry().Register(config); err != nil {
		return err
	}
	return RegisterRaw(w, config.Name, ComponentTypeScorer, func(ctx *Context, input []byte) ([]byte, error) {
		var req ScorerRequest
		if len(input) > 0 {
			if err := json.Unmarshal(input, &req); err != nil {
				return nil, err
			}
		}
		out, err := config.Handler(ctx, req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
}

// ExactMatchScorer returns a deterministic exact-match scorer.
func ExactMatchScorer() ScorerConfig {
	return ScorerConfig{
		Name:        "exact_match",
		Description: "Passes when output equals expected.",
		Handler: func(_ context.Context, req ScorerRequest) (ScorerResult, error) {
			passed := reflect.DeepEqual(req.Output, req.Expected)
			score := 0.0
			if passed {
				score = 1
			}
			return ScorerResult{Score: score, Passed: passed}, nil
		},
	}
}

// ContainsScorer returns a string containment scorer.
func ContainsScorer() ScorerConfig {
	return ScorerConfig{
		Name:        "contains",
		Description: "Passes when output contains expected text.",
		Handler: func(_ context.Context, req ScorerRequest) (ScorerResult, error) {
			needle, _ := req.Expected.(string)
			haystack := formatScorerValue(req.Output)
			passed := needle != "" && strings.Contains(haystack, needle)
			score := 0.0
			if passed {
				score = 1
			}
			return ScorerResult{Score: score, Passed: passed}, nil
		},
	}
}

func formatScorerValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		encoded, err := json.Marshal(v)
		if err == nil {
			return string(encoded)
		}
		return fmt.Sprint(v)
	}
}
