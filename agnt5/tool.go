package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
)

// ToolHandler executes a tool call.
type ToolHandler func(context.Context, map[string]any) (any, error)

// Tool describes a callable tool.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Schema      map[string]any `json:"schema,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Handler     ToolHandler    `json:"-"`
}

// ToolOption mutates Tool construction.
type ToolOption func(*Tool)

func WithToolDescription(description string) ToolOption {
	return func(t *Tool) { t.Description = description }
}

func WithToolSchema(schema map[string]any) ToolOption {
	return func(t *Tool) { t.Schema = cloneAnyMap(schema) }
}

func WithToolMetadata(metadata map[string]any) ToolOption {
	return func(t *Tool) { t.Metadata = cloneAnyMap(metadata) }
}

// NewTool creates a Tool.
func NewTool(name string, handler ToolHandler, opts ...ToolOption) (Tool, error) {
	if strings.TrimSpace(name) == "" {
		return Tool{}, ErrInvalidComponentName
	}
	if handler == nil {
		return Tool{}, ErrNilHandler
	}
	tool := Tool{Name: name, Handler: handler, Schema: map[string]any{}, Metadata: map[string]any{}}
	for _, opt := range opts {
		if opt != nil {
			opt(&tool)
		}
	}
	return tool, nil
}

// ToolRegistry stores tools by name.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewToolRegistry creates an empty tool registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

var defaultToolRegistry = NewToolRegistry()

// DefaultToolRegistry returns the process-global tool registry.
func DefaultToolRegistry() *ToolRegistry {
	return defaultToolRegistry
}

func (r *ToolRegistry) Register(tool Tool) error {
	if strings.TrimSpace(tool.Name) == "" {
		return ErrInvalidComponentName
	}
	if tool.Handler == nil {
		return ErrNilHandler
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[tool.Name]; exists {
		return ErrDuplicateComponent
	}
	tool.Schema = cloneAnyMap(tool.Schema)
	tool.Metadata = cloneAnyMap(tool.Metadata)
	r.tools[tool.Name] = tool
	return nil
}

func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[name]
	if !ok {
		return Tool{}, false
	}
	tool.Schema = cloneAnyMap(tool.Schema)
	tool.Metadata = cloneAnyMap(tool.Metadata)
	return tool, true
}

func (r *ToolRegistry) List() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Tool, 0, len(names))
	for _, name := range names {
		tool := r.tools[name]
		tool.Schema = cloneAnyMap(tool.Schema)
		tool.Metadata = cloneAnyMap(tool.Metadata)
		out = append(out, tool)
	}
	return out
}

func (r *ToolRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools = make(map[string]Tool)
}

// CallTool executes a tool from this registry.
func (r *ToolRegistry) CallTool(ctx context.Context, name string, input map[string]any) (any, error) {
	tool, ok := r.Get(name)
	if !ok {
		return nil, errors.New("agnt5: tool not found: " + name)
	}
	return tool.Handler(ctx, input)
}

// RegisterTool registers a Tool both in the global tool registry and on a worker.
func RegisterTool(w *Worker, tool Tool, opts ...ComponentOption) error {
	if err := DefaultToolRegistry().Register(tool); err != nil {
		return err
	}
	return RegisterRaw(w, tool.Name, ComponentTypeTool, func(ctx *Context, input []byte) ([]byte, error) {
		var args map[string]any
		if len(input) > 0 {
			if err := json.Unmarshal(input, &args); err != nil {
				return nil, err
			}
		}
		out, err := tool.Handler(ctx, args)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
