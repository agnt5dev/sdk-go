package agnt5

import (
	"encoding/json"
	"strconv"
	"strings"
)

type rawInvoker func(*Context, []byte) ([]byte, error)

// Component describes a registered Go handler.
type Component struct {
	Name     string
	Type     ComponentType
	Config   map[string]string
	Metadata map[string]string

	invoke rawInvoker
}

// ComponentOption mutates component registration metadata.
type ComponentOption func(*Component)

// WithComponentMetadata adds metadata to a component registration.
func WithComponentMetadata(metadata map[string]string) ComponentOption {
	return func(c *Component) {
		for key, value := range metadata {
			c.Metadata[key] = value
		}
	}
}

// WithComponentConfig adds config values to a component registration.
func WithComponentConfig(config map[string]string) ComponentOption {
	return func(c *Component) {
		for key, value := range config {
			c.Config[key] = value
		}
	}
}

// WithRetry configures component retry metadata.
func WithRetry(maxAttempts, initialIntervalMS, maxIntervalMS int) ComponentOption {
	return func(c *Component) {
		if maxAttempts > 0 {
			c.Config["max_attempts"] = intString(maxAttempts)
		}
		if initialIntervalMS > 0 {
			c.Config["initial_interval_ms"] = intString(initialIntervalMS)
		}
		if maxIntervalMS > 0 {
			c.Config["max_interval_ms"] = intString(maxIntervalMS)
		}
	}
}

// WithBackoff configures component backoff metadata.
func WithBackoff(backoffType string, multiplier float64) ComponentOption {
	return func(c *Component) {
		if backoffType != "" {
			c.Config["backoff_type"] = backoffType
		}
		if multiplier > 0 {
			c.Config["backoff_multiplier"] = floatString(multiplier)
		}
	}
}

func newComponent(name string, componentType ComponentType, invoker rawInvoker, opts ...ComponentOption) (Component, error) {
	if strings.TrimSpace(name) == "" {
		return Component{}, ErrInvalidComponentName
	}
	if invoker == nil {
		return Component{}, ErrNilHandler
	}
	component := Component{
		Name:     name,
		Type:     componentType,
		Config:   make(map[string]string),
		Metadata: make(map[string]string),
		invoke:   invoker,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&component)
		}
	}
	return component, nil
}

// Info returns the registration descriptor for this component.
func (c Component) Info() ComponentInfo {
	return ComponentInfo{
		Name:     c.Name,
		Type:     c.Type,
		Config:   cloneStringMap(c.Config),
		Metadata: cloneStringMap(c.Metadata),
	}
}

func typedInvoker[In any, Out any](handler func(*Context, In) (Out, error)) rawInvoker {
	return func(ctx *Context, input []byte) ([]byte, error) {
		var in In
		if len(input) > 0 {
			if err := json.Unmarshal(input, &in); err != nil {
				return nil, err
			}
		}
		out, err := handler(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}
}

func rawComponentInvoker(handler func(*Context, []byte) ([]byte, error)) rawInvoker {
	return func(ctx *Context, input []byte) ([]byte, error) {
		return handler(ctx, input)
	}
}

func intString(value int) string {
	return strconv.Itoa(value)
}

func floatString(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
