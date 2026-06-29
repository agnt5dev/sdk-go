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
	Triggers []TriggerSpec

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

// WithCron marks a workflow as scheduled with a cron expression.
func WithCron(expression string) ComponentOption {
	return func(c *Component) {
		if strings.TrimSpace(expression) != "" {
			c.Metadata["cron"] = strings.TrimSpace(expression)
		}
	}
}

// WithTriggers attaches runtime trigger declarations to a component.
func WithTriggers(triggers ...TriggerSpec) ComponentOption {
	return func(c *Component) {
		for _, trigger := range triggers {
			if strings.TrimSpace(trigger.TriggerType) == "" {
				continue
			}
			c.Triggers = append(c.Triggers, trigger)
		}
	}
}

// EventTrigger declares an event trigger for a workflow.
func EventTrigger(name string) TriggerSpec {
	return TriggerSpec{TriggerType: "event", EventName: strings.TrimSpace(name)}
}

// WebhookTrigger declares a webhook event trigger. Runtime webhooks dispatch as
// "{source}.{event}", matching Python and TypeScript SDK helpers.
func WebhookTrigger(source, event string) TriggerSpec {
	source = strings.TrimSpace(strings.ToLower(source))
	event = strings.TrimSpace(event)
	name := event
	if source != "" && event != "" {
		name = source + "." + event
	}
	return EventTrigger(name)
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
		Triggers: nil,
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
		Triggers: cloneTriggers(c.Triggers),
	}
}

func cloneTriggers(in []TriggerSpec) []TriggerSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]TriggerSpec, len(in))
	copy(out, in)
	return out
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
