package agnt5

import (
	"sort"
	"sync"
)

// Registry stores registered components by name.
type Registry struct {
	mu         sync.RWMutex
	components map[string]Component
}

// NewRegistry creates an empty component registry.
func NewRegistry() *Registry {
	return &Registry{components: make(map[string]Component)}
}

// RegisterFunction registers a typed function component.
func RegisterFunction[In any, Out any](w *Worker, name string, handler func(*Context, In) (Out, error), opts ...ComponentOption) error {
	if w == nil {
		return ErrNilWorker
	}
	if handler == nil {
		return ErrNilHandler
	}
	component, err := newComponent(name, ComponentTypeFunction, typedInvoker(handler), opts...)
	if err != nil {
		return err
	}
	return w.registry.Register(component)
}

// RegisterWorkflow registers a typed workflow component.
func RegisterWorkflow[In any, Out any](w *Worker, name string, handler func(*Context, In) (Out, error), opts ...ComponentOption) error {
	if w == nil {
		return ErrNilWorker
	}
	if handler == nil {
		return ErrNilHandler
	}
	component, err := newComponent(name, ComponentTypeWorkflow, typedInvoker(handler), opts...)
	if err != nil {
		return err
	}
	return w.registry.Register(component)
}

// RegisterRaw registers an escape-hatch component that receives and returns raw JSON bytes.
func RegisterRaw(w *Worker, name string, componentType ComponentType, handler func(*Context, []byte) ([]byte, error), opts ...ComponentOption) error {
	if w == nil {
		return ErrNilWorker
	}
	if handler == nil {
		return ErrNilHandler
	}
	component, err := newComponent(name, componentType, rawComponentInvoker(handler), opts...)
	if err != nil {
		return err
	}
	return w.registry.Register(component)
}

// Register stores a component by name.
func (r *Registry) Register(component Component) error {
	if component.Name == "" {
		return ErrInvalidComponentName
	}
	if component.invoke == nil {
		return ErrNilHandler
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.components[component.Name]; exists {
		return ErrDuplicateComponent
	}
	r.components[component.Name] = component
	return nil
}

// Get returns a component by name.
func (r *Registry) Get(name string) (Component, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	component, ok := r.components[name]
	if !ok {
		return Component{}, false
	}
	component.Config = cloneStringMap(component.Config)
	component.Metadata = cloneStringMap(component.Metadata)
	return component, true
}

// List returns all registered components.
func (r *Registry) List() []Component {
	r.mu.RLock()
	defer r.mu.RUnlock()
	components := make([]Component, 0, len(r.components))
	for _, component := range r.components {
		component.Config = cloneStringMap(component.Config)
		component.Metadata = cloneStringMap(component.Metadata)
		components = append(components, component)
	}
	sort.Slice(components, func(i, j int) bool {
		return components[i].Name < components[j].Name
	})
	return components
}

// ComponentInfos returns deterministic component registration descriptors.
func (r *Registry) ComponentInfos() []ComponentInfo {
	components := r.List()
	infos := make([]ComponentInfo, len(components))
	for i, component := range components {
		infos[i] = component.Info()
	}
	return infos
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
