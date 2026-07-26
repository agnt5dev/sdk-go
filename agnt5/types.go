package agnt5

// ComponentType identifies the kind of component registered with a worker.
type ComponentType string

const (
	ComponentTypeRun      ComponentType = "run"
	ComponentTypeFunction ComponentType = "function"
	ComponentTypeWorkflow ComponentType = "workflow"
	ComponentTypeAgent    ComponentType = "agent"
	ComponentTypeTool     ComponentType = "tool"
	ComponentTypeMCP      ComponentType = "mcp"
	ComponentTypeEntity   ComponentType = "entity"
	ComponentTypeScorer   ComponentType = "scorer"
	ComponentTypeChat     ComponentType = "chat"
)

// WorkerMode controls how the worker receives assignments from the runtime.
type WorkerMode string

const (
	WorkerModePush WorkerMode = "push"
	WorkerModePull WorkerMode = "pull"
)

// Invocation contains the runtime data needed to execute one component.
type Invocation struct {
	ID             string
	RunID          string
	ComponentName  string
	ComponentType  ComponentType
	Input          []byte
	Attempt        int
	Metadata       map[string]string
	LeaseID        string
	IsStreaming    bool
	StreamFallback bool
}

// InvocationResult is the language-local result of executing a component.
type InvocationResult struct {
	Output   []byte
	Metadata map[string]string
	LeaseID  string
	Events   []Event
}

// TriggerSpec declares a runtime trigger attached to a component registration.
type TriggerSpec struct {
	TriggerID        string `json:"trigger_id,omitempty"`
	TriggerType      string `json:"trigger_type"`
	EventName        string `json:"event_name,omitempty"`
	FilterExpression string `json:"filter_expression,omitempty"`
	InputMapping     string `json:"input_mapping,omitempty"`
	BatchWindowMS    int64  `json:"batch_window_ms,omitempty"`
	DelayExpression  string `json:"delay_expression,omitempty"`
}

// ComponentInfo is the SDK-local registration descriptor. Transport adapters
// convert this shape to the runtime protobuf ComponentInfo.
type ComponentInfo struct {
	Name         string
	Type         ComponentType
	InputSchema  map[string]any
	OutputSchema map[string]any
	Config       map[string]string
	Metadata     map[string]string
	Triggers     []TriggerSpec
}
