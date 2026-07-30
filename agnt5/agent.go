package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// AgentInput is the default dispatch input for an Agent component.
type AgentInput struct {
	Message  string    `json:"message"`
	Messages []Message `json:"messages,omitempty"`
}

// AgentResult is returned by Agent.Run.
type AgentResult struct {
	AgentName       string          `json:"agent_name"`
	Response        string          `json:"response"`
	Messages        []Message       `json:"messages,omitempty"`
	ToolCalls       int             `json:"tool_calls,omitempty"`
	ToolCallDetails []AgentToolCall `json:"tool_call_details,omitempty"`
	HandoffTo       string          `json:"handoff_to,omitempty"`
	HandoffMetadata map[string]any  `json:"handoff_metadata,omitempty"`
	Metadata        map[string]any  `json:"metadata,omitempty"`
}

// AgentToolCall records a tool execution requested by a model.
type AgentToolCall struct {
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Iteration int            `json:"iteration"`
	Result    any            `json:"result,omitempty"`
	Error     string         `json:"error,omitempty"`
	Handoff   string         `json:"handoff,omitempty"`
}

// Handoff exposes another agent as a callable transfer target.
type Handoff struct {
	Agent           *Agent         `json:"-"`
	Description     string         `json:"description,omitempty"`
	ToolName        string         `json:"tool_name,omitempty"`
	PassFullHistory bool           `json:"pass_full_history,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// Agent is a small provider-neutral agent loop.
type Agent struct {
	Name         string
	Instructions string
	Model        LanguageModel
	Tools        []Tool
	Handoffs     []Handoff
	MaxTurns     int
	Cache        *PromptCache
	// Deprecated compatibility aliases. Prefer Cache.
	CacheControl bool
	CacheTTL     string
}

// AgentOption mutates Agent construction.
type AgentOption func(*Agent)

func WithAgentInstructions(instructions string) AgentOption {
	return func(a *Agent) { a.Instructions = instructions }
}

func WithAgentModel(model LanguageModel) AgentOption {
	return func(a *Agent) { a.Model = model }
}

func WithAgentTools(tools ...Tool) AgentOption {
	return func(a *Agent) { a.Tools = append(a.Tools, tools...) }
}

func WithAgentHandoffs(handoffs ...Handoff) AgentOption {
	return func(a *Agent) { a.Handoffs = append(a.Handoffs, handoffs...) }
}

func WithAgentMaxTurns(maxTurns int) AgentOption {
	return func(a *Agent) {
		if maxTurns > 0 {
			a.MaxTurns = maxTurns
		}
	}
}

func WithAgentCacheControl(enabled bool, ttl string) AgentOption {
	return func(a *Agent) {
		a.CacheControl = enabled
		a.CacheTTL = ttl
		a.Cache = &PromptCache{Enabled: enabled || strings.TrimSpace(ttl) != "", TTL: ttl}
	}
}

func WithAgentPromptCache(cache *PromptCache) AgentOption {
	return func(a *Agent) {
		if cache == nil {
			a.Cache = nil
			a.CacheControl = false
			a.CacheTTL = ""
			return
		}
		copied := *cache
		if copied.TTL != "" || copied.Key != "" || copied.Retention != "" || copied.Resource != "" {
			copied.Enabled = true
		}
		a.Cache = &copied
		a.CacheControl = copied.Enabled
		a.CacheTTL = copied.TTL
	}
}

// HandoffOption mutates handoff construction.
type HandoffOption func(*Handoff)

func WithHandoffDescription(description string) HandoffOption {
	return func(h *Handoff) { h.Description = description }
}

func WithHandoffToolName(toolName string) HandoffOption {
	return func(h *Handoff) { h.ToolName = toolName }
}

func WithHandoffFullHistory(pass bool) HandoffOption {
	return func(h *Handoff) { h.PassFullHistory = pass }
}

func WithHandoffMetadata(metadata map[string]any) HandoffOption {
	return func(h *Handoff) { h.Metadata = cloneAnyMap(metadata) }
}

// NewHandoff exposes an agent as a transfer target for another agent.
func NewHandoff(agent *Agent, opts ...HandoffOption) (Handoff, error) {
	if agent == nil {
		return Handoff{}, errors.New("agnt5: nil handoff agent")
	}
	handoff := Handoff{
		Agent:       agent,
		Description: "Transfer to " + agent.Name,
		ToolName:    handoffToolName(agent.Name),
		Metadata:    map[string]any{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&handoff)
		}
	}
	if strings.TrimSpace(handoff.ToolName) == "" {
		handoff.ToolName = handoffToolName(agent.Name)
	}
	return handoff, nil
}

// NewAgent constructs an Agent.
func NewAgent(name string, opts ...AgentOption) (*Agent, error) {
	if name == "" {
		return nil, ErrInvalidComponentName
	}
	agent := &Agent{Name: name, Model: StaticModel{}, MaxTurns: 1}
	for _, opt := range opts {
		if opt != nil {
			opt(agent)
		}
	}
	return agent, nil
}

// Run executes the agent loop with the configured LanguageModel.
func (a *Agent) Run(ctx *Context, input AgentInput) (AgentResult, error) {
	if a == nil {
		return AgentResult{}, errors.New("agnt5: nil agent")
	}
	if a.Model == nil {
		return AgentResult{}, errors.New("agnt5: agent model is required")
	}
	if ctx == nil {
		ctx = newContext(context.Background(), Invocation{ID: a.Name, RunID: a.Name, ComponentName: a.Name, ComponentType: ComponentTypeAgent}, nil, "")
	}

	messages := a.initialMessages(input)
	tools := append([]Tool{}, a.Tools...)
	handoffsByTool := a.handoffsByToolName()
	for _, handoff := range a.Handoffs {
		if handoff.Agent == nil {
			continue
		}
		toolName := handoff.ToolName
		if strings.TrimSpace(toolName) == "" {
			toolName = handoffToolName(handoff.Agent.Name)
		}
		if normalized, ok := handoffsByTool[toolName]; ok {
			tools = append(tools, normalized.tool())
		}
	}

	maxTurns := a.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 1
	}

	agentParentCorrelationID := ctx.parentCorrelationID()
	agentCorrelationID := agentParentCorrelationID
	if !ctx.managesAgent(a.Name) {
		agentCorrelationID = newCorrelationID("agent")
	}
	if agentCorrelationID == "" {
		agentCorrelationID = newCorrelationID("agent")
	}
	agentModel, _ := languageModelIdentity(a.Model, GenerateRequest{})
	toolNames := make([]string, 0, len(tools))
	for _, tool := range tools {
		toolNames = append(toolNames, tool.Name)
	}
	a.emitLifecycle(ctx, lifecycleEvent(
		"agent.started",
		a.Name,
		"agent",
		agentCorrelationID,
		agentParentCorrelationID,
		map[string]any{
			"agent_name":     a.Name,
			"agent_model":    agentModel,
			"tool_names":     toolNames,
			"max_iterations": maxTurns,
			"input_data":     input,
		},
	))
	agentContext := ctx.withParentCorrelationID(agentCorrelationID)

	var toolCalls []AgentToolCall
	for iteration := 1; iteration <= maxTurns; iteration++ {
		iterationCorrelationID := newCorrelationID("iteration")
		iterationStartedAt := time.Now()
		_ = agentContext.Emit(lifecycleEvent(
			"agent.iteration.started",
			a.Name,
			"agent",
			iterationCorrelationID,
			agentCorrelationID,
			map[string]any{
				"agent_name":     a.Name,
				"operation":      "iteration",
				"iteration":      iteration,
				"max_iterations": maxTurns,
				"input_data":     map[string]any{"iteration": iteration, "max_iterations": maxTurns},
			},
		))
		iterationContext := agentContext.withParentCorrelationID(iterationCorrelationID)

		resp, err := iterationContext.Generate(a.Model, GenerateRequest{
			Messages: messages,
			Tools:    tools,
			Cache:    a.Cache,
		})
		if err != nil {
			a.emitLifecycle(ctx, lifecycleEvent(
				"agent.failed",
				a.Name,
				"agent",
				agentCorrelationID,
				agentParentCorrelationID,
				map[string]any{
					"agent_name": a.Name,
					"iterations": iteration - 1,
					"error":      err.Error(),
				},
			))
			return AgentResult{}, err
		}

		assistantMessage := Message{
			Role:      MessageRoleAssistant,
			Content:   resp.Content,
			ToolCalls: cloneToolCalls(resp.ToolCalls),
		}
		messages = append(messages, assistantMessage)
		if len(resp.ToolCalls) == 0 {
			_ = agentContext.Emit(lifecycleEvent(
				"agent.iteration.completed",
				a.Name,
				"agent",
				iterationCorrelationID,
				agentCorrelationID,
				map[string]any{
					"agent_name":       a.Name,
					"operation":        "iteration",
					"iteration":        iteration,
					"has_tool_calls":   false,
					"tool_calls_count": 0,
					"duration_ms":      time.Since(iterationStartedAt).Milliseconds(),
				},
			))
			result := AgentResult{
				AgentName:       a.Name,
				Response:        resp.Content,
				Messages:        cloneMessages(messages),
				ToolCalls:       len(toolCalls),
				ToolCallDetails: cloneAgentToolCalls(toolCalls),
			}
			a.emitLifecycle(ctx, lifecycleEvent(
				"agent.completed",
				a.Name,
				"agent",
				agentCorrelationID,
				agentParentCorrelationID,
				agentResultEventData(result, iteration),
			))
			return result, nil
		}

		for callIndex, call := range resp.ToolCalls {
			if call.ID == "" {
				call.ID = fmt.Sprintf("call_%d_%d", iteration, callIndex+1)
			}
			record := AgentToolCall{
				ID:        call.ID,
				Name:      call.Name,
				Arguments: cloneAnyMap(call.Arguments),
				Iteration: iteration,
			}
			toolCorrelationID := newCorrelationID("tool")
			toolStartedAt := time.Now()
			_ = iterationContext.Emit(lifecycleEvent(
				"tool_call.started",
				call.Name,
				"agent",
				toolCorrelationID,
				iterationCorrelationID,
				map[string]any{
					"agent_name":   a.Name,
					"operation":    "tool_call",
					"tool_name":    call.Name,
					"tool_call_id": call.ID,
					"input_data":   cloneAnyMap(call.Arguments),
					"tool_call":    record,
				},
			))

			if handoff, ok := handoffsByTool[call.Name]; ok {
				result, err := a.runHandoff(
					iterationContext,
					handoff,
					input,
					messages,
					call,
					record,
					toolCalls,
					iteration,
					agentCorrelationID,
					agentParentCorrelationID,
					iterationCorrelationID,
					toolCorrelationID,
					toolStartedAt,
					iterationStartedAt,
				)
				if err != nil {
					return AgentResult{}, err
				}
				return result, nil
			}

			tool, ok := a.lookupTool(call.Name)
			if !ok || tool.Handler == nil {
				err := errors.New("agnt5: tool not found: " + call.Name)
				record.Error = err.Error()
				toolCalls = append(toolCalls, record)
				_ = iterationContext.Emit(lifecycleEvent(
					"tool_call.failed",
					call.Name,
					"agent",
					toolCorrelationID,
					iterationCorrelationID,
					map[string]any{
						"agent_name":   a.Name,
						"operation":    "tool_call",
						"tool_name":    call.Name,
						"tool_call_id": call.ID,
						"error":        err.Error(),
						"tool_call":    record,
						"duration_ms":  time.Since(toolStartedAt).Milliseconds(),
					},
				))
				a.emitLifecycle(ctx, lifecycleEvent(
					"agent.failed",
					a.Name,
					"agent",
					agentCorrelationID,
					agentParentCorrelationID,
					map[string]any{"agent_name": a.Name, "iterations": iteration - 1, "error": err.Error()},
				))
				return AgentResult{}, err
			}

			output, err := tool.Handler(iterationContext, cloneAnyMap(call.Arguments))
			if err != nil {
				record.Error = err.Error()
				toolCalls = append(toolCalls, record)
				_ = iterationContext.Emit(lifecycleEvent(
					"tool_call.failed",
					call.Name,
					"agent",
					toolCorrelationID,
					iterationCorrelationID,
					map[string]any{
						"agent_name":   a.Name,
						"operation":    "tool_call",
						"tool_name":    call.Name,
						"tool_call_id": call.ID,
						"error":        err.Error(),
						"tool_call":    record,
						"duration_ms":  time.Since(toolStartedAt).Milliseconds(),
					},
				))
				a.emitLifecycle(ctx, lifecycleEvent(
					"agent.failed",
					a.Name,
					"agent",
					agentCorrelationID,
					agentParentCorrelationID,
					map[string]any{"agent_name": a.Name, "iterations": iteration - 1, "error": err.Error()},
				))
				return AgentResult{}, err
			}

			record.Result = output
			toolCalls = append(toolCalls, record)
			messages = append(messages, Message{
				Role:       MessageRoleTool,
				Name:       call.Name,
				ToolCallID: call.ID,
				Content:    serializeAgentValue(output),
			})
			_ = iterationContext.Emit(lifecycleEvent(
				"tool_call.completed",
				call.Name,
				"agent",
				toolCorrelationID,
				iterationCorrelationID,
				map[string]any{
					"agent_name":   a.Name,
					"operation":    "tool_call",
					"tool_name":    call.Name,
					"tool_call_id": call.ID,
					"output_data":  output,
					"tool_call":    record,
					"duration_ms":  time.Since(toolStartedAt).Milliseconds(),
				},
			))
		}

		_ = agentContext.Emit(lifecycleEvent(
			"agent.iteration.completed",
			a.Name,
			"agent",
			iterationCorrelationID,
			agentCorrelationID,
			map[string]any{
				"agent_name":       a.Name,
				"operation":        "iteration",
				"iteration":        iteration,
				"has_tool_calls":   true,
				"tool_calls_count": len(resp.ToolCalls),
				"duration_ms":      time.Since(iterationStartedAt).Milliseconds(),
			},
		))
	}

	err := errors.New("agnt5: agent max turns exceeded")
	a.emitLifecycle(ctx, lifecycleEvent(
		"agent.failed",
		a.Name,
		"agent",
		agentCorrelationID,
		agentParentCorrelationID,
		map[string]any{"agent_name": a.Name, "iterations": maxTurns, "error": err.Error()},
	))
	return AgentResult{}, err
}

// RegisterAgent registers an Agent as a worker component.
func RegisterAgent(w *Worker, agent *Agent, opts ...ComponentOption) error {
	if agent == nil {
		return ErrNilHandler
	}
	return RegisterRaw(w, agent.Name, ComponentTypeAgent, func(ctx *Context, input []byte) ([]byte, error) {
		ctx.setManagedAgent(agent.Name)
		var in AgentInput
		if len(input) > 0 {
			if err := json.Unmarshal(input, &in); err != nil {
				return nil, err
			}
		}
		out, err := agent.Run(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
}

func (a *Agent) emitLifecycle(ctx *Context, event Event) {
	if ctx == nil || ctx.managesAgent(a.Name) {
		return
	}
	_ = ctx.Emit(event)
}

// AgentRegistry stores named agents for application-level orchestration.
type AgentRegistry struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{agents: make(map[string]*Agent)}
}

var defaultAgentRegistry = NewAgentRegistry()

func DefaultAgentRegistry() *AgentRegistry {
	return defaultAgentRegistry
}

func (r *AgentRegistry) Register(agent *Agent) error {
	if agent == nil {
		return errors.New("agnt5: nil agent")
	}
	if strings.TrimSpace(agent.Name) == "" {
		return ErrInvalidComponentName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.agents[agent.Name]; exists {
		return ErrDuplicateComponent
	}
	r.agents[agent.Name] = agent
	return nil
}

func (r *AgentRegistry) Get(name string) (*Agent, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	agent, ok := r.agents[name]
	return agent, ok
}

func (r *AgentRegistry) List() []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.agents))
	for name := range r.agents {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*Agent, 0, len(names))
	for _, name := range names {
		out = append(out, r.agents[name])
	}
	return out
}

func (r *AgentRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents = make(map[string]*Agent)
}

// AgentManager provides a small orchestration facade over an AgentRegistry.
type AgentManager struct {
	registry *AgentRegistry
}

func NewAgentManager(registry *AgentRegistry) *AgentManager {
	if registry == nil {
		registry = NewAgentRegistry()
	}
	return &AgentManager{registry: registry}
}

func (m *AgentManager) Register(agent *Agent) error {
	if m == nil {
		return errors.New("agnt5: nil agent manager")
	}
	return m.registry.Register(agent)
}

func (m *AgentManager) Get(name string) (*Agent, bool) {
	if m == nil || m.registry == nil {
		return nil, false
	}
	return m.registry.Get(name)
}

func (m *AgentManager) List() []*Agent {
	if m == nil || m.registry == nil {
		return nil
	}
	return m.registry.List()
}

func (m *AgentManager) Run(ctx *Context, name string, input AgentInput) (AgentResult, error) {
	if m == nil || m.registry == nil {
		return AgentResult{}, errors.New("agnt5: nil agent manager")
	}
	agent, ok := m.registry.Get(name)
	if !ok {
		return AgentResult{}, errors.New("agnt5: agent not found: " + name)
	}
	return agent.Run(ctx, input)
}

func (a *Agent) initialMessages(input AgentInput) []Message {
	messages := cloneMessages(input.Messages)
	if a.Instructions != "" {
		messages = append([]Message{{Role: MessageRoleSystem, Content: a.Instructions}}, messages...)
	}
	if input.Message != "" {
		messages = append(messages, Message{Role: MessageRoleUser, Content: input.Message})
	}
	return messages
}

func (a *Agent) lookupTool(name string) (Tool, bool) {
	for _, tool := range a.Tools {
		if tool.Name == name {
			tool.Schema = cloneAnyMap(tool.Schema)
			tool.Metadata = cloneAnyMap(tool.Metadata)
			return tool, true
		}
	}
	return DefaultToolRegistry().Get(name)
}

func (a *Agent) handoffsByToolName() map[string]Handoff {
	out := make(map[string]Handoff, len(a.Handoffs))
	for _, handoff := range a.Handoffs {
		if handoff.Agent == nil {
			continue
		}
		if strings.TrimSpace(handoff.ToolName) == "" {
			handoff.ToolName = handoffToolName(handoff.Agent.Name)
		}
		handoff.Metadata = cloneAnyMap(handoff.Metadata)
		out[handoff.ToolName] = handoff
	}
	return out
}

func (h Handoff) tool() Tool {
	description := h.Description
	if description == "" && h.Agent != nil {
		description = "Transfer to " + h.Agent.Name
	}
	return Tool{
		Name:        h.ToolName,
		Description: description,
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"message": map[string]any{
					"type":        "string",
					"description": "Message or task to pass to the target agent.",
				},
			},
		},
		Metadata: cloneAnyMap(h.Metadata),
	}
}

func (a *Agent) runHandoff(
	ctx *Context,
	handoff Handoff,
	input AgentInput,
	messages []Message,
	call ToolCall,
	record AgentToolCall,
	previousCalls []AgentToolCall,
	iteration int,
	agentCorrelationID string,
	agentParentCorrelationID string,
	iterationCorrelationID string,
	toolCorrelationID string,
	toolStartedAt time.Time,
	iterationStartedAt time.Time,
) (AgentResult, error) {
	message := firstHandoffMessage(call.Arguments)
	if message == "" {
		message = input.Message
	}
	handoffInput := AgentInput{Message: message}
	if handoff.PassFullHistory {
		handoffInput.Messages = cloneMessages(messages)
	}
	result, err := handoff.Agent.Run(ctx, handoffInput)
	if err != nil {
		record.Error = err.Error()
		toolCalls := append(cloneAgentToolCalls(previousCalls), record)
		_ = ctx.Emit(lifecycleEvent(
			"tool_call.failed",
			call.Name,
			"agent",
			toolCorrelationID,
			iterationCorrelationID,
			map[string]any{
				"agent_name":   a.Name,
				"operation":    "tool_call",
				"tool_name":    call.Name,
				"tool_call_id": call.ID,
				"error":        err.Error(),
				"tool_call":    record,
				"duration_ms":  time.Since(toolStartedAt).Milliseconds(),
			},
		))
		a.emitLifecycle(ctx, lifecycleEvent(
			"agent.failed",
			a.Name,
			"agent",
			agentCorrelationID,
			agentParentCorrelationID,
			map[string]any{
				"agent_name": a.Name,
				"iterations": iteration - 1,
				"error":      err.Error(),
				"tool_calls": toolCalls,
			},
		))
		return AgentResult{}, err
	}

	record.Result = map[string]any{"agent_name": result.AgentName, "response": result.Response}
	record.Handoff = handoff.Agent.Name
	toolCalls := append(cloneAgentToolCalls(previousCalls), record)
	_ = ctx.Emit(lifecycleEvent(
		"tool_call.completed",
		call.Name,
		"agent",
		toolCorrelationID,
		iterationCorrelationID,
		map[string]any{
			"agent_name":   a.Name,
			"operation":    "tool_call",
			"tool_name":    call.Name,
			"tool_call_id": call.ID,
			"output_data":  record.Result,
			"tool_call":    record,
			"duration_ms":  time.Since(toolStartedAt).Milliseconds(),
		},
	))
	_ = ctx.Emit(lifecycleEvent(
		"agent.iteration.completed",
		a.Name,
		"agent",
		iterationCorrelationID,
		agentCorrelationID,
		map[string]any{
			"agent_name":       a.Name,
			"operation":        "iteration",
			"iteration":        iteration,
			"has_tool_calls":   true,
			"tool_calls_count": 1,
			"handoff_to":       handoff.Agent.Name,
			"duration_ms":      time.Since(iterationStartedAt).Milliseconds(),
		},
	))

	final := AgentResult{
		AgentName:       a.Name,
		Response:        result.Response,
		Messages:        append(cloneMessages(messages), result.Messages...),
		ToolCalls:       len(toolCalls),
		ToolCallDetails: cloneAgentToolCalls(toolCalls),
		HandoffTo:       handoff.Agent.Name,
		HandoffMetadata: cloneAnyMap(handoff.Metadata),
		Metadata: map[string]any{
			"handoff_result": result,
		},
	}
	a.emitLifecycle(ctx, lifecycleEvent(
		"agent.completed",
		a.Name,
		"agent",
		agentCorrelationID,
		agentParentCorrelationID,
		agentResultEventData(final, iteration),
	))
	return final, nil
}

func agentResultEventData(result AgentResult, iterations int) map[string]any {
	data := map[string]any{}
	if payload, err := json.Marshal(result); err == nil {
		_ = json.Unmarshal(payload, &data)
	}
	data["iterations"] = iterations
	data["tool_calls_count"] = result.ToolCalls
	data["output_length"] = len(result.Response)
	return data
}

func firstHandoffMessage(args map[string]any) string {
	for _, key := range []string{"message", "input", "task", "prompt"} {
		if value, ok := args[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func handoffToolName(agentName string) string {
	name := strings.ToLower(strings.TrimSpace(agentName))
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			if b.Len() > 0 && !strings.HasSuffix(b.String(), "_") {
				b.WriteByte('_')
			}
		}
	}
	slug := strings.Trim(b.String(), "_")
	if slug == "" {
		slug = "agent"
	}
	return "transfer_to_" + slug
}

func serializeAgentValue(value any) string {
	if value == nil {
		return "null"
	}
	if text, ok := value.(string); ok {
		return text
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

func cloneMessages(in []Message) []Message {
	if len(in) == 0 {
		return nil
	}
	out := make([]Message, len(in))
	for i, message := range in {
		message.ToolCalls = cloneToolCalls(message.ToolCalls)
		out[i] = message
	}
	return out
}

func cloneAgentToolCalls(in []AgentToolCall) []AgentToolCall {
	if len(in) == 0 {
		return nil
	}
	out := make([]AgentToolCall, len(in))
	for i, call := range in {
		call.Arguments = cloneAnyMap(call.Arguments)
		out[i] = call
	}
	return out
}
