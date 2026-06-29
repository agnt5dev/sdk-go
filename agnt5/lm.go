package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// MessageRole identifies a chat message role.
type MessageRole string

const (
	MessageRoleSystem    MessageRole = "system"
	MessageRoleUser      MessageRole = "user"
	MessageRoleAssistant MessageRole = "assistant"
	MessageRoleTool      MessageRole = "tool"
)

// Message is one LLM or agent conversation message.
type Message struct {
	Role       MessageRole `json:"role"`
	Content    string      `json:"content"`
	Name       string      `json:"name,omitempty"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
}

// ToolCall is a provider-neutral request to execute a tool.
type ToolCall struct {
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Raw       map[string]any `json:"raw,omitempty"`
}

// GenerateRequest is a provider-neutral model request.
type GenerateRequest struct {
	Model       string         `json:"model,omitempty"`
	Messages    []Message      `json:"messages"`
	Tools       []Tool         `json:"tools,omitempty"`
	Temperature *float64       `json:"temperature,omitempty"`
	MaxTokens   *int           `json:"max_tokens,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// TokenUsage captures provider token accounting.
type TokenUsage struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
	TotalTokens  int `json:"total_tokens,omitempty"`
}

// GenerateResponse is a provider-neutral model response.
type GenerateResponse struct {
	ID           string         `json:"id,omitempty"`
	Model        string         `json:"model,omitempty"`
	Content      string         `json:"content"`
	Usage        TokenUsage     `json:"usage,omitempty"`
	FinishReason string         `json:"finish_reason,omitempty"`
	ToolCalls    []ToolCall     `json:"tool_calls,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// LanguageModel is the provider boundary used by Agent and direct LLM helpers.
type LanguageModel interface {
	Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error)
}

// StaticModel is a deterministic model useful for tests and examples.
type StaticModel struct {
	Model     string
	Content   string
	ToolCalls []ToolCall
}

func (m StaticModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	model := m.Model
	if model == "" {
		model = request.Model
	}
	content := m.Content
	if content == "" && len(request.Messages) > 0 {
		content = request.Messages[len(request.Messages)-1].Content
	}
	return GenerateResponse{Model: model, Content: content, ToolCalls: cloneToolCalls(m.ToolCalls)}, nil
}

// ScriptedModel returns a deterministic sequence of responses.
type ScriptedModel struct {
	mu        sync.Mutex
	Responses []GenerateResponse
}

func (m *ScriptedModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Responses) == 0 {
		return StaticModel{}.Generate(context.Background(), request)
	}
	resp := m.Responses[0]
	m.Responses = m.Responses[1:]
	return resp, nil
}

// OpenAIConfig configures an OpenAI-compatible chat-completions provider.
type OpenAIConfig struct {
	BaseURL      string
	APIKey       string
	APIKeyHeader string
	AuthScheme   string
	Model        string
	Organization string
	HTTPClient   *http.Client
	Headers      map[string]string
	Path         string
}

// OpenAIModel is a minimal OpenAI-compatible LanguageModel.
type OpenAIModel struct {
	config OpenAIConfig
}

// NewOpenAIModel constructs an OpenAI-compatible model.
func NewOpenAIModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.openai.com"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &OpenAIModel{config: config}
}

func (m *OpenAIModel) Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error) {
	if m == nil {
		return GenerateResponse{}, errors.New("agnt5: nil model")
	}
	model := request.Model
	if model == "" {
		model = m.config.Model
	}
	if model == "" {
		return GenerateResponse{}, errors.New("agnt5: model is required")
	}
	payload := map[string]any{
		"model":    model,
		"messages": openAIMessages(request.Messages),
	}
	if len(request.Tools) > 0 {
		payload["tools"] = openAITools(request.Tools)
	}
	if request.Temperature != nil {
		payload["temperature"] = *request.Temperature
	}
	if request.MaxTokens != nil {
		payload["max_tokens"] = *request.MaxTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return GenerateResponse{}, err
	}
	path := m.config.Path
	if path == "" {
		path = "/v1/chat/completions"
	}
	endpoint := strings.TrimRight(m.config.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return GenerateResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.config.APIKey != "" {
		if m.config.APIKeyHeader != "" {
			req.Header.Set(m.config.APIKeyHeader, m.config.APIKey)
		} else {
			scheme := m.config.AuthScheme
			if scheme == "" {
				scheme = "Bearer"
			}
			req.Header.Set("Authorization", scheme+" "+m.config.APIKey)
		}
	}
	if m.config.Organization != "" {
		req.Header.Set("OpenAI-Organization", m.config.Organization)
	}
	for key, value := range m.config.Headers {
		req.Header.Set(key, value)
	}
	resp, err := m.config.HTTPClient.Do(req)
	if err != nil {
		return GenerateResponse{}, err
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return GenerateResponse{}, err
	}
	if resp.StatusCode >= 400 {
		return GenerateResponse{}, errors.New("agnt5: model provider returned HTTP " + intString(resp.StatusCode))
	}
	content := ""
	var toolCalls []ToolCall
	finishReason := ""
	if choices, ok := decoded["choices"].([]any); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			finishReason, _ = choice["finish_reason"].(string)
			if message, ok := choice["message"].(map[string]any); ok {
				content, _ = message["content"].(string)
				toolCalls = parseOpenAIToolCalls(message["tool_calls"])
			}
		}
	}
	return GenerateResponse{
		ID:           firstString(decoded, "id"),
		Model:        firstString(decoded, "model"),
		Content:      content,
		Usage:        parseOpenAIUsage(decoded["usage"]),
		FinishReason: finishReason,
		ToolCalls:    toolCalls,
		Metadata:     decoded,
	}, nil
}

// AnthropicConfig configures Anthropic Messages API access.
type AnthropicConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
	Version    string
}

type AnthropicModel struct {
	config AnthropicConfig
}

func NewAnthropicModel(config AnthropicConfig) *AnthropicModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.anthropic.com"
	}
	if config.Version == "" {
		config.Version = "2023-06-01"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &AnthropicModel{config: config}
}

func (m *AnthropicModel) Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error) {
	if m == nil {
		return GenerateResponse{}, errors.New("agnt5: nil model")
	}
	model := request.Model
	if model == "" {
		model = m.config.Model
	}
	if model == "" {
		return GenerateResponse{}, errors.New("agnt5: model is required")
	}
	payload := map[string]any{
		"model":      model,
		"messages":   anthropicMessages(request.Messages),
		"max_tokens": 1024,
	}
	if system := firstSystemMessage(request.Messages); system != "" {
		payload["system"] = system
	}
	if len(request.Tools) > 0 {
		payload["tools"] = anthropicTools(request.Tools)
	}
	if request.MaxTokens != nil {
		payload["max_tokens"] = *request.MaxTokens
	}
	if request.Temperature != nil {
		payload["temperature"] = *request.Temperature
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return GenerateResponse{}, err
	}
	endpoint := strings.TrimRight(m.config.BaseURL, "/") + "/v1/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return GenerateResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-version", m.config.Version)
	if m.config.APIKey != "" {
		req.Header.Set("x-api-key", m.config.APIKey)
	}
	resp, err := m.config.HTTPClient.Do(req)
	if err != nil {
		return GenerateResponse{}, err
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return GenerateResponse{}, err
	}
	if resp.StatusCode >= 400 {
		return GenerateResponse{}, errors.New("agnt5: anthropic provider returned HTTP " + intString(resp.StatusCode))
	}
	content, toolCalls := parseAnthropicContent(decoded["content"])
	return GenerateResponse{
		ID:           firstString(decoded, "id"),
		Model:        firstString(decoded, "model"),
		Content:      content,
		Usage:        parseAnthropicUsage(decoded["usage"]),
		FinishReason: firstString(decoded, "stop_reason"),
		ToolCalls:    toolCalls,
		Metadata:     decoded,
	}, nil
}

type AzureOpenAIConfig struct {
	Endpoint   string
	APIKey     string
	Deployment string
	APIVersion string
	HTTPClient *http.Client
}

// NewAzureOpenAIModel constructs an Azure OpenAI chat-completions adapter.
func NewAzureOpenAIModel(config AzureOpenAIConfig) *OpenAIModel {
	apiVersion := config.APIVersion
	if apiVersion == "" {
		apiVersion = "2024-02-15-preview"
	}
	base := strings.TrimRight(config.Endpoint, "/")
	if config.Deployment != "" {
		base += "/openai/deployments/" + url.PathEscape(config.Deployment)
	}
	return NewOpenAIModel(OpenAIConfig{
		BaseURL:      base,
		APIKey:       config.APIKey,
		APIKeyHeader: "api-key",
		Model:        config.Deployment,
		HTTPClient:   config.HTTPClient,
		Path:         "/chat/completions?api-version=" + url.QueryEscape(apiVersion),
	})
}

func NewOpenRouterModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://openrouter.ai/api"
	}
	return NewOpenAIModel(config)
}

func NewGroqModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.groq.com/openai"
	}
	return NewOpenAIModel(config)
}

func NewDeepSeekModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.deepseek.com"
	}
	return NewOpenAIModel(config)
}

func NewMistralModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.mistral.ai"
	}
	return NewOpenAIModel(config)
}

func NewTogetherModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.together.xyz"
	}
	return NewOpenAIModel(config)
}

func NewXAIModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.x.ai"
	}
	return NewOpenAIModel(config)
}

func NewOllamaModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "http://localhost:11434"
	}
	return NewOpenAIModel(config)
}

// Generate runs a model and emits LLM lifecycle events.
func (c *Context) Generate(model LanguageModel, request GenerateRequest) (GenerateResponse, error) {
	if model == nil {
		return GenerateResponse{}, errors.New("agnt5: nil language model")
	}
	_ = c.Emit(Event{Type: "lm.started", Data: map[string]any{"model": request.Model}})
	resp, err := model.Generate(c, request)
	if err != nil {
		_ = c.Emit(Event{Type: "lm.failed", Data: map[string]any{"model": request.Model, "error": err.Error()}})
		return GenerateResponse{}, err
	}
	_ = c.Emit(Event{Type: "lm.completed", Data: map[string]any{"model": resp.Model, "usage": resp.Usage}})
	return resp, nil
}

func openAIMessages(messages []Message) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		item := map[string]any{
			"role":    string(message.Role),
			"content": message.Content,
		}
		if message.Name != "" {
			item["name"] = message.Name
		}
		if message.ToolCallID != "" {
			item["tool_call_id"] = message.ToolCallID
		}
		if len(message.ToolCalls) > 0 {
			item["tool_calls"] = openAIResponseToolCalls(message.ToolCalls)
		}
		out = append(out, item)
	}
	return out
}

func openAITools(tools []Tool) []map[string]any {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		parameters := tool.Schema
		if len(parameters) == 0 {
			parameters = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        tool.Name,
				"description": tool.Description,
				"parameters":  parameters,
			},
		})
	}
	return out
}

func openAIResponseToolCalls(toolCalls []ToolCall) []map[string]any {
	out := make([]map[string]any, 0, len(toolCalls))
	for _, call := range toolCalls {
		args, _ := json.Marshal(call.Arguments)
		out = append(out, map[string]any{
			"id":   call.ID,
			"type": "function",
			"function": map[string]any{
				"name":      call.Name,
				"arguments": string(args),
			},
		})
	}
	return out
}

func parseOpenAIToolCalls(value any) []ToolCall {
	rawCalls, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]ToolCall, 0, len(rawCalls))
	for _, raw := range rawCalls {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fn, _ := item["function"].(map[string]any)
		name, _ := fn["name"].(string)
		arguments := parseToolArguments(fn["arguments"])
		out = append(out, ToolCall{
			ID:        firstString(item, "id"),
			Name:      name,
			Arguments: arguments,
			Raw:       item,
		})
	}
	return out
}

func parseToolArguments(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case string:
		if typed == "" {
			return map[string]any{}
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(typed), &decoded); err == nil {
			return decoded
		}
		return map[string]any{"value": typed}
	default:
		return map[string]any{}
	}
}

func parseOpenAIUsage(value any) TokenUsage {
	usage, _ := value.(map[string]any)
	return TokenUsage{
		InputTokens:  intFromAny(usage["prompt_tokens"]),
		OutputTokens: intFromAny(usage["completion_tokens"]),
		TotalTokens:  intFromAny(usage["total_tokens"]),
	}
}

func firstSystemMessage(messages []Message) string {
	for _, message := range messages {
		if message.Role == MessageRoleSystem {
			return message.Content
		}
	}
	return ""
}

func anthropicMessages(messages []Message) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		if message.Role == MessageRoleSystem {
			continue
		}
		if message.Role == MessageRoleTool {
			out = append(out, map[string]any{
				"role": "user",
				"content": []map[string]any{{
					"type":        "tool_result",
					"tool_use_id": message.ToolCallID,
					"content":     message.Content,
				}},
			})
			continue
		}
		item := map[string]any{
			"role":    string(message.Role),
			"content": message.Content,
		}
		if len(message.ToolCalls) > 0 {
			blocks := make([]map[string]any, 0, len(message.ToolCalls)+1)
			if message.Content != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": message.Content})
			}
			for _, call := range message.ToolCalls {
				blocks = append(blocks, map[string]any{
					"type":  "tool_use",
					"id":    call.ID,
					"name":  call.Name,
					"input": call.Arguments,
				})
			}
			item["content"] = blocks
		}
		out = append(out, item)
	}
	return out
}

func anthropicTools(tools []Tool) []map[string]any {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		schema := tool.Schema
		if len(schema) == 0 {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{
			"name":         tool.Name,
			"description":  tool.Description,
			"input_schema": schema,
		})
	}
	return out
}

func parseAnthropicContent(value any) (string, []ToolCall) {
	blocks, ok := value.([]any)
	if !ok {
		return "", nil
	}
	var text strings.Builder
	var toolCalls []ToolCall
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		switch block["type"] {
		case "text":
			if s, ok := block["text"].(string); ok {
				text.WriteString(s)
			}
		case "tool_use":
			name, _ := block["name"].(string)
			toolCalls = append(toolCalls, ToolCall{
				ID:        firstString(block, "id"),
				Name:      name,
				Arguments: parseToolArguments(block["input"]),
				Raw:       block,
			})
		}
	}
	return text.String(), toolCalls
}

func parseAnthropicUsage(value any) TokenUsage {
	usage, _ := value.(map[string]any)
	input := intFromAny(usage["input_tokens"])
	output := intFromAny(usage["output_tokens"])
	return TokenUsage{InputTokens: input, OutputTokens: output, TotalTokens: input + output}
}

func cloneToolCalls(in []ToolCall) []ToolCall {
	if len(in) == 0 {
		return nil
	}
	out := make([]ToolCall, len(in))
	for i, call := range in {
		call.Arguments = cloneAnyMap(call.Arguments)
		call.Raw = cloneAnyMap(call.Raw)
		out[i] = call
	}
	return out
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	case string:
		var parsed int
		_, _ = fmt.Sscan(typed, &parsed)
		return parsed
	default:
		return 0
	}
}
