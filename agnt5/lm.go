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
	ID          string         `json:"id,omitempty"`
	Name        string         `json:"name"`
	Arguments   map[string]any `json:"arguments,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
	CallID      string         `json:"call_id,omitempty"`
	SpanID      string         `json:"span_id,omitempty"`
	TimestampNS int64          `json:"timestamp_ns,omitempty"`
	StartedAt   int64          `json:"started_at,omitempty"`
	EndedAt     int64          `json:"ended_at,omitempty"`
	Status      string         `json:"status,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// GenerateRequest is a provider-neutral model request.
type GenerateRequest struct {
	Model       string       `json:"model,omitempty"`
	Messages    []Message    `json:"messages"`
	Tools       []Tool       `json:"tools,omitempty"`
	Temperature *float64     `json:"temperature,omitempty"`
	MaxTokens   *int         `json:"max_tokens,omitempty"`
	Cache       *PromptCache `json:"cache,omitempty"`
	// Deprecated compatibility aliases. Prefer Cache.
	CacheControl        bool           `json:"cache_control,omitempty"`
	CacheTTL            string         `json:"cache_ttl,omitempty"`
	GoogleCachedContent string         `json:"google_cached_content,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
	// RecoveryPolicy controls how an interrupted model call is settled.
	// The default is unknown_outcome.
	RecoveryPolicy RecoveryPolicy `json:"recovery_policy,omitempty"`
}

// PromptCache is a provider-neutral prompt-cache policy.
type PromptCache struct {
	Enabled   bool   `json:"enabled,omitempty"`
	TTL       string `json:"ttl,omitempty"`
	Key       string `json:"key,omitempty"`
	Retention string `json:"retention,omitempty"`
	Resource  string `json:"resource,omitempty"`
}

func EnablePromptCache() *PromptCache {
	return &PromptCache{Enabled: true}
}

func PromptCacheWithTTL(ttl string) *PromptCache {
	return &PromptCache{Enabled: true, TTL: ttl}
}

func PromptCacheResource(name string) *PromptCache {
	return &PromptCache{Enabled: true, Resource: name}
}

// TokenUsage captures provider token accounting.
type TokenUsage struct {
	InputTokens         int `json:"input_tokens,omitempty"`
	OutputTokens        int `json:"output_tokens,omitempty"`
	TotalTokens         int `json:"total_tokens,omitempty"`
	CachedTokens        int `json:"cached_tokens,omitempty"`
	CacheCreationTokens int `json:"cache_creation_tokens,omitempty"`
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

// ModelStreamChunkType identifies one provider-neutral model stream event.
type ModelStreamChunkType string

const (
	ModelStreamMessageStart  ModelStreamChunkType = "message_start"
	ModelStreamMessageDelta  ModelStreamChunkType = "message_delta"
	ModelStreamMessageStop   ModelStreamChunkType = "message_stop"
	ModelStreamThinkingStart ModelStreamChunkType = "thinking_start"
	ModelStreamThinkingDelta ModelStreamChunkType = "thinking_delta"
	ModelStreamThinkingStop  ModelStreamChunkType = "thinking_stop"
	ModelStreamToolCallStart ModelStreamChunkType = "tool_call_start"
	ModelStreamToolCallDelta ModelStreamChunkType = "tool_call_delta"
	ModelStreamToolCallStop  ModelStreamChunkType = "tool_call_stop"
)

// ModelStreamChunk is emitted while a StreamingLanguageModel is generating.
// Tool-call arguments use their provider-neutral JSON representation so agents
// can expose partial arguments without executing an incomplete call.
type ModelStreamChunk struct {
	Type           ModelStreamChunkType
	Content        string
	Index          int
	ToolCallID     string
	ToolName       string
	ArgumentsDelta string
	Arguments      map[string]any
}

// StreamingLanguageModel is the optional streaming extension implemented by
// models that can return incremental text and tool-call chunks. LanguageModel
// remains the required compatibility surface, so existing models are unchanged.
type StreamingLanguageModel interface {
	LanguageModel
	Stream(
		ctx context.Context,
		request GenerateRequest,
		emit func(ModelStreamChunk) error,
	) (GenerateResponse, error)
}

func (r GenerateRequest) promptCache() *PromptCache {
	if r.Cache != nil {
		cache := *r.Cache
		if cache.TTL != "" || cache.Key != "" || cache.Retention != "" || cache.Resource != "" {
			cache.Enabled = true
		}
		return &cache
	}
	if r.CacheControl || r.CacheTTL != "" || r.GoogleCachedContent != "" {
		return &PromptCache{
			Enabled:  r.CacheControl || r.CacheTTL != "" || r.GoogleCachedContent != "",
			TTL:      r.CacheTTL,
			Resource: r.GoogleCachedContent,
		}
	}
	return nil
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
	if cache := request.promptCache(); cache != nil && strings.TrimSpace(cache.Resource) != "" {
		return GenerateResponse{}, errors.New("agnt5: explicit context caches are only supported for Google Gemini")
	}
	// OpenAI-compatible chat completions expose prompt cache hits in usage,
	// but provider-specific key/retention hints are only sent by the
	// Responses API adapters in the Rust-backed SDKs.
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
	cache := request.promptCache()
	if cache != nil && strings.TrimSpace(cache.Resource) != "" {
		return GenerateResponse{}, errors.New("agnt5: explicit context caches are only supported for Google Gemini")
	}
	if cache != nil && (cache.Enabled || strings.TrimSpace(cache.TTL) != "") {
		cacheControl := map[string]any{"type": "ephemeral"}
		if ttl := strings.TrimSpace(cache.TTL); ttl != "" {
			cacheControl["ttl"] = ttl
		}
		payload["cache_control"] = cacheControl
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

// GoogleConfig configures Google Gemini API access.
type GoogleConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	Version    string
	HTTPClient *http.Client
}

// GoogleModel is a minimal Gemini LanguageModel.
type GoogleModel struct {
	config GoogleConfig
}

func NewGoogleModel(config GoogleConfig) *GoogleModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://generativelanguage.googleapis.com"
	}
	if config.Version == "" {
		config.Version = "v1beta"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &GoogleModel{config: config}
}

func NewGeminiModel(config GoogleConfig) *GoogleModel {
	return NewGoogleModel(config)
}

func (m *GoogleModel) Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error) {
	if m == nil {
		return GenerateResponse{}, errors.New("agnt5: nil model")
	}
	model := request.Model
	if model == "" {
		model = m.config.Model
	}
	model = normalizeGoogleModel(model)
	if model == "" {
		return GenerateResponse{}, errors.New("agnt5: model is required")
	}
	payload := map[string]any{
		"contents": googleContents(request.Messages),
	}
	if tools := googleTools(request.Tools); len(tools) > 0 {
		payload["tools"] = tools
	}
	if system := firstSystemMessage(request.Messages); system != "" {
		payload["systemInstruction"] = googleContent("", system)
	}
	if cache := request.promptCache(); cache != nil && strings.TrimSpace(cache.Resource) != "" {
		payload["cachedContent"] = strings.TrimSpace(cache.Resource)
	}
	generationConfig := map[string]any{}
	if request.Temperature != nil {
		generationConfig["temperature"] = *request.Temperature
	}
	if request.MaxTokens != nil {
		generationConfig["maxOutputTokens"] = *request.MaxTokens
	}
	if len(generationConfig) > 0 {
		payload["generationConfig"] = generationConfig
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return GenerateResponse{}, err
	}
	endpoint := strings.TrimRight(m.config.BaseURL, "/") + "/" + strings.Trim(m.config.Version, "/") + "/models/" + url.PathEscape(model) + ":generateContent?key=" + url.QueryEscape(m.config.APIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return GenerateResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
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
		return GenerateResponse{}, errors.New("agnt5: google provider returned HTTP " + intString(resp.StatusCode))
	}
	content, finishReason, toolCalls := parseGoogleContent(decoded["candidates"])
	return GenerateResponse{
		ID:           firstString(decoded, "id"),
		Model:        model,
		Content:      content,
		Usage:        parseGoogleUsage(decoded["usageMetadata"]),
		FinishReason: finishReason,
		ToolCalls:    toolCalls,
		Metadata:     decoded,
	}, nil
}

func (m *GoogleModel) CreateCachedContent(ctx context.Context, model string, system string, contents []string, ttlSeconds int) (string, error) {
	if m == nil {
		return "", errors.New("agnt5: nil model")
	}
	model = normalizeGoogleModel(model)
	if model == "" {
		model = normalizeGoogleModel(m.config.Model)
	}
	if model == "" {
		return "", errors.New("agnt5: model is required")
	}
	cacheContents := googleTextContents(contents)
	if len(cacheContents) == 0 {
		return "", errors.New("agnt5: cache contents are required")
	}
	payload := map[string]any{
		"model":    "models/" + model,
		"contents": cacheContents,
	}
	if strings.TrimSpace(system) != "" {
		payload["systemInstruction"] = googleContent("", system)
	}
	if ttlSeconds > 0 {
		payload["ttl"] = intString(ttlSeconds) + "s"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	endpoint := strings.TrimRight(m.config.BaseURL, "/") + "/" + strings.Trim(m.config.Version, "/") + "/cachedContents?key=" + url.QueryEscape(m.config.APIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.config.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", errors.New("agnt5: google cache create returned HTTP " + intString(resp.StatusCode))
	}
	name, _ := decoded["name"].(string)
	if name == "" {
		return "", errors.New("agnt5: google cache create response missing name")
	}
	return name, nil
}

func (m *GoogleModel) DeleteCachedContent(ctx context.Context, name string) error {
	if m == nil {
		return errors.New("agnt5: nil model")
	}
	name = strings.TrimLeft(strings.TrimSpace(name), "/")
	if name == "" {
		return errors.New("agnt5: cache name is required")
	}
	endpoint := strings.TrimRight(m.config.BaseURL, "/") + "/" + strings.Trim(m.config.Version, "/") + "/" + name + "?key=" + url.QueryEscape(m.config.APIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := m.config.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return errors.New("agnt5: google cache delete returned HTTP " + intString(resp.StatusCode))
	}
	return nil
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

// NewMoonshotModel constructs an OpenAI-compatible Moonshot AI (Kimi) adapter.
func NewMoonshotModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "https://api.moonshot.ai"
	}
	return NewOpenAIModel(config)
}

func NewOllamaModel(config OpenAIConfig) *OpenAIModel {
	if config.BaseURL == "" {
		config.BaseURL = "http://localhost:11434"
	}
	return NewOpenAIModel(config)
}

// Generate runs a model. Under a negotiated durable_activation_v1 runtime the
// MODEL activation record is the lm boundary (the runtime journals
// lm.started/completed/failed from the activation RPCs) and only stream deltas
// are emitted, correlated to the activation ID. Otherwise the SDK emits the
// lm lifecycle events itself.
func (c *Context) Generate(model LanguageModel, request GenerateRequest) (GenerateResponse, error) {
	if model == nil {
		return GenerateResponse{}, errors.New("agnt5: nil language model")
	}
	modelName, provider := languageModelIdentity(model, request)
	durable := c.Metadata(durableActivationV1Capability) == "true"
	lmCorrelationID := newCorrelationID("lm")
	parentCorrelationID := c.parentCorrelationID()
	startedAt := time.Now()
	if !durable {
		_ = c.Emit(lifecycleEvent(
			"lm.started",
			modelName,
			"lm",
			lmCorrelationID,
			parentCorrelationID,
			map[string]any{
				"model":      modelName,
				"provider":   provider,
				"input_data": map[string]any{"messages": request.Messages, "tools_count": len(request.Tools)},
			},
		))
	}
	var (
		resp GenerateResponse
		err  error
	)
	if streamingModel, ok := model.(StreamingLanguageModel); ok && c.IsStreaming() {
		if durable {
			resp, err = c.streamDurableModel(streamingModel, request, modelName, provider, func(chunk ModelStreamChunk, activationID string) error {
				return c.Emit(modelStreamEvent(chunk, activationID, parentCorrelationID, modelName))
			})
		} else {
			resp, err = streamingModel.Stream(c, request, func(chunk ModelStreamChunk) error {
				return c.Emit(modelStreamEvent(chunk, lmCorrelationID, parentCorrelationID, modelName))
			})
		}
	} else if durable {
		resp, err = c.generateDurableModel(model, request, modelName, provider)
	} else {
		resp, err = model.Generate(c, request)
	}
	if err != nil {
		if !durable {
			_ = c.Emit(lifecycleEvent(
				"lm.failed",
				modelName,
				"lm",
				lmCorrelationID,
				parentCorrelationID,
				map[string]any{
					"model":         modelName,
					"provider":      provider,
					"error":         err.Error(),
					"duration_ms":   time.Since(startedAt).Milliseconds(),
					"error_message": err.Error(),
				},
			))
		}
		return GenerateResponse{}, err
	}
	if !durable {
		_ = c.Emit(lifecycleEvent(
			"lm.completed",
			modelName,
			"lm",
			lmCorrelationID,
			parentCorrelationID,
			map[string]any{
				"model":          modelName,
				"provider":       provider,
				"response_model": resp.Model,
				"usage":          resp.Usage,
				"input_tokens":   resp.Usage.InputTokens,
				"output_tokens":  resp.Usage.OutputTokens,
				"total_tokens":   resp.Usage.TotalTokens,
				"cached_tokens":  resp.Usage.CachedTokens,
				"duration_ms":    time.Since(startedAt).Milliseconds(),
				"output_data":    map[string]any{"output": resp.Content, "tool_calls": resp.ToolCalls},
			},
		))
	}
	return resp, nil
}

func modelStreamEvent(
	chunk ModelStreamChunk,
	correlationID string,
	parentCorrelationID string,
	modelName string,
) Event {
	data := map[string]any{"index": chunk.Index}
	eventType := ""
	switch chunk.Type {
	case ModelStreamMessageStart:
		eventType = "lm.message.start"
	case ModelStreamMessageDelta:
		eventType = "lm.message.delta"
		data["content"] = chunk.Content
	case ModelStreamMessageStop:
		eventType = "lm.message.stop"
	case ModelStreamThinkingStart:
		eventType = "lm.thinking.start"
	case ModelStreamThinkingDelta:
		eventType = "lm.thinking.delta"
		data["content"] = chunk.Content
	case ModelStreamThinkingStop:
		eventType = "lm.thinking.stop"
	case ModelStreamToolCallStart:
		eventType = "lm.tool_call.start"
		data["id"] = chunk.ToolCallID
		data["name"] = chunk.ToolName
	case ModelStreamToolCallDelta:
		eventType = "lm.tool_call.delta"
		data["input_delta"] = chunk.ArgumentsDelta
	case ModelStreamToolCallStop:
		eventType = "lm.tool_call.stop"
		data["id"] = chunk.ToolCallID
		data["name"] = chunk.ToolName
		data["input"] = cloneAnyMap(chunk.Arguments)
	}
	event := lifecycleEvent(eventType, modelName, "lm", correlationID, parentCorrelationID, data)
	event.ContentIndex = chunk.Index
	return event
}

func languageModelIdentity(model LanguageModel, request GenerateRequest) (string, string) {
	name := strings.TrimSpace(request.Model)
	provider := ""
	switch typed := model.(type) {
	case StaticModel:
		if name == "" {
			name = strings.TrimSpace(typed.Model)
		}
		provider = "static"
	case *StaticModel:
		if typed != nil && name == "" {
			name = strings.TrimSpace(typed.Model)
		}
		provider = "static"
	case *ScriptedModel:
		provider = "scripted"
	case *OpenAIModel:
		if typed != nil {
			if name == "" {
				name = strings.TrimSpace(typed.config.Model)
			}
			provider = openAIProvider(typed.config.BaseURL)
		}
	case *AnthropicModel:
		if typed != nil {
			if name == "" {
				name = strings.TrimSpace(typed.config.Model)
			}
			provider = "anthropic"
		}
	case *GoogleModel:
		if typed != nil {
			if name == "" {
				name = strings.TrimSpace(typed.config.Model)
			}
			provider = "google"
		}
	}
	if name == "" {
		name = "language_model"
	}
	if provider != "" && provider != "static" && provider != "scripted" && !strings.Contains(name, "/") {
		name = provider + "/" + name
	}
	return name, provider
}

func openAIProvider(baseURL string) string {
	normalized := strings.ToLower(baseURL)
	switch {
	case strings.Contains(normalized, "azure.com"):
		return "azure-openai"
	case strings.Contains(normalized, "openrouter.ai"):
		return "openrouter"
	case strings.Contains(normalized, "groq.com"):
		return "groq"
	case strings.Contains(normalized, "deepseek.com"):
		return "deepseek"
	case strings.Contains(normalized, "mistral.ai"):
		return "mistral"
	case strings.Contains(normalized, "together.xyz"):
		return "together"
	case strings.Contains(normalized, "api.x.ai"):
		return "xai"
	case strings.Contains(normalized, "moonshot.ai"):
		return "moonshot"
	case strings.Contains(normalized, "localhost:11434"):
		return "ollama"
	default:
		return "openai"
	}
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
	cachedTokens := 0
	if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
		cachedTokens = intFromAny(details["cached_tokens"])
	}
	return TokenUsage{
		InputTokens:  intFromAny(usage["prompt_tokens"]),
		OutputTokens: intFromAny(usage["completion_tokens"]),
		TotalTokens:  intFromAny(usage["total_tokens"]),
		CachedTokens: cachedTokens,
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
	cacheCreation := intFromAny(usage["cache_creation_input_tokens"])
	cacheRead := intFromAny(usage["cache_read_input_tokens"])
	inputTotal := input + cacheCreation + cacheRead
	return TokenUsage{
		InputTokens:         inputTotal,
		OutputTokens:        output,
		TotalTokens:         inputTotal + output,
		CachedTokens:        cacheRead,
		CacheCreationTokens: cacheCreation,
	}
}

func normalizeGoogleModel(model string) string {
	model = strings.TrimSpace(model)
	if provider, rest, ok := strings.Cut(model, "/"); ok {
		if provider == "google" || provider == "gemini" {
			return strings.TrimPrefix(rest, "models/")
		}
		return model
	}
	return strings.TrimPrefix(model, "models/")
}

func googleContents(messages []Message) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	toolCallNames := make(map[string]string)
	for _, message := range messages {
		if message.Role == MessageRoleSystem {
			continue
		}
		if message.Role == MessageRoleTool {
			name := strings.TrimSpace(message.Name)
			if name == "" {
				name = toolCallNames[message.ToolCallID]
			}
			if name == "" {
				name = message.ToolCallID
			}
			out = append(out, map[string]any{
				"role": "user",
				"parts": []map[string]any{{
					"functionResponse": map[string]any{
						"name":     name,
						"response": googleFunctionResponse(message.Content),
						"id":       message.ToolCallID,
					},
				}},
			})
			continue
		}
		role := "user"
		if message.Role == MessageRoleAssistant {
			role = "model"
		}
		if len(message.ToolCalls) == 0 {
			out = append(out, googleContent(role, message.Content))
			continue
		}

		parts := make([]map[string]any, 0, len(message.ToolCalls)+1)
		if message.Content != "" {
			parts = append(parts, map[string]any{"text": message.Content})
		}
		for _, call := range message.ToolCalls {
			if call.ID != "" {
				toolCallNames[call.ID] = call.Name
			}
			part := map[string]any{
				"functionCall": map[string]any{
					"name": call.Name,
					"args": cloneAnyMap(call.Arguments),
					"id":   call.ID,
				},
			}
			if signature, ok := call.Raw["thoughtSignature"].(string); ok && signature != "" {
				part["thoughtSignature"] = signature
			}
			parts = append(parts, part)
		}
		out = append(out, map[string]any{"role": role, "parts": parts})
	}
	return out
}

func googleTools(tools []Tool) []map[string]any {
	if len(tools) == 0 {
		return nil
	}
	declarations := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		parameters := cloneAnyMap(tool.Schema)
		if len(parameters) == 0 {
			parameters = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		declaration := map[string]any{
			"name":       tool.Name,
			"parameters": parameters,
		}
		if tool.Description != "" {
			declaration["description"] = tool.Description
		}
		declarations = append(declarations, declaration)
	}
	return []map[string]any{{"functionDeclarations": declarations}}
}

func googleFunctionResponse(content string) map[string]any {
	var value any
	if err := json.Unmarshal([]byte(content), &value); err != nil {
		return map[string]any{"result": content}
	}
	if object, ok := value.(map[string]any); ok {
		return object
	}
	return map[string]any{"result": value}
}

func googleTextContents(contents []string) []map[string]any {
	out := make([]map[string]any, 0, len(contents))
	for _, content := range contents {
		if strings.TrimSpace(content) == "" {
			continue
		}
		out = append(out, googleContent("user", content))
	}
	return out
}

func googleContent(role string, text string) map[string]any {
	content := map[string]any{
		"parts": []map[string]any{{"text": text}},
	}
	if role != "" {
		content["role"] = role
	}
	return content
}

func parseGoogleContent(value any) (string, string, []ToolCall) {
	candidates, ok := value.([]any)
	if !ok || len(candidates) == 0 {
		return "", "", nil
	}
	candidate, _ := candidates[0].(map[string]any)
	finishReason, _ := candidate["finishReason"].(string)
	content, _ := candidate["content"].(map[string]any)
	parts, _ := content["parts"].([]any)
	var text strings.Builder
	var toolCalls []ToolCall
	for _, raw := range parts {
		part, _ := raw.(map[string]any)
		if s, ok := part["text"].(string); ok {
			text.WriteString(s)
		}
		functionCall, ok := part["functionCall"].(map[string]any)
		if !ok {
			continue
		}
		name, _ := functionCall["name"].(string)
		id := firstString(functionCall, "id")
		if id == "" {
			id = "call_" + intString(len(toolCalls))
		}
		toolCalls = append(toolCalls, ToolCall{
			ID:        id,
			Name:      name,
			Arguments: parseToolArguments(functionCall["args"]),
			Raw:       cloneAnyMap(part),
		})
	}
	return text.String(), finishReason, toolCalls
}

func parseGoogleUsage(value any) TokenUsage {
	usage, _ := value.(map[string]any)
	return TokenUsage{
		InputTokens:  intFromAny(usage["promptTokenCount"]),
		OutputTokens: intFromAny(usage["candidatesTokenCount"]),
		TotalTokens:  intFromAny(usage["totalTokenCount"]),
		CachedTokens: intFromAny(usage["cachedContentTokenCount"]),
	}
}

func cloneToolCalls(in []ToolCall) []ToolCall {
	if len(in) == 0 {
		return nil
	}
	out := make([]ToolCall, len(in))
	for i, call := range in {
		call.Arguments = cloneAnyMap(call.Arguments)
		call.Raw = cloneAnyMap(call.Raw)
		call.Metadata = cloneAnyMap(call.Metadata)
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
