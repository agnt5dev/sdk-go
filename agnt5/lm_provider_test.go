package agnt5

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestOpenAIModelSendsToolsAndParsesToolCalls(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("method: %s", req.Method)
		}
		if req.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path: %s", req.URL.Path)
		}
		if req.Header.Get("Authorization") != "Bearer sk-test" {
			t.Fatalf("authorization: %q", req.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(req.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		body := `{
			"id":"chatcmpl-1",
			"model":"gpt-test",
			"choices":[{
				"finish_reason":"tool_calls",
				"message":{
					"role":"assistant",
					"content":"",
					"tool_calls":[{
						"id":"call-1",
						"type":"function",
						"function":{"name":"lookup","arguments":"{\"key\":\"user_123\"}"}
					}]
				}
			}],
			"usage":{
				"prompt_tokens":3,
				"completion_tokens":4,
				"total_tokens":7,
				"prompt_tokens_details":{"cached_tokens":2}
			}
		}`
		return jsonResponse(req, http.StatusOK, body), nil
	})}

	tool, err := NewTool("lookup", func(context.Context, map[string]any) (any, error) { return nil, nil },
		WithToolDescription("Lookup a user"),
		WithToolSchema(map[string]any{
			"type":       "object",
			"properties": map[string]any{"key": map[string]any{"type": "string"}},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	model := NewOpenAIModel(OpenAIConfig{
		BaseURL:    "http://provider.test",
		APIKey:     "sk-test",
		Model:      "gpt-test",
		HTTPClient: client,
	})
	resp, err := model.Generate(context.Background(), GenerateRequest{
		Messages: []Message{{Role: MessageRoleUser, Content: "lookup user_123"}},
		Tools:    []Tool{tool},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != "chatcmpl-1" || resp.FinishReason != "tool_calls" || resp.Usage.TotalTokens != 7 || resp.Usage.CachedTokens != 2 {
		t.Fatalf("response = %#v", resp)
	}
	if len(resp.ToolCalls) != 1 ||
		resp.ToolCalls[0].Name != "lookup" ||
		resp.ToolCalls[0].Arguments["key"] != "user_123" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
	if captured["model"] != "gpt-test" {
		t.Fatalf("captured body = %#v", captured)
	}
	if tools, ok := captured["tools"].([]any); !ok || len(tools) != 1 {
		t.Fatalf("captured tools = %#v", captured["tools"])
	}
}

func TestAnthropicModelSendsToolsAndParsesToolUse(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("method: %s", req.Method)
		}
		if req.URL.Path != "/v1/messages" {
			t.Fatalf("path: %s", req.URL.Path)
		}
		if req.Header.Get("x-api-key") != "anthropic-key" {
			t.Fatalf("x-api-key: %q", req.Header.Get("x-api-key"))
		}
		if req.Header.Get("anthropic-version") == "" {
			t.Fatalf("missing anthropic-version")
		}
		if err := json.NewDecoder(req.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		body := `{
			"id":"msg_1",
			"model":"claude-test",
			"stop_reason":"tool_use",
			"content":[
				{"type":"text","text":"Need lookup."},
				{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"key":"user_123"}}
			],
			"usage":{"input_tokens":5,"output_tokens":6}
		}`
		return jsonResponse(req, http.StatusOK, body), nil
	})}

	tool, err := NewTool("lookup", func(context.Context, map[string]any) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	model := NewAnthropicModel(AnthropicConfig{
		BaseURL:    "http://provider.test",
		APIKey:     "anthropic-key",
		Model:      "claude-test",
		HTTPClient: client,
	})
	resp, err := model.Generate(context.Background(), GenerateRequest{
		Messages: []Message{
			{Role: MessageRoleSystem, Content: "Use tools carefully."},
			{Role: MessageRoleUser, Content: "lookup user_123"},
		},
		Tools: []Tool{tool},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content != "Need lookup." || resp.FinishReason != "tool_use" || resp.Usage.TotalTokens != 11 {
		t.Fatalf("response = %#v", resp)
	}
	if len(resp.ToolCalls) != 1 ||
		resp.ToolCalls[0].ID != "toolu_1" ||
		resp.ToolCalls[0].Arguments["key"] != "user_123" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
	if captured["system"] != "Use tools carefully." {
		t.Fatalf("captured body = %#v", captured)
	}
	if tools, ok := captured["tools"].([]any); !ok || len(tools) != 1 {
		t.Fatalf("captured tools = %#v", captured["tools"])
	}
}

func TestAnthropicModelSendsCacheControlAndParsesCacheUsage(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(req.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		body := `{
			"id":"msg_cache",
			"model":"claude-test",
			"stop_reason":"end_turn",
			"content":[{"type":"text","text":"cached"}],
			"usage":{
				"input_tokens":5,
				"output_tokens":6,
				"cache_creation_input_tokens":7,
				"cache_read_input_tokens":8
			}
		}`
		return jsonResponse(req, http.StatusOK, body), nil
	})}

	model := NewAnthropicModel(AnthropicConfig{
		BaseURL:    "http://provider.test",
		APIKey:     "anthropic-key",
		Model:      "claude-test",
		HTTPClient: client,
	})
	resp, err := model.Generate(context.Background(), GenerateRequest{
		Messages: []Message{
			{Role: MessageRoleSystem, Content: "Stable instructions."},
			{Role: MessageRoleUser, Content: "question"},
		},
		Cache: PromptCacheWithTTL("1h"),
	})
	if err != nil {
		t.Fatal(err)
	}

	cacheControl, ok := captured["cache_control"].(map[string]any)
	if !ok {
		t.Fatalf("missing cache_control in %#v", captured)
	}
	if cacheControl["type"] != "ephemeral" || cacheControl["ttl"] != "1h" {
		t.Fatalf("cache_control = %#v", cacheControl)
	}
	if resp.Usage.InputTokens != 20 || resp.Usage.CachedTokens != 8 || resp.Usage.CacheCreationTokens != 7 {
		t.Fatalf("usage = %#v", resp.Usage)
	}
}

func TestGoogleModelUsesCachedContentAndParsesUsage(t *testing.T) {
	var captured map[string]any
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("method: %s", req.Method)
		}
		if req.URL.Path != "/v1beta/models/gemini-2.5-flash:generateContent" {
			t.Fatalf("path: %s", req.URL.Path)
		}
		if req.URL.Query().Get("key") != "google-key" {
			t.Fatalf("key: %s", req.URL.RawQuery)
		}
		if err := json.NewDecoder(req.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		body := `{
			"candidates":[{
				"finishReason":"STOP",
				"content":{"parts":[{"text":"answer"}]}
			}],
			"usageMetadata":{
				"promptTokenCount":10,
				"candidatesTokenCount":5,
				"totalTokenCount":15,
				"cachedContentTokenCount":8
			}
		}`
		return jsonResponse(req, http.StatusOK, body), nil
	})}

	model := NewGoogleModel(GoogleConfig{
		BaseURL:    "http://provider.test",
		APIKey:     "google-key",
		Model:      "google/gemini-2.5-flash",
		HTTPClient: client,
	})
	resp, err := model.Generate(context.Background(), GenerateRequest{
		Messages: []Message{
			{Role: MessageRoleSystem, Content: "You are helpful."},
			{Role: MessageRoleUser, Content: "question"},
		},
		Cache: PromptCacheResource("cachedContents/cache_123"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if captured["cachedContent"] != "cachedContents/cache_123" {
		t.Fatalf("captured body = %#v", captured)
	}
	if resp.Content != "answer" || resp.Usage.CachedTokens != 8 {
		t.Fatalf("response = %#v", resp)
	}
}

func TestGoogleModelCreateAndDeleteCachedContent(t *testing.T) {
	var createBody map[string]any
	var sawDelete bool
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodPost:
			if req.URL.Path != "/v1beta/cachedContents" {
				t.Fatalf("create path: %s", req.URL.Path)
			}
			if err := json.NewDecoder(req.Body).Decode(&createBody); err != nil {
				t.Fatal(err)
			}
			return jsonResponse(req, http.StatusOK, `{"name":"cachedContents/cache_123"}`), nil
		case http.MethodDelete:
			sawDelete = true
			if req.URL.Path != "/v1beta/cachedContents/cache_123" {
				t.Fatalf("delete path: %s", req.URL.Path)
			}
			return jsonResponse(req, http.StatusOK, `{}`), nil
		default:
			t.Fatalf("method: %s", req.Method)
			return nil, nil
		}
	})}

	model := NewGoogleModel(GoogleConfig{
		BaseURL:    "http://provider.test",
		APIKey:     "google-key",
		HTTPClient: client,
	})
	name, err := model.CreateCachedContent(
		context.Background(),
		"google/gemini-2.5-flash",
		"You are helpful.",
		[]string{"large stable document"},
		3600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if name != "cachedContents/cache_123" {
		t.Fatalf("name = %q", name)
	}
	if createBody["model"] != "models/gemini-2.5-flash" || createBody["ttl"] != "3600s" {
		t.Fatalf("create body = %#v", createBody)
	}
	if err := model.DeleteCachedContent(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if !sawDelete {
		t.Fatal("delete not called")
	}
}

func TestAzureOpenAIModelUsesAPIKeyHeader(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/openai/deployments/gpt-4o/chat/completions" {
			t.Fatalf("path: %s", req.URL.Path)
		}
		if req.URL.Query().Get("api-version") != "2024-06-01" {
			t.Fatalf("query: %s", req.URL.RawQuery)
		}
		if req.Header.Get("api-key") != "azure-key" {
			t.Fatalf("api-key: %q", req.Header.Get("api-key"))
		}
		if req.Header.Get("Authorization") != "" {
			t.Fatalf("authorization should be empty, got %q", req.Header.Get("Authorization"))
		}
		return jsonResponse(req, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`), nil
	})}
	model := NewAzureOpenAIModel(AzureOpenAIConfig{
		Endpoint:   "http://azure.test",
		APIKey:     "azure-key",
		Deployment: "gpt-4o",
		APIVersion: "2024-06-01",
		HTTPClient: client,
	})
	resp, err := model.Generate(context.Background(), GenerateRequest{
		Messages: []Message{{Role: MessageRoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content != "ok" {
		t.Fatalf("response = %#v", resp)
	}
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
