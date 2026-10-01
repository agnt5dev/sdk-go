package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// Mirrors sdk-core's lm::model_caps tests (AGNT5-1403, AGNT5-1456).
func TestModelCapsMatchSDKCore(t *testing.T) {
	for _, model := range []string{"gpt-5", "gpt-5.5", "gpt-6-luna", "openai/gpt-6-luna", "gpt-7", "o1", "o4-mini"} {
		if !isOpenAIReasoningModel(model) {
			t.Errorf("%s should be an OpenAI reasoning model", model)
		}
	}
	for _, model := range []string{"gpt-4o", "gpt-4.1", "gpt-oss-120b", "groq/openai/gpt-oss-20b", "omni-moderation-latest"} {
		if isOpenAIReasoningModel(model) {
			t.Errorf("%s should not be an OpenAI reasoning model", model)
		}
	}
	for _, model := range []string{
		"claude-opus-4-7", "anthropic/claude-opus-5", "claude-sonnet-5", "claude-fable-5-1",
		"anthropic.claude-opus-4-7-v1:0", "us.anthropic.claude-sonnet-5-20260301-v1:0",
		"claude-opus-4-7@20260115", "claude-newfamily-1",
	} {
		if !claudeRejectsSamplingParams(model) {
			t.Errorf("%s should reject sampling parameters", model)
		}
	}
	for _, model := range []string{
		"claude-haiku-4-5", "claude-haiku-4-5-20251001", "claude-sonnet-4-6", "claude-opus-4-6",
		"claude-opus-4-20250514", "claude-3-7-sonnet-20250219", "claude-3-opus-20240229",
		"claude-2.1", "claude-instant-1.2", "anthropic.claude-3-5-sonnet-20240620-v1:0", "gpt-6-luna",
	} {
		if claudeRejectsSamplingParams(model) {
			t.Errorf("%s should accept sampling parameters", model)
		}
	}
}

func captureClient(t *testing.T, captured *map[string]any, status int, body string) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		*captured = nil
		if err := json.NewDecoder(req.Body).Decode(captured); err != nil {
			t.Fatal(err)
		}
		return jsonResponse(req, status, body), nil
	})}
}

const gpt6ChatCompletion = `{"id":"chatcmpl-1","model":"gpt-6-luna","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`

// Chat Completions accepts tools on gpt-6 only with reasoning off, so a Go
// agent with tools failed by default (AGNT5-1325).
func TestOpenAIModelSendsReasoningEffort(t *testing.T) {
	var captured map[string]any
	client := captureClient(t, &captured, http.StatusOK, gpt6ChatCompletion)
	model := NewOpenAIModel(OpenAIConfig{BaseURL: "http://provider.test", APIKey: "sk-test", Model: "gpt-6-luna", HTTPClient: client})
	tools := []Tool{{Name: "lookup", Description: "Look up", Schema: map[string]any{"type": "object"}}}
	messages := []Message{{Role: MessageRoleUser, Content: "hi"}}

	if _, err := model.Generate(context.Background(), GenerateRequest{Messages: messages, Tools: tools}); err != nil {
		t.Fatal(err)
	}
	if captured["reasoning_effort"] != "none" {
		t.Fatalf("gpt-6 with tools: reasoning_effort = %#v, want none", captured["reasoning_effort"])
	}

	if _, err := model.Generate(context.Background(), GenerateRequest{Messages: messages, Tools: tools, ReasoningEffort: "low"}); err != nil {
		t.Fatal(err)
	}
	if captured["reasoning_effort"] != "low" {
		t.Fatalf("explicit effort: reasoning_effort = %#v, want low", captured["reasoning_effort"])
	}

	if _, err := model.Generate(context.Background(), GenerateRequest{Messages: messages}); err != nil {
		t.Fatal(err)
	}
	if _, ok := captured["reasoning_effort"]; ok {
		t.Fatalf("no tools, no effort: reasoning_effort must be omitted: %#v", captured)
	}

	gpt4 := NewOpenAIModel(OpenAIConfig{BaseURL: "http://provider.test", APIKey: "sk-test", Model: "gpt-4.1-mini", HTTPClient: client})
	if _, err := gpt4.Generate(context.Background(), GenerateRequest{Messages: messages, Tools: tools}); err != nil {
		t.Fatal(err)
	}
	if _, ok := captured["reasoning_effort"]; ok {
		t.Fatalf("gpt-4.1 with tools must not get reasoning_effort: %#v", captured)
	}
}

func TestModelProviderErrorKeepsTheProviderBody(t *testing.T) {
	var captured map[string]any
	body := `{"error":{"message":"Unsupported parameter: 'max_tokens' is not supported with this model.","type":"invalid_request_error"}}`
	client := captureClient(t, &captured, http.StatusBadRequest, body)
	model := NewOpenAIModel(OpenAIConfig{BaseURL: "http://provider.test", APIKey: "sk-test", Model: "gpt-6-luna", HTTPClient: client})

	_, err := model.Generate(context.Background(), GenerateRequest{Messages: []Message{{Role: MessageRoleUser, Content: "hi"}}})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") || !strings.Contains(err.Error(), "Unsupported parameter") {
		t.Fatalf("error = %v, want HTTP 400 with the provider's message", err)
	}
}

func TestAnthropicModelDropsSamplingForNewClaude(t *testing.T) {
	var captured map[string]any
	client := captureClient(t, &captured, http.StatusOK, `{"id":"msg_1","model":"claude-opus-5","content":[{"type":"text","text":"391"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	temperature := 0.7
	request := GenerateRequest{Messages: []Message{{Role: MessageRoleUser, Content: "17 x 23?"}}, Temperature: &temperature}

	opus := NewAnthropicModel(AnthropicConfig{BaseURL: "http://provider.test", APIKey: "sk-ant", Model: "claude-opus-5", HTTPClient: client})
	if _, err := opus.Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, ok := captured["temperature"]; ok {
		t.Fatalf("temperature must not reach claude-opus-5: %#v", captured)
	}
	if captured["max_tokens"] != float64(16384) {
		t.Fatalf("claude-opus-5 max_tokens = %#v, want 16384", captured["max_tokens"])
	}

	haiku := NewAnthropicModel(AnthropicConfig{BaseURL: "http://provider.test", APIKey: "sk-ant", Model: "claude-haiku-4-5", HTTPClient: client})
	if _, err := haiku.Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if captured["temperature"] != 0.7 || captured["max_tokens"] != float64(4096) {
		t.Fatalf("claude-haiku-4-5 payload = %#v, want temperature 0.7 and max_tokens 4096", captured)
	}
}

type judgeRecordingModel struct {
	requests []GenerateRequest
	err      error
}

func (m *judgeRecordingModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	m.requests = append(m.requests, request)
	if m.err != nil {
		return GenerateResponse{}, m.err
	}
	return GenerateResponse{Content: `{"score":1,"passed":true,"explanation":"ok"}`}, nil
}

// The judge sent `openai/gpt-6-luna` verbatim and recorded call failures as a
// score of 0 (AGNT5-1374).
func TestLLMJudgeStripsProviderPrefixAndReportsCallFailures(t *testing.T) {
	model := &judgeRecordingModel{}
	ctx := WithLLMJudgeModel(context.Background(), model)
	request := ScorerRequest{Output: "391", Config: map[string]any{"criteria": "correct", "model": "openai/gpt-6-luna"}}
	if _, err := NewScorerRegistry().Run(ctx, "llm_judge", request); err != nil {
		t.Fatal(err)
	}
	if got := model.requests[0].Model; got != "gpt-6-luna" {
		t.Fatalf("judge model = %q, want gpt-6-luna", got)
	}

	failing := &judgeRecordingModel{err: errors.New("agnt5: model provider returned HTTP 400: bad request")}
	ctx = WithLLMJudgeModel(context.Background(), failing)
	result, err := NewScorerRegistry().Run(ctx, "llm_judge", request)
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("judge failure = (%#v, %v), want an error", result, err)
	}
}
