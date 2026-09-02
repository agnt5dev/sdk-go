package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuiltInScorerGoldens(t *testing.T) {
	path := filepath.Join("..", "..", "sdk-core", "test-fixtures", "eval", "builtin_goldens.json")
	if _, err := os.Stat(path); err != nil {
		path = filepath.Join("testdata", "eval", "builtin_goldens.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Name   string          `json:"name"`
			Scorer string          `json:"scorer"`
			Input  json.RawMessage `json:"input"`
			Expect struct {
				Score  float64 `json:"score"`
				Passed bool    `json:"passed"`
				Label  string  `json:"label"`
			} `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		t.Run(test.Name, func(t *testing.T) {
			var request ScorerRequest
			if err := json.Unmarshal(test.Input, &request); err != nil {
				t.Fatal(err)
			}
			result, err := NewScorerRegistry().Run(context.Background(), test.Scorer, request)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(result.Score-test.Expect.Score) > 1e-9 || result.Passed != test.Expect.Passed {
				t.Fatalf("result = %#v, expected score=%v passed=%v", result, test.Expect.Score, test.Expect.Passed)
			}
			if test.Expect.Label != "" && result.Label != test.Expect.Label {
				t.Fatalf("label = %q, expected %q", result.Label, test.Expect.Label)
			}
		})
	}
}

func TestBuiltInRegistryAndWorkerInterception(t *testing.T) {
	registry := NewScorerRegistry()
	if got, want := len(registry.List()), len(BuiltInDeterministicScorerNames)+len(BuiltInJudgeScorerNames); got != want {
		t.Fatalf("built-ins = %d, want %d", got, want)
	}
	worker := NewWorker("eval-worker")
	input := []byte(`{"output":"yes","expected":"yes"}`)
	result, err := worker.invoke(context.Background(), Invocation{ComponentName: "exact_match", ComponentType: ComponentTypeScorer, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	var score ScorerResult
	if err := json.Unmarshal(result.Output, &score); err != nil {
		t.Fatal(err)
	}
	if !score.Passed || score.Score != 1 {
		t.Fatalf("score = %#v", score)
	}
}

func TestLLMJudgeWithInjectedModelAndChoiceScores(t *testing.T) {
	ctx := WithLLMJudgeModel(context.Background(), StaticModel{Content: `{"label":"partial","score":0.1,"passed":false,"explanation":"some evidence"}`})
	result, err := NewScorerRegistry().Run(ctx, "llm_judge", ScorerRequest{
		Output: "answer",
		Config: map[string]any{
			"criteria": "quality", "model": "test-model",
			"choice_scores": map[string]any{"fail": 0.0, "partial": 0.5, "pass": 1.0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Score != 0.5 || result.Passed || result.Label != "partial" {
		t.Fatalf("result = %#v", result)
	}
}

func TestEvaluatorPresetSpecs(t *testing.T) {
	zero := 0.0
	spec := (Correctness{}).ToEvalScorerSpec()
	if spec.Name != "correctness" || spec.Config["preset_version"] != EvaluatorPresetVersion || spec.Config["include_input"] != true {
		t.Fatalf("correctness spec = %#v", spec)
	}
	faithfulness := (Faithfulness{EvaluatorPresetConfig: EvaluatorPresetConfig{ContextFields: []string{"input.context"}}}).ToEvalScorerSpec()
	if faithfulness.Name != "faithfulness" {
		t.Fatalf("faithfulness spec = %#v", faithfulness)
	}
	judge := NewLLMJudge(LLMJudgeConfig{Criteria: "helpful", Model: "anthropic/claude-test"}).ToEvalScorerSpec()
	if judge.Config["provider"] != "anthropic" || judge.Config["model"] != "claude-test" {
		t.Fatalf("judge spec = %#v", judge)
	}
	zeroThreshold := (Correctness{EvaluatorPresetConfig: EvaluatorPresetConfig{Threshold: &zero}}).ToEvalScorerSpec()
	if zeroThreshold.Config["threshold"] != 0.0 {
		t.Fatalf("explicit zero threshold = %#v", zeroThreshold.Config["threshold"])
	}
}

func TestScorerBindingsAndTypedErrors(t *testing.T) {
	registry := NewScorerRegistry()
	result, err := registry.Run(context.Background(), "contains", ScorerRequest{
		Output:   map[string]any{"choices": []any{map[string]any{"text": "hello parity"}}},
		Expected: "parity",
		Config:   map[string]any{"output_field": "output.choices.0.text", "output_type": "string"},
	})
	if err != nil || !result.Passed || result.Metadata["output_field"] != "output.choices.0.text" {
		t.Fatalf("bound result = %#v, err = %v", result, err)
	}
	_, err = registry.Run(context.Background(), "missing_scorer", ScorerRequest{})
	var notFound *ScorerNotFoundError
	if !errors.As(err, &notFound) || notFound.Name != "missing_scorer" {
		t.Fatalf("missing scorer error = %#v", err)
	}
}

func TestBatchEvalAndResponseHelpers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"ok":true},"scores":[{"name":"exact_match","score":1,"passed":true,"label":"match"}],"run_id":"run-1","trace_id":"trace-1","duration_ms":4}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	result := client.BatchEval(context.Background(), "check", []BatchEvalItem{
		{Input: map[string]any{"id": 1}, Expected: true},
		{Input: map[string]any{"id": 2}, Expected: true},
	}, BatchEvalOptions{MaxConcurrency: 2})
	if !result.IsSuccess() || result.PassRate() != 1 || len(result.Results) != 2 {
		t.Fatalf("batch = %#v", result)
	}
	if score, ok := result.Results[0].GetScore("exact_match"); !ok || score.Label != "match" {
		t.Fatalf("scores = %#v", result.Results[0].Scores)
	}
}

func TestTraceAssertionsAndMetrics(t *testing.T) {
	events := []TraceEvent{
		{EventType: "lm.completed", TimestampNS: 1_000_000, Data: map[string]any{"total_tokens": 10}},
		{EventType: "tool.call.completed", EventID: "event-2", CorrelationID: "call-1", TimestampNS: 2_000_000, Name: "search", Data: map[string]any{"name": "search", "arguments": `{"q":"go"}`}},
	}
	assertionResult := TraceScorer(events, []TraceAssertion{MaxTokens(20), MaxLMCalls(1), NoErrors(), DurationUnder(5 * time.Millisecond)})
	if !assertionResult.Passed || assertionResult.Score != 1 {
		t.Fatalf("assertions = %#v", assertionResult)
	}
	if names := ToolCallNames(ExtractToolCallsFromEvents(events)); len(names) != 1 || names[0] != "search" {
		t.Fatalf("tool names = %#v", names)
	}
	traceContext := TraceEvalContext{
		SchemaVersion: TraceEvalContextSchema, ProjectID: "project-1", SessionID: "session-1", RootRunID: "run-1",
		ExecutionSteps: []TraceEvalExecutionStep{
			{Index: 1, Kind: "tool_call", ToolName: "search", Status: "completed"},
			{Index: 2, Kind: "tool_call", ToolName: "lookup", Status: "completed"},
		},
		Features: TraceEvalFeatures{ToolCallCount: 2},
	}
	result, err := NewScorerRegistry().Run(context.Background(), "tool_sequence_exact", ScorerRequest{TraceEvalContext: traceContext, Config: map[string]any{"tools": []string{"search", "lookup"}}})
	if err != nil || !result.Passed {
		t.Fatalf("trajectory = %#v, err = %v", result, err)
	}
}
