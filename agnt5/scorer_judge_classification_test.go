package agnt5

import (
	"context"
	"strings"
	"testing"
)

func TestClassificationJudge(t *testing.T) {
	for _, tc := range []struct{ response, label string }{
		{`{"score":0.95,"passed":true}`, "Pass"},
		{`{"score":0.1,"passed":false}`, "Fail"},
		{`{"passed":true}`, "Pass"},
		{`{"score":0.95}`, "Pass"},
		{`{"label":"Fail","score":0.95,"passed":true}`, "Fail"},
		{`{}`, ""},
		{`{"label":"Maybe"}`, ""},
		{`{"passed":"true"}`, ""},
		{`not json`, ""},
	} {
		t.Run(tc.response, func(t *testing.T) {
			model := &recordingModel{response: GenerateResponse{Content: tc.response}}
			ctx := WithLLMJudgeModel(context.Background(), model)
			result, err := runLLMJudge(ctx, ScorerRequest{Output: "84", Config: map[string]any{
				"criteria": "Correct?", "model": "gpt-test", "choice_scores": map[string]float64{"Fail": 0, "Pass": 1},
			}})
			if tc.label == "" {
				if err == nil {
					t.Fatalf("unusable output returned a score: %+v", result)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Label != tc.label {
				t.Fatalf("label = %q, want %q", result.Label, tc.label)
			}
			if result.Passed != (tc.label == "Pass") {
				t.Fatalf("passed = %v", result.Passed)
			}
			system := model.request.Messages[0].Content
			if !strings.Contains(system, `"label"`) || !strings.Contains(system, "Pass") || !strings.Contains(system, "Fail") {
				t.Fatalf("classification system prompt lacks allowed labels: %s", system)
			}
		})
	}
}

func TestMulticlassJudgeNearestScore(t *testing.T) {
	for _, tc := range []struct{ response, label string }{
		{`{"score":0.8}`, "Good"}, {`{"score":0.6}`, "Partial"}, {`{"score":0.75}`, ""},
	} {
		t.Run(tc.response, func(t *testing.T) {
			ctx := WithLLMJudgeModel(context.Background(), StaticModel{Content: tc.response})
			result, err := runLLMJudge(ctx, ScorerRequest{Output: "84", Config: map[string]any{
				"criteria": "Quality?", "model": "gpt-test", "choice_scores": map[string]float64{"Bad": 0, "Partial": 0.5, "Good": 1},
			}})
			if tc.label == "" {
				if err == nil {
					t.Fatal("ambiguous label must be an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Label != tc.label {
				t.Fatalf("label = %q, want %q", result.Label, tc.label)
			}
		})
	}
}
