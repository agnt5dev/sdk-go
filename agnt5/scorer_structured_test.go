package agnt5

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestStructuredAssertionsContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/eval/structured_assertions.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct {
			Name   string
			Input  ScorerRequest
			Expect ScorerResult
		}
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, c := range fixtures.Cases {
		t.Run(c.Name, func(t *testing.T) {
			result, err := NewScorerRegistry().Run(context.Background(), "structured_assertions", c.Input)
			if err != nil {
				t.Fatal(err)
			}
			if result.Score != c.Expect.Score || result.Passed != c.Expect.Passed || result.Label != c.Expect.Label {
				t.Fatalf("got %#v want %#v", result, c.Expect)
			}
		})
	}
}

func TestStructuredAssertionsResourceLimits(t *testing.T) {
	var deep any
	for i := 0; i < 66; i++ {
		deep = []any{deep}
	}
	distinct := make([]int, 1000)
	for i := range distinct {
		distinct[i] = i
	}
	for _, c := range []struct {
		expr   string
		output any
	}{
		{"true", deep}, {"unique(output)", distinct}, {"all(output,is_number)", make([]int, 4097)},
	} {
		r := StructuredAssertions(ScorerRequest{Output: c.output, Config: map[string]any{"assertions": []any{map[string]any{"expr": c.expr}}, "score_threshold": 0}})
		if r.Label != "input_error" || r.Passed || r.Score != 0 {
			t.Fatalf("limit must fail closed: %#v", r)
		}
	}
}

func TestStructuredAssertionsMalformedExpressions(t *testing.T) {
	alphabet := []string{"(", ")", "!", ".", "x", "0", ",", "\"", "[", "é", "💡", "&&"}
	seed := uint32(7)
	for i := 0; i < 1000; i++ {
		expr := ""
		for j := 0; j < 32; j++ {
			seed = seed*1664525 + 1013904223
			expr += alphabet[int(seed)%len(alphabet)]
		}
		r := StructuredAssertions(ScorerRequest{Config: map[string]any{"assertions": []any{map[string]any{"expr": expr}}}})
		if r.Passed {
			t.Fatalf("unexpected pass for %q", expr)
		}
	}
}
