package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

var BuiltInDeterministicScorerNames = []string{
	"exact_match", "contains", "regex_match", "json_valid", "json_schema",
	"numeric_range", "levenshtein", "tool_called", "tool_not_called",
	"tool_sequence", "tool_sequence_in_order", "tool_sequence_exact",
	"tool_sequence_any_order", "tool_trajectory", "tool_params_match",
	"max_tool_calls", "max_llm_calls", "max_tokens", "duration_under",
	"no_errors", "tool_failure_recovered", "step_efficiency", "plan_quality",
	"plan_adherence", "state_equals",
}

var BuiltInJudgeScorerNames = []string{
	"llm_judge", "correctness", "faithfulness", "goal_success", "agent_judge",
}

func builtInScorerConfigs() []ScorerConfig {
	configs := make([]ScorerConfig, 0, len(BuiltInDeterministicScorerNames)+len(BuiltInJudgeScorerNames))
	for _, name := range BuiltInDeterministicScorerNames {
		scope := ScorerScopeItem
		if name != "exact_match" && name != "contains" && name != "regex_match" && name != "json_valid" && name != "json_schema" && name != "numeric_range" && name != "levenshtein" {
			scope = ScorerScopeTrace
		}
		scorerName := name
		configs = append(configs, ScorerConfig{
			Name:        scorerName,
			Description: builtInScorerDescription(scorerName),
			Scope:       scope,
			Handler: func(_ context.Context, request ScorerRequest) (ScorerResult, error) {
				return runDeterministicBuiltIn(scorerName, request), nil
			},
		})
	}
	for _, name := range BuiltInJudgeScorerNames {
		scorerName := name
		scope := ScorerScopeItem
		if name == "goal_success" {
			scope = ScorerScopeSession
		}
		configs = append(configs, ScorerConfig{
			Name:        scorerName,
			Description: builtInScorerDescription(scorerName),
			Scope:       scope,
			IsAsync:     true,
			Handler: func(ctx context.Context, request ScorerRequest) (ScorerResult, error) {
				return runJudgeBuiltIn(ctx, scorerName, request), nil
			},
		})
	}
	return configs
}

func mustBuiltInScorer(name string) ScorerConfig {
	for _, config := range builtInScorerConfigs() {
		if config.Name == name {
			return config
		}
	}
	panic("unknown built-in scorer: " + name)
}

func builtInScorerDescription(name string) string {
	descriptions := map[string]string{
		"exact_match": "Exact value match", "contains": "Substring containment check",
		"regex_match": "Regular expression match", "json_valid": "Valid JSON check",
		"json_schema": "Validate against a JSON Schema", "numeric_range": "Numeric output is in range",
		"levenshtein": "Levenshtein similarity", "llm_judge": "LLM-as-judge evaluation",
		"correctness": "Managed correctness judge", "faithfulness": "Managed faithfulness judge",
		"goal_success": "Managed session goal-success judge", "agent_judge": "Evidence-inspecting agent judge",
	}
	if description := descriptions[name]; description != "" {
		return description
	}
	return "AGNT5 built-in " + name + " scorer"
}

func runDeterministicBuiltIn(name string, request ScorerRequest) ScorerResult {
	switch name {
	case "exact_match":
		return exactMatchResult(request)
	case "contains":
		return containsResult(request)
	case "regex_match":
		return regexMatchResult(request)
	case "json_valid":
		return jsonValidResult(request)
	case "json_schema":
		return jsonSchemaResult(request)
	case "numeric_range":
		return numericRangeResult(request)
	case "levenshtein":
		return levenshteinResult(request)
	default:
		return runTraceMetric(name, request)
	}
}

func exactMatchResult(request ScorerRequest) ScorerResult {
	caseSensitive := boolConfigDefault(request.Config, "case_sensitive", true)
	output := formatScorerValue(request.Output)
	expected := formatScorerValue(request.Expected)
	if !caseSensitive {
		output = strings.ToLower(output)
		expected = strings.ToLower(expected)
	}
	passed := output == expected
	label := "mismatch"
	if passed {
		label = "match"
	}
	return binaryScorerResult(passed, label, "")
}

func containsResult(request ScorerRequest) ScorerResult {
	patternValue, configured := request.Config["pattern"]
	if !configured {
		patternValue = request.Expected
	}
	pattern := formatScorerValue(patternValue)
	output := formatScorerValue(request.Output)
	caseSensitive := boolConfigDefault(request.Config, "case_sensitive", true)
	if !caseSensitive {
		pattern = strings.ToLower(pattern)
		output = strings.ToLower(output)
	}
	passed := strings.Contains(output, pattern)
	label := "not_found"
	if passed {
		label = "found"
	}
	return binaryScorerResult(passed, label, "")
}

func regexMatchResult(request ScorerRequest) ScorerResult {
	patternValue, configured := request.Config["pattern"]
	if !configured {
		patternValue = request.Expected
	}
	pattern := formatScorerValue(patternValue)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return ScorerResult{Score: 0, Passed: false, Label: "error", Explanation: "Invalid regex: " + err.Error()}
	}
	passed := re.MatchString(formatScorerValue(request.Output))
	label := "no_match"
	if passed {
		label = "match"
	}
	return binaryScorerResult(passed, label, "")
}

func jsonValidResult(request ScorerRequest) ScorerResult {
	valid := true
	if value, ok := request.Output.(string); ok {
		var decoded any
		valid = json.Unmarshal([]byte(value), &decoded) == nil
	}
	label := "invalid"
	if valid {
		label = "valid"
	}
	return binaryScorerResult(valid, label, "")
}

func jsonSchemaResult(request ScorerRequest) ScorerResult {
	rawSchema, ok := request.Config["schema"]
	if !ok {
		return scorerConfigError("json_schema requires `config.schema`")
	}
	value := request.Output
	if encoded, ok := value.(string); ok {
		if err := json.Unmarshal([]byte(encoded), &value); err != nil {
			return ScorerResult{Score: 0, Passed: false, Label: "parse_error", Explanation: "output is not valid JSON: " + err.Error()}
		}
	}
	schemaDoc, err := jsonSchemaValue(rawSchema)
	if err != nil {
		return scorerConfigError("invalid schema: " + err.Error())
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const schemaURL = "urn:agnt5:eval:json-schema"
	if err := compiler.AddResource(schemaURL, schemaDoc); err != nil {
		return scorerConfigError("invalid schema: " + err.Error())
	}
	validator, err := compiler.Compile(schemaURL)
	if err != nil {
		return scorerConfigError("invalid schema: " + err.Error())
	}
	instance, err := jsonSchemaValue(value)
	if err != nil {
		return ScorerResult{Score: 0, Passed: false, Label: "parse_error", Explanation: "output is not valid JSON: " + err.Error()}
	}
	if err := validator.Validate(instance); err != nil {
		errorsList := flattenJSONSchemaErrors(err)
		explanation := "schema validation failed"
		if len(errorsList) > 0 {
			explanation = errorsList[0]
		}
		return ScorerResult{Score: 0, Passed: false, Label: "invalid", Explanation: explanation, Metadata: map[string]any{"errors": errorsList}}
	}
	return ScorerResult{Score: 1, Passed: true, Label: "valid"}
}

func jsonSchemaValue(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
}

func flattenJSONSchemaErrors(err error) []string {
	var validationError *jsonschema.ValidationError
	if !errors.As(err, &validationError) {
		return []string{err.Error()}
	}
	out := make([]string, 0)
	var visit func(*jsonschema.ValidationError)
	visit = func(current *jsonschema.ValidationError) {
		if len(current.Causes) == 0 {
			out = append(out, current.Error())
			return
		}
		for _, cause := range current.Causes {
			visit(cause)
		}
	}
	visit(validationError)
	return out
}

func numericRangeResult(request ScorerRequest) ScorerResult {
	minimum, hasMinimum := floatConfig(request.Config, "min")
	maximum, hasMaximum := floatConfig(request.Config, "max")
	if !hasMinimum && !hasMaximum {
		return scorerConfigError("numeric_range requires at least one of `min` or `max`")
	}
	value, ok := scorerFloat(request.Output)
	if !ok {
		return ScorerResult{Score: 0, Passed: false, Label: "parse_error", Explanation: "output is not numeric: " + formatScorerValue(request.Output)}
	}
	inclusive := boolConfigDefault(request.Config, "inclusive", true)
	aboveMinimum := !hasMinimum || (inclusive && value >= minimum) || (!inclusive && value > minimum)
	belowMaximum := !hasMaximum || (inclusive && value <= maximum) || (!inclusive && value < maximum)
	passed := aboveMinimum && belowMaximum
	label := "out_of_range"
	if passed {
		label = "in_range"
	}
	explanation := fmt.Sprintf("value=%v, min=%s, max=%s, inclusive=%t", value, optionalFloatString(minimum, hasMinimum), optionalFloatString(maximum, hasMaximum), inclusive)
	return binaryScorerResult(passed, label, explanation)
}

func levenshteinResult(request ScorerRequest) ScorerResult {
	output := []rune(formatScorerValue(request.Output))
	expected := []rune(formatScorerValue(request.Expected))
	distance := levenshteinDistance(output, expected)
	maxLength := len(output)
	if len(expected) > maxLength {
		maxLength = len(expected)
	}
	similarity := 1.0
	if maxLength > 0 {
		similarity = 1 - float64(distance)/float64(maxLength)
	}
	threshold, ok := floatConfig(request.Config, "threshold")
	if !ok {
		threshold = 0.5
	}
	return ScorerResult{Score: similarity, Passed: similarity >= threshold, Explanation: fmt.Sprintf("Edit distance: %d, similarity: %.1f%%", distance, similarity*100)}
}

func levenshteinDistance(a, b []rune) int {
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)
	for index := range previous {
		previous[index] = index
	}
	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = minInt(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}

func minInt(values ...int) int {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}

func binaryScorerResult(passed bool, label, explanation string) ScorerResult {
	score := 0.0
	if passed {
		score = 1
	}
	return ScorerResult{Score: score, Passed: passed, Label: label, Explanation: explanation}
}

func formatScorerValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	encoded, err := json.Marshal(value)
	if err == nil {
		return string(encoded)
	}
	return fmt.Sprint(value)
}

func boolConfigDefault(config map[string]any, key string, fallback bool) bool {
	if value, ok := config[key].(bool); ok {
		return value
	}
	return fallback
}

func floatConfig(config map[string]any, key string) (float64, bool) {
	value, ok := config[key]
	if !ok || value == nil {
		return 0, false
	}
	return scorerFloat(value)
}

func scorerFloat(value any) (float64, bool) {
	var result float64
	switch typed := value.(type) {
	case float64:
		result = typed
	case float32:
		result = float64(typed)
	case int:
		result = float64(typed)
	case int8:
		result = float64(typed)
	case int16:
		result = float64(typed)
	case int32:
		result = float64(typed)
	case int64:
		result = float64(typed)
	case uint:
		result = float64(typed)
	case uint8:
		result = float64(typed)
	case uint16:
		result = float64(typed)
	case uint32:
		result = float64(typed)
	case uint64:
		result = float64(typed)
	case json.Number:
		var err error
		result, err = typed.Float64()
		if err != nil {
			return 0, false
		}
	case string:
		var err error
		result, err = strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0, false
	}
	return result, true
}

func optionalFloatString(value float64, present bool) string {
	if !present {
		return "none"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}
