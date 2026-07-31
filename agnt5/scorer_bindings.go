package agnt5

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func applyScorerFieldBindings(request ScorerRequest) (ScorerRequest, map[string]any, error) {
	metadata := make(map[string]any)
	var err error
	request.Output, err = bindScorerRequestField(request.Config, "output", request.Output, metadata)
	if err != nil {
		return ScorerRequest{}, nil, err
	}
	if request.Expected != nil || hasScorerFieldBinding(request.Config, "expected") {
		request.Expected, err = bindScorerRequestField(request.Config, "expected", request.Expected, metadata)
		if err != nil {
			return ScorerRequest{}, nil, err
		}
	}
	if request.Input != nil || hasScorerFieldBinding(request.Config, "input") {
		request.Input, err = bindScorerRequestField(request.Config, "input", request.Input, metadata)
		if err != nil {
			return ScorerRequest{}, nil, err
		}
	}
	return request, metadata, nil
}

func hasScorerFieldBinding(config map[string]any, root string) bool {
	if len(config) == 0 {
		return false
	}
	_, hasField := config[root+"_field"]
	_, hasType := config[root+"_type"]
	return hasField || hasType
}

func bindScorerRequestField(config map[string]any, root string, value any, metadata map[string]any) (any, error) {
	fieldKey := root + "_field"
	typeKey := root + "_type"
	selected := value
	if selector, ok := stringConfig(config, fieldKey); ok && strings.TrimSpace(selector) != "" {
		var err error
		selected, err = selectScorerValue(value, strings.TrimSpace(selector), root)
		if err != nil {
			return nil, err
		}
		metadata[fieldKey] = strings.TrimSpace(selector)
	}
	if rawType, ok := stringConfig(config, typeKey); ok {
		expectedType := scorerBindingType(rawType)
		if expectedType != "" {
			if !scorerValueMatchesType(selected, expectedType) {
				return nil, fmt.Errorf("%s selected %s; expected %s", fieldKey, scorerValueType(selected), expectedType)
			}
			metadata[typeKey] = expectedType
		}
	}
	return selected, nil
}

func selectScorerValue(value any, selector, root string) (any, error) {
	path := selector
	if selector == root {
		path = ""
	} else if strings.HasPrefix(selector, root+".") {
		path = strings.TrimPrefix(selector, root+".")
	}
	if path == "" {
		return value, nil
	}
	current := value
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return nil, fmt.Errorf("%s_field %q contains an empty path segment", root, selector)
		}
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("%s_field %q was not found", root, selector)
			}
			current = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("%s_field %q was not found", root, selector)
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("%s_field %q was not found", root, selector)
		}
	}
	return current, nil
}

func scorerBindingType(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "", "score", "classification", "json":
		return ""
	default:
		return normalized
	}
}

func scorerValueMatchesType(value any, expected string) bool {
	switch expected {
	case "null":
		return value == nil
	case "bool", "boolean":
		_, ok := value.(bool)
		return ok
	case "number":
		return isScorerNumber(value)
	case "string":
		_, ok := value.(string)
		return ok
	case "array":
		return reflect.ValueOf(value).IsValid() && reflect.ValueOf(value).Kind() == reflect.Slice
	case "object":
		return reflect.ValueOf(value).IsValid() && reflect.ValueOf(value).Kind() == reflect.Map
	default:
		return false
	}
}

func scorerValueType(value any) string {
	if value == nil {
		return "null"
	}
	if _, ok := value.(bool); ok {
		return "boolean"
	}
	if isScorerNumber(value) {
		return "number"
	}
	if _, ok := value.(string); ok {
		return "string"
	}
	kind := reflect.ValueOf(value).Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		return "array"
	}
	if kind == reflect.Map || kind == reflect.Struct {
		return "object"
	}
	return kind.String()
}

func isScorerNumber(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return true
	default:
		return false
	}
}

func normalizeScorerResult(result ScorerResult) ScorerResult {
	if math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
		result.Score = 0
	}
	result.Score = math.Max(0, math.Min(1, result.Score))
	result.Metadata = cloneAnyMap(result.Metadata)
	return result
}

// NewScorerResult creates a clamped scorer result and infers Passed at 0.5.
func NewScorerResult(score float64, explanation string) ScorerResult {
	result := ScorerResult{Score: score, Passed: score >= 0.5, Explanation: explanation}
	return normalizeScorerResult(result)
}

// PassingScorerResult creates a fully passing scorer result.
func PassingScorerResult(explanation string) ScorerResult {
	return ScorerResult{Score: 1, Passed: true, Explanation: explanation}
}

// FailingScorerResult creates a fully failing scorer result.
func FailingScorerResult(explanation string) ScorerResult {
	return ScorerResult{Score: 0, Passed: false, Explanation: explanation}
}

func scorerConfigError(explanation string) ScorerResult {
	return ScorerResult{Score: 0, Passed: false, Label: "config_error", Explanation: explanation}
}

func scorerInputError(explanation string) ScorerResult {
	return ScorerResult{Score: 0, Passed: false, Label: "input_error", Explanation: explanation}
}

func mergeAnyMaps(maps ...map[string]any) map[string]any {
	var out map[string]any
	for _, values := range maps {
		if len(values) == 0 {
			continue
		}
		if out == nil {
			out = make(map[string]any)
		}
		for key, value := range values {
			out[key] = value
		}
	}
	return out
}

func stringConfig(config map[string]any, key string) (string, bool) {
	value, ok := config[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}
