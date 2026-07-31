package agnt5

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
)

const TraceEvalContextSchema = "agnt5.eval.trace_eval_context.v1"

// ToolTrajectoryMode controls tool-call order matching.
type ToolTrajectoryMode string

const (
	ToolTrajectoryExact    ToolTrajectoryMode = "exact"
	ToolTrajectoryInOrder  ToolTrajectoryMode = "in_order"
	ToolTrajectoryAnyOrder ToolTrajectoryMode = "any_order"
)

// TraceEvalContext is the redacted, normalized artifact used by trace scorers.
type TraceEvalContext struct {
	SchemaVersion           string                   `json:"schema_version"`
	SessionID               string                   `json:"session_id"`
	ProjectID               string                   `json:"project_id"`
	DeploymentID            string                   `json:"deployment_id,omitempty"`
	RootRunID               string                   `json:"root_run_id"`
	TraceID                 string                   `json:"trace_id,omitempty"`
	Task                    *TraceEvalTask           `json:"task,omitempty"`
	Session                 TraceEvalSession         `json:"session,omitempty"`
	Plan                    TraceEvalPlan            `json:"plan,omitempty"`
	ExecutionSteps          []TraceEvalExecutionStep `json:"execution_steps,omitempty"`
	Features                TraceEvalFeatures        `json:"features,omitempty"`
	EvidenceRefs            any                      `json:"evidence_refs,omitempty"`
	RedactionPolicySnapshot any                      `json:"redaction_policy_snapshot,omitempty"`
}

type TraceEvalTask struct {
	TextSafe string `json:"text_safe,omitempty"`
}

type TraceEvalSession struct {
	TurnCount int64           `json:"turn_count,omitempty"`
	Turns     []TraceEvalTurn `json:"turns,omitempty"`
}

type TraceEvalTurn struct {
	TurnIndex   int64  `json:"turn_index,omitempty"`
	Role        string `json:"role,omitempty"`
	StartedAt   int64  `json:"started_at,omitempty"`
	EndedAt     int64  `json:"ended_at,omitempty"`
	MessageRef  string `json:"message_ref,omitempty"`
	MessageHash string `json:"message_hash,omitempty"`
	SummarySafe string `json:"summary_safe,omitempty"`
}

type TraceEvalPlan struct {
	Detected bool                `json:"detected"`
	Steps    []TraceEvalPlanStep `json:"steps,omitempty"`
}

type TraceEvalPlanStep struct {
	Index          int64  `json:"index,omitempty"`
	TextSafe       string `json:"text_safe,omitempty"`
	ExpectedAction string `json:"expected_action,omitempty"`
	ExpectedTool   string `json:"expected_tool,omitempty"`
}

type TraceEvalExecutionStep struct {
	Index           int64    `json:"index"`
	Kind            string   `json:"kind"`
	SpanID          string   `json:"span_id,omitempty"`
	RunID           string   `json:"run_id,omitempty"`
	Name            string   `json:"name,omitempty"`
	Role            string   `json:"role,omitempty"`
	Status          string   `json:"status,omitempty"`
	StartedAt       int64    `json:"started_at,omitempty"`
	EndedAt         int64    `json:"ended_at,omitempty"`
	DurationMS      int64    `json:"duration_ms,omitempty"`
	SummarySafe     string   `json:"summary_safe,omitempty"`
	ToolName        string   `json:"tool_name,omitempty"`
	Provider        string   `json:"provider,omitempty"`
	Model           string   `json:"model,omitempty"`
	Tokens          int64    `json:"tokens,omitempty"`
	InputRef        string   `json:"input_ref,omitempty"`
	InputHash       string   `json:"input_hash,omitempty"`
	OutputRef       string   `json:"output_ref,omitempty"`
	OutputHash      string   `json:"output_hash,omitempty"`
	ArgumentsRef    string   `json:"arguments_ref,omitempty"`
	ArgumentsHash   string   `json:"arguments_hash,omitempty"`
	ResultRef       string   `json:"result_ref,omitempty"`
	ResultHash      string   `json:"result_hash,omitempty"`
	PromptRef       string   `json:"prompt_ref,omitempty"`
	PromptHash      string   `json:"prompt_hash,omitempty"`
	ResponseRef     string   `json:"response_ref,omitempty"`
	ResponseHash    string   `json:"response_hash,omitempty"`
	ErrorCode       string   `json:"error_code,omitempty"`
	ErrorClass      string   `json:"error_class,omitempty"`
	ErrorSafe       string   `json:"error_safe,omitempty"`
	MatchesPlanStep *int64   `json:"matches_plan_step,omitempty"`
	Flags           []string `json:"flags,omitempty"`
}

type TraceEvalStepGroup struct {
	ToolName string  `json:"tool_name,omitempty"`
	StepIDs  []int64 `json:"step_ids,omitempty"`
	Reason   string  `json:"reason,omitempty"`
}

type TraceEvalRetryGroup struct {
	StepIDs []int64 `json:"step_ids,omitempty"`
	Reason  string  `json:"reason,omitempty"`
}

type TraceEvalMissingPlanStep struct {
	PlanStepIndex int64  `json:"plan_step_index,omitempty"`
	TextSafe      string `json:"text_safe,omitempty"`
}

type TraceEvalFeatures struct {
	ExecutionStepCount  int64                      `json:"execution_step_count,omitempty"`
	ToolCallCount       int64                      `json:"tool_call_count,omitempty"`
	UniqueToolCallCount int64                      `json:"unique_tool_call_count,omitempty"`
	TurnCount           int64                      `json:"turn_count,omitempty"`
	LMCallCount         int64                      `json:"llm_call_count,omitempty"`
	TotalTokens         int64                      `json:"total_tokens,omitempty"`
	ErrorCount          int64                      `json:"error_count,omitempty"`
	DuplicateToolCalls  []TraceEvalStepGroup       `json:"duplicate_tool_calls,omitempty"`
	PlanStepsTotal      int64                      `json:"plan_steps_total,omitempty"`
	PlanStepsMatched    int64                      `json:"plan_steps_matched,omitempty"`
	PlanStepsMissing    []TraceEvalMissingPlanStep `json:"plan_steps_missing,omitempty"`
	OffPathSteps        []int64                    `json:"off_path_steps,omitempty"`
	RetryGroups         []TraceEvalRetryGroup      `json:"retry_groups,omitempty"`
}

// ExtractToolCallsFromEvents normalizes tool calls from journal event payloads.
func ExtractToolCallsFromEvents(events []TraceEvent) []ToolCall {
	calls := make([]ToolCall, 0)
	byKey := make(map[string]int)
	add := func(call ToolCall, fallback string) {
		if call.Name == "" {
			return
		}
		key := call.CallID
		if key == "" {
			key = call.SpanID
		}
		if key == "" {
			key = fallback
		}
		if index, ok := byKey[key]; ok {
			calls[index] = mergeToolCall(calls[index], call)
			return
		}
		byKey[key] = len(calls)
		calls = append(calls, call)
	}
	for _, event := range events {
		payloads := toolCallPayloads(event.Data)
		for index, payload := range payloads {
			add(toolCallFromPayload(payload, event), fmt.Sprintf("%s:payload:%d", event.EventID, index))
		}
		if strings.Contains(strings.ToLower(event.EventType), "tool") {
			add(toolCallFromPayload(event.Data, event), event.EventID)
		}
	}
	return calls
}

func ToolCallNames(calls []ToolCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		if call.Name != "" {
			out = append(out, call.Name)
		}
	}
	return out
}

func ToolTrajectoryMatches(actual, expected []string, mode ToolTrajectoryMode) bool {
	switch mode {
	case ToolTrajectoryInOrder:
		index := 0
		if len(expected) == 0 {
			return true
		}
		for _, name := range actual {
			if name == expected[index] {
				index++
				if index == len(expected) {
					return true
				}
			}
		}
		return false
	case ToolTrajectoryAnyOrder:
		counts := make(map[string]int)
		for _, name := range actual {
			counts[name]++
		}
		for _, name := range expected {
			if counts[name] == 0 {
				return false
			}
			counts[name]--
		}
		return true
	default:
		return reflect.DeepEqual(actual, expected)
	}
}

func toolCallPayloads(data map[string]any) []map[string]any {
	out := make([]map[string]any, 0)
	addList := func(value any) {
		if values, ok := value.([]any); ok {
			for _, item := range values {
				if mapped, ok := item.(map[string]any); ok {
					out = append(out, mapped)
				}
			}
		}
	}
	addList(data["tool_calls"])
	addList(data["toolCalls"])
	for _, key := range []string{"normalized_session", "session", "trace_session", "journal_session", "response", "output", "message"} {
		if nested, ok := data[key].(map[string]any); ok {
			addList(nested["tool_calls"])
			addList(nested["toolCalls"])
		}
	}
	if choices, ok := data["choices"].([]any); ok {
		for _, choice := range choices {
			if mapped, ok := choice.(map[string]any); ok {
				if message, ok := mapped["message"].(map[string]any); ok {
					addList(message["tool_calls"])
					addList(message["toolCalls"])
				}
			}
		}
	}
	return out
}

func toolCallFromPayload(payload map[string]any, event TraceEvent) ToolCall {
	function, _ := payload["function"].(map[string]any)
	name := firstNonEmptyString(payload["name"], payload["tool_name"], function["name"])
	if name == "" && strings.Contains(strings.ToLower(event.EventType), "tool") {
		name = event.Name
	}
	callID := firstNonEmptyString(payload["call_id"], payload["tool_call_id"], payload["id"])
	if callID == "" && strings.Contains(strings.ToLower(event.EventType), "tool") {
		callID = event.CorrelationID
	}
	argumentValue := firstPresentValue(payload["arguments"], payload["args"], function["arguments"])
	if encoded, ok := argumentValue.(string); ok {
		var decoded any
		if json.Unmarshal([]byte(encoded), &decoded) == nil {
			argumentValue = decoded
		}
	}
	arguments, _ := argumentValue.(map[string]any)
	raw := map[string]any{}
	if argumentValue != nil && arguments == nil {
		raw["arguments"] = argumentValue
	}
	status := firstNonEmptyString(payload["status"])
	if status == "" {
		switch {
		case strings.HasSuffix(event.EventType, ".started"):
			status = "started"
		case strings.HasSuffix(event.EventType, ".completed"):
			status = "completed"
		case strings.HasSuffix(event.EventType, ".failed"):
			status = "failed"
		}
	}
	return ToolCall{
		ID: callID, Name: name, Arguments: arguments, Raw: raw, CallID: callID,
		SpanID:      firstNonEmptyString(payload["span_id"], event.CorrelationID),
		TimestampNS: firstInt64(payload["timestamp_ns"], event.TimestampNS),
		StartedAt:   firstInt64(payload["started_at"]), EndedAt: firstInt64(payload["ended_at"]),
		Status:   status,
		Metadata: map[string]any{"source_event_id": event.EventID, "source_event_type": event.EventType},
	}
}

func mergeToolCall(existing, incoming ToolCall) ToolCall {
	if incoming.Name != "" {
		existing.Name = incoming.Name
	}
	if incoming.Arguments != nil {
		existing.Arguments = incoming.Arguments
	}
	if incoming.CallID != "" {
		existing.CallID = incoming.CallID
	}
	if incoming.SpanID != "" {
		existing.SpanID = incoming.SpanID
	}
	if existing.TimestampNS == 0 {
		existing.TimestampNS = incoming.TimestampNS
	}
	if existing.StartedAt == 0 {
		existing.StartedAt = incoming.StartedAt
	}
	if incoming.EndedAt != 0 {
		existing.EndedAt = incoming.EndedAt
	}
	if incoming.Status != "" {
		existing.Status = incoming.Status
	}
	existing.Metadata = mergeAnyMaps(existing.Metadata, incoming.Metadata)
	return existing
}

func runTraceMetric(name string, request ScorerRequest) ScorerResult {
	if name == "state_equals" {
		return stateEqualsResult(request)
	}
	traceContext, failure := traceEvalContextFromRequest(request)
	if failure != nil {
		return *failure
	}
	switch name {
	case "tool_called", "tool_not_called":
		return toolPresenceResult(name, traceContext, request.Config)
	case "tool_sequence", "tool_sequence_in_order":
		return toolSequenceResult(name, traceContext, request.Config, ToolTrajectoryInOrder)
	case "tool_sequence_exact":
		return toolSequenceResult(name, traceContext, request.Config, ToolTrajectoryExact)
	case "tool_sequence_any_order":
		return toolSequenceResult(name, traceContext, request.Config, ToolTrajectoryAnyOrder)
	case "tool_trajectory":
		mode := ToolTrajectoryMode(stringConfigDefault(request.Config, "mode", stringConfigDefault(request.Config, "pattern", "exact")))
		return toolSequenceResult(name, traceContext, request.Config, mode)
	case "tool_params_match":
		return toolParamsMatchResult(traceContext, request.Config)
	case "max_tool_calls", "max_llm_calls", "max_tokens":
		return maxTraceCountResult(name, traceContext, request.Config)
	case "duration_under":
		return durationUnderResult(traceContext, request.Config)
	case "no_errors":
		return noErrorsResult(traceContext, request.Config)
	case "tool_failure_recovered":
		return toolFailureRecoveredResult(traceContext, request.Config)
	case "step_efficiency":
		return stepEfficiencyResult(traceContext, request.Config)
	case "plan_adherence":
		return planAdherenceResult(traceContext, request.Config)
	case "plan_quality":
		return planQualityResult(traceContext, request.Config)
	default:
		return scorerConfigError("unknown built-in scorer " + name)
	}
}

func traceEvalContextFromRequest(request ScorerRequest) (TraceEvalContext, *ScorerResult) {
	raw := request.TraceEvalContext
	if raw == nil {
		raw = request.Config["trace_eval_context"]
	}
	if raw == nil {
		result := ScorerResult{Score: 0, Passed: false, Label: "artifact_error", Explanation: "trace_eval_context is required for trace metric scoring"}
		return TraceEvalContext{}, &result
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		result := ScorerResult{Score: 0, Passed: false, Label: "artifact_error", Explanation: "invalid trace_eval_context: " + err.Error()}
		return TraceEvalContext{}, &result
	}
	var context TraceEvalContext
	if err := json.Unmarshal(encoded, &context); err != nil {
		result := ScorerResult{Score: 0, Passed: false, Label: "artifact_error", Explanation: "invalid trace_eval_context: " + err.Error()}
		return TraceEvalContext{}, &result
	}
	if message := validateTraceEvalContext(context); message != "" {
		result := ScorerResult{Score: 0, Passed: false, Label: "artifact_error", Explanation: message}
		return TraceEvalContext{}, &result
	}
	return context, nil
}

func validateTraceEvalContext(value TraceEvalContext) string {
	if value.SchemaVersion != TraceEvalContextSchema {
		return fmt.Sprintf("invalid trace eval context schema_version %q", value.SchemaVersion)
	}
	if strings.TrimSpace(value.ProjectID) == "" {
		return "project_id is required"
	}
	if strings.TrimSpace(value.SessionID) == "" {
		return "session_id is required"
	}
	if strings.TrimSpace(value.RootRunID) == "" {
		return "root_run_id is required"
	}
	for index, step := range value.ExecutionSteps {
		if step.Index <= 0 {
			return fmt.Sprintf("execution_steps[%d].index must be positive", index)
		}
		if strings.TrimSpace(step.Kind) == "" {
			return fmt.Sprintf("execution_steps[%d].kind is required", index)
		}
	}
	return ""
}

func toolPresenceResult(name string, trace TraceEvalContext, config map[string]any) ScorerResult {
	tool, ok := requiredConfigString(config, "tool")
	if !ok {
		return scorerConfigError(name + " requires `tool` in config")
	}
	actual := traceToolNames(trace)
	found := false
	for _, candidate := range actual {
		if candidate == tool {
			found = true
			break
		}
	}
	matched := found
	passLabel, failLabel := "called", "not_called"
	if name == "tool_not_called" {
		matched = !found
		passLabel, failLabel = "not_called", "called"
	}
	explanation := fmt.Sprintf("tool `%s` was %scalled", tool, map[bool]string{true: "", false: "not "}[found])
	return traceBinaryResult(name, trace, config, matched, passLabel, failLabel, explanation, map[string]any{"expected_tool": tool, "actual_tools": actual})
}

func toolSequenceResult(name string, trace TraceEvalContext, config map[string]any, mode ToolTrajectoryMode) ScorerResult {
	if mode != ToolTrajectoryExact && mode != ToolTrajectoryInOrder && mode != ToolTrajectoryAnyOrder {
		return scorerConfigError(name + " mode must be exact, in_order, or any_order")
	}
	expected, ok := stringSliceConfig(config, "tools")
	if !ok || len(expected) == 0 {
		return scorerConfigError(name + " requires non-empty `tools` in config")
	}
	actual := traceToolNames(trace)
	matched := ToolTrajectoryMatches(actual, expected, mode)
	return traceBinaryResult(name, trace, config, matched, "matched", "not_matched", fmt.Sprintf("tool trajectory %s expected %s sequence", map[bool]string{true: "matched", false: "did not match"}[matched], mode), map[string]any{"mode": mode, "expected_tools": expected, "actual_tools": actual})
}

func toolParamsMatchResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	tool, ok := requiredConfigString(config, "tool")
	if !ok {
		return scorerConfigError("tool_params_match requires `tool` in config")
	}
	expected, ok := config["params"].(map[string]any)
	if !ok || len(expected) == 0 {
		return scorerConfigError("tool_params_match requires `params` object in config")
	}
	matchedIndexes := make([]int64, 0)
	candidates := 0
	for _, step := range trace.ExecutionSteps {
		if step.Kind != "tool_call" || step.ToolName != tool {
			continue
		}
		candidates++
		actual := safeToolAttributes(step)
		matched := true
		for key, value := range expected {
			if !reflect.DeepEqual(actual[key], value) {
				matched = false
				break
			}
		}
		if matched {
			matchedIndexes = append(matchedIndexes, step.Index)
		}
	}
	matched := len(matchedIndexes) > 0
	explanation := fmt.Sprintf("tool `%s` was called, but no call matched the configured safe parameters", tool)
	if matched {
		explanation = fmt.Sprintf("tool `%s` had a call matching the configured safe parameters", tool)
	} else if candidates == 0 {
		explanation = fmt.Sprintf("tool `%s` was not called", tool)
	}
	return traceBinaryResult("tool_params_match", trace, config, matched, "matched", "not_matched", explanation, map[string]any{"tool": tool, "candidate_count": candidates, "matched_step_indexes": matchedIndexes})
}

func maxTraceCountResult(name string, trace TraceEvalContext, config map[string]any) ScorerResult {
	maximum, ok := nonnegativeIntConfig(config, "max")
	if !ok {
		return scorerConfigError(name + " requires non-negative `max` in config")
	}
	var actual int64
	label := "count"
	switch name {
	case "max_tool_calls":
		label = "tool_calls"
		actual = trace.Features.ToolCallCount
		if actual == 0 {
			actual = int64(len(traceToolSteps(trace)))
		}
	case "max_llm_calls":
		label = "llm_calls"
		actual = trace.Features.LMCallCount
		if actual == 0 {
			for _, step := range trace.ExecutionSteps {
				if step.Kind == "llm_call" {
					actual++
				}
			}
		}
	case "max_tokens":
		label = "tokens"
		actual = trace.Features.TotalTokens
		if actual == 0 {
			for _, step := range trace.ExecutionSteps {
				actual += step.Tokens
			}
		}
	}
	matched := actual <= maximum
	return traceBinaryResult(name, trace, config, matched, "within_limit", "over_limit", fmt.Sprintf("%s count was %d (max: %d)", label, actual, maximum), map[string]any{"actual": actual, "max": maximum})
}

func durationUnderResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	maximum, ok := nonnegativeIntConfig(config, "max_ms")
	if !ok {
		return scorerConfigError("duration_under requires non-negative `max_ms` in config")
	}
	actual := traceDurationMS(trace)
	return traceBinaryResult("duration_under", trace, config, actual <= maximum, "within_limit", "over_limit", fmt.Sprintf("trace duration was %dms (max: %dms)", actual, maximum), map[string]any{"actual_ms": actual, "max_ms": maximum})
}

func noErrorsResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	count := traceErrorCount(trace)
	explanation := "trace contained no errors"
	if count > 0 {
		explanation = fmt.Sprintf("trace contained %d error(s)", count)
	}
	return traceBinaryResult("no_errors", trace, config, count == 0, "no_errors", "has_errors", explanation, map[string]any{"error_count": count})
}

func toolFailureRecoveredResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	tool := stringConfigDefault(config, "tool", "")
	errorCode := stringConfigDefault(config, "error_code", stringConfigDefault(config, "errorCode", ""))
	firstFailure := int64(0)
	failures := 0
	for _, step := range trace.ExecutionSteps {
		failed := step.Kind == "tool_call" && (strings.EqualFold(step.Status, "failed") || step.ErrorCode != "" || step.ErrorSafe != "")
		if !failed || (tool != "" && step.ToolName != tool && step.Name != tool) || (errorCode != "" && step.ErrorCode != errorCode) {
			continue
		}
		failures++
		if firstFailure == 0 || step.Index < firstFailure {
			firstFailure = step.Index
		}
	}
	recovered := false
	if firstFailure > 0 {
		for _, step := range trace.ExecutionSteps {
			if step.Index > firstFailure && successfulProgressStep(step) {
				recovered = true
				break
			}
		}
	}
	explanation := "trace contained a failed tool call, but no later successful progress"
	if recovered {
		explanation = "trace recovered after a tool failure"
	} else if failures == 0 {
		explanation = "trace did not contain the configured failed tool call"
	}
	return traceBinaryResult("tool_failure_recovered", trace, config, recovered, "recovered", "not_recovered", explanation, map[string]any{"tool": tool, "error_code": errorCode, "failed_tool_count": failures, "first_failure_index": firstFailure})
}

func stateEqualsResult(request ScorerRequest) ScorerResult {
	name, ok := requiredConfigString(request.Config, "name")
	if !ok {
		return scorerConfigError("state_equals requires `name` in config")
	}
	expected, ok := request.Config["expected"]
	if !ok {
		return scorerConfigError("state_equals requires `expected` in config")
	}
	var actual any
	found := false
	for _, states := range []map[string]any{request.State, request.States, request.StateSnapshots} {
		if value, ok := states[name]; ok {
			actual, found = value, true
			break
		}
	}
	if !found {
		return ScorerResult{Score: 0, Passed: false, Label: "artifact_error", Explanation: fmt.Sprintf("state_equals could not find state snapshot `%s`", name)}
	}
	threshold, failure := scoreThreshold(request.Config, "state_equals", 1)
	if failure != nil {
		return *failure
	}
	matched := reflect.DeepEqual(actual, expected)
	score := 0.0
	if matched {
		score = 1
	}
	label := "not_matched"
	if matched {
		label = "matched"
	}
	return ScorerResult{Score: score, Passed: score >= threshold, Label: label, Explanation: fmt.Sprintf("state `%s` %s expected value", name, map[bool]string{true: "matched", false: "did not match"}[matched]), Metadata: map[string]any{"builtin": "state_equals", "state_name": name, "threshold": threshold, "actual": actual, "expected": expected}}
}

func stepEfficiencyResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	threshold, failure := scoreThreshold(config, "step_efficiency", .8)
	if failure != nil {
		return *failure
	}
	actual := int64(0)
	for _, step := range trace.ExecutionSteps {
		if !(step.Kind == "llm_call" && step.Role == "planning") {
			actual++
		}
	}
	minimum, configured := nonnegativeIntConfig(config, "minimum_steps")
	source := "config"
	if !configured {
		minimum, configured = nonnegativeIntConfig(config, "min_steps")
	}
	if !configured {
		source = "plan"
		minimum = trace.Features.PlanStepsTotal
	}
	if !configured && minimum == 0 {
		source = "inferred"
		minimum = trace.Features.UniqueToolCallCount
		for _, step := range trace.ExecutionSteps {
			if step.Kind == "llm_call" && step.Role == "final_response" {
				minimum++
			}
		}
		if minimum == 0 && actual > 0 {
			minimum = 1
		}
	}
	excess := maxInt64(actual-minimum, 0)
	duplicate := groupExtras(trace.Features.DuplicateToolCalls)
	retries := retryExtras(trace.Features.RetryGroups)
	errors := maxInt64(trace.Features.ErrorCount, 0)
	penalties := excess + duplicate + int64(len(trace.Features.OffPathSteps)) + retries + errors
	denominator := actual + minimum + penalties
	score := 1.0
	if denominator > 0 && penalties > 0 {
		score = 1 - float64(penalties)/float64(denominator)
	}
	score = math.Max(0, math.Min(1, score))
	passed := score >= threshold
	label := "inefficient"
	if passed && score >= .95 {
		label = "efficient"
	} else if score >= .5 {
		label = "needs_review"
	}
	return ScorerResult{Score: score, Passed: passed, Label: label, Explanation: fmt.Sprintf("step efficiency score %.3f: actual_steps=%d, minimum_steps=%d, penalties=%d", score, actual, minimum, penalties), Metadata: map[string]any{"builtin": "step_efficiency", "trace_eval_context_schema": trace.SchemaVersion, "threshold": threshold, "actual_steps": actual, "minimum_steps": minimum, "minimum_steps_source": source, "excess_steps": excess, "penalty_units": penalties, "duplicate_tool_call_count": duplicate, "off_path_step_count": len(trace.Features.OffPathSteps), "retry_extra_count": retries, "error_count": errors}}
}

func planAdherenceResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	threshold, failure := scoreThreshold(config, "plan_adherence", .8)
	if failure != nil {
		return *failure
	}
	total := trace.Features.PlanStepsTotal
	if total == 0 {
		total = int64(len(trace.Plan.Steps))
	}
	if !trace.Plan.Detected || total == 0 {
		return ScorerResult{Score: 0, Passed: false, Label: "no_plan", Explanation: "plan adherence cannot be evaluated because no plan was detected", Metadata: map[string]any{"builtin": "plan_adherence", "reason": "no_plan_detected", "threshold": threshold}}
	}
	matched := trace.Features.PlanStepsMatched
	if matched == 0 {
		seen := map[int64]struct{}{}
		for _, step := range trace.ExecutionSteps {
			if step.MatchesPlanStep != nil {
				seen[*step.MatchesPlanStep] = struct{}{}
			}
		}
		matched = int64(len(seen))
	}
	if matched > total {
		matched = total
	}
	missing := int64(len(trace.Features.PlanStepsMissing))
	if missing == 0 && matched < total {
		missing = total - matched
	}
	offPath := int64(len(trace.Features.OffPathSteps))
	retries := retryExtras(trace.Features.RetryGroups)
	errors := maxInt64(trace.Features.ErrorCount, 0)
	deviations := missing + offPath + retries + errors
	denominator := total + offPath + retries + errors
	score := float64(0)
	if denominator > 0 {
		score = float64(matched) / float64(denominator)
	}
	score = math.Max(0, math.Min(1, score))
	passed := score >= threshold
	label := planMetricLabel(score, passed, "adhered", "partial_adherence", "not_adhered")
	return ScorerResult{Score: score, Passed: passed, Label: label, Explanation: fmt.Sprintf("plan adherence score %.3f: matched_steps=%d/%d, deviations=%d", score, matched, total, deviations), Metadata: map[string]any{"builtin": "plan_adherence", "threshold": threshold, "matched_plan_steps": matched, "missing_plan_step_count": missing, "off_path_step_count": offPath, "retry_extra_count": retries, "error_count": errors, "deviation_count": deviations}}
}

func planQualityResult(trace TraceEvalContext, config map[string]any) ScorerResult {
	threshold, failure := scoreThreshold(config, "plan_quality", .8)
	if failure != nil {
		return *failure
	}
	steps := trace.Plan.Steps
	if !trace.Plan.Detected || len(steps) == 0 {
		return ScorerResult{Score: 0, Passed: false, Label: "no_plan", Explanation: "plan quality cannot be evaluated because no plan was detected", Metadata: map[string]any{"builtin": "plan_quality", "reason": "no_plan_detected", "threshold": threshold}}
	}
	countScore := planStepCountScore(len(steps))
	specific := 0
	for _, step := range steps {
		if len(meaningfulTokens(step.TextSafe)) >= 2 && !genericPlanStep(step.TextSafe) {
			specific++
		}
	}
	specificity := float64(specific) / float64(len(steps))
	structure := (countScore + specificity) / 2
	completeness := planCompletenessScore(steps)
	order := planOrderScore(steps)
	relevance := planRelevanceScore(trace)
	score := .25*structure + .30*completeness + .20*order + .25*relevance
	score = math.Max(0, math.Min(1, score))
	passed := score >= threshold
	label := planMetricLabel(score, passed, "good_plan", "needs_review", "poor_plan")
	return ScorerResult{Score: score, Passed: passed, Label: label, Explanation: fmt.Sprintf("plan quality score %.3f: structure=%.3f, completeness=%.3f, order=%.3f, relevance=%.3f", score, structure, completeness, order, relevance), Metadata: map[string]any{"builtin": "plan_quality", "threshold": threshold, "structure_score": structure, "step_count_score": countScore, "specificity_score": specificity, "completeness_score": completeness, "order_score": order, "relevance_score": relevance, "plan_steps": steps}}
}

func traceBinaryResult(name string, trace TraceEvalContext, config map[string]any, matched bool, passLabel, failLabel, explanation string, extra map[string]any) ScorerResult {
	threshold, failure := scoreThreshold(config, name, 1)
	if failure != nil {
		return *failure
	}
	score := 0.0
	label := failLabel
	if matched {
		score = 1
		label = passLabel
	}
	metadata := map[string]any{"builtin": name, "trace_eval_context_schema": trace.SchemaVersion, "threshold": threshold, "session_id": trace.SessionID, "root_run_id": trace.RootRunID, "evidence_refs": trace.EvidenceRefs}
	metadata = mergeAnyMaps(metadata, extra)
	return ScorerResult{Score: score, Passed: score >= threshold, Label: label, Explanation: explanation, Metadata: metadata}
}

func scoreThreshold(config map[string]any, name string, fallback float64) (float64, *ScorerResult) {
	threshold, ok := floatConfig(config, "score_threshold")
	if !ok {
		threshold, ok = floatConfig(config, "threshold")
	}
	if !ok {
		threshold = fallback
	}
	if threshold < 0 || threshold > 1 {
		result := scorerConfigError(name + " score_threshold must be between 0 and 1")
		return 0, &result
	}
	return threshold, nil
}
func traceToolSteps(trace TraceEvalContext) []TraceEvalExecutionStep {
	out := make([]TraceEvalExecutionStep, 0)
	for _, step := range trace.ExecutionSteps {
		if step.Kind == "tool_call" {
			out = append(out, step)
		}
	}
	return out
}
func traceToolNames(trace TraceEvalContext) []string {
	out := make([]string, 0)
	for _, step := range traceToolSteps(trace) {
		if strings.TrimSpace(step.ToolName) != "" {
			out = append(out, strings.TrimSpace(step.ToolName))
		}
	}
	return out
}
func safeToolAttributes(step TraceEvalExecutionStep) map[string]any {
	out := map[string]any{"duration_ms": step.DurationMS}
	for key, value := range map[string]string{"tool_name": step.ToolName, "name": step.Name, "status": step.Status, "summary_safe": step.SummarySafe, "arguments_ref": step.ArgumentsRef, "arguments_hash": step.ArgumentsHash, "result_ref": step.ResultRef, "result_hash": step.ResultHash, "input_hash": step.InputHash, "output_hash": step.OutputHash} {
		if strings.TrimSpace(value) != "" {
			out[key] = value
		}
	}
	return out
}
func traceDurationMS(trace TraceEvalContext) int64 {
	var first, last int64
	for _, step := range trace.ExecutionSteps {
		if step.StartedAt > 0 && (first == 0 || step.StartedAt < first) {
			first = step.StartedAt
		}
		if step.StartedAt > last {
			last = step.StartedAt
		}
		if step.EndedAt > last {
			last = step.EndedAt
		}
	}
	if first > 0 && last >= first {
		return last - first
	}
	var total int64
	for _, step := range trace.ExecutionSteps {
		if step.DurationMS > 0 {
			total += step.DurationMS
		}
	}
	return total
}
func traceErrorCount(trace TraceEvalContext) int64 {
	if trace.Features.ErrorCount > 0 {
		return trace.Features.ErrorCount
	}
	var count int64
	for _, step := range trace.ExecutionSteps {
		if step.Kind == "error" || step.ErrorCode != "" || strings.EqualFold(step.Status, "failed") {
			count++
		}
	}
	return count
}
func successfulProgressStep(step TraceEvalExecutionStep) bool {
	return step.Kind != "error" && step.Kind != "state" && !strings.EqualFold(step.Status, "failed") && step.ErrorCode == "" && (strings.EqualFold(step.Status, "completed") || strings.EqualFold(step.Status, "success") || step.OutputRef != "" || step.OutputHash != "")
}
func requiredConfigString(config map[string]any, key string) (string, bool) {
	value, ok := config[key].(string)
	value = strings.TrimSpace(value)
	return value, ok && value != ""
}
func stringConfigDefault(config map[string]any, key, fallback string) string {
	if value, ok := config[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}
func stringSliceConfig(config map[string]any, key string) ([]string, bool) {
	raw, ok := config[key]
	if !ok {
		return nil, false
	}
	switch values := raw.(type) {
	case []string:
		out := append([]string(nil), values...)
		return out, true
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			text, ok := value.(string)
			if !ok || strings.TrimSpace(text) == "" {
				return nil, false
			}
			out = append(out, strings.TrimSpace(text))
		}
		return out, true
	default:
		return nil, false
	}
}
func nonnegativeIntConfig(config map[string]any, key string) (int64, bool) {
	value, ok := config[key]
	if !ok {
		return 0, false
	}
	number, ok := scorerFloat(value)
	if !ok || number < 0 || math.Trunc(number) != number {
		return 0, false
	}
	return int64(number), true
}
func groupExtras(groups []TraceEvalStepGroup) int64 {
	var out int64
	for _, group := range groups {
		if len(group.StepIDs) > 1 {
			out += int64(len(group.StepIDs) - 1)
		}
	}
	return out
}
func retryExtras(groups []TraceEvalRetryGroup) int64 {
	var out int64
	for _, group := range groups {
		if len(group.StepIDs) > 1 {
			out += int64(len(group.StepIDs) - 1)
		}
	}
	return out
}
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func planMetricLabel(score float64, passed bool, pass, review, fail string) string {
	if passed {
		return pass
	}
	if score >= .5 {
		return review
	}
	return fail
}
func planStepCountScore(count int) float64 {
	switch {
	case count == 0:
		return 0
	case count == 1:
		return .7
	case count <= 6:
		return 1
	case count <= 10:
		return .8
	default:
		return .55
	}
}
func planCompletenessScore(steps []TraceEvalPlanStep) float64 {
	work, final := false, false
	for _, step := range steps {
		action := strings.ToLower(strings.TrimSpace(step.ExpectedAction))
		text := strings.ToLower(step.TextSafe)
		if action == "tool_call" || action == "read_result" || strings.Contains(text, "search") || strings.Contains(text, "lookup") || strings.Contains(text, "read") || strings.Contains(text, "fetch") {
			work = true
		}
		if action == "final_answer" || strings.Contains(text, "answer") || strings.Contains(text, "respond") || strings.Contains(text, "summar") {
			final = true
		}
	}
	score := 0.0
	if work {
		score += .45
	}
	if final {
		score += .45
	}
	if len(steps) >= 2 {
		score += .1
	} else if work || final {
		score += .05
	}
	return math.Min(1, score)
}
func planOrderScore(steps []TraceEvalPlanStep) float64 {
	score := 1.0
	seen := map[string]struct{}{}
	final, work := -1, -1
	for index, step := range steps {
		if step.Index != 0 && step.Index != int64(index+1) {
			score -= .2
		}
		key := strings.Join(meaningfulTokens(step.TextSafe), " ")
		if key != "" {
			if _, ok := seen[key]; ok {
				score -= .25
			}
			seen[key] = struct{}{}
		}
		action := strings.ToLower(strings.TrimSpace(step.ExpectedAction))
		if action == "final_answer" && final < 0 {
			final = index
		}
		if (action == "tool_call" || action == "read_result") && work < 0 {
			work = index
		}
	}
	if final >= 0 && work >= 0 && final < work {
		score -= .25
	}
	return math.Max(0, math.Min(1, score))
}
func planRelevanceScore(trace TraceEvalContext) float64 {
	if trace.Task == nil || strings.TrimSpace(trace.Task.TextSafe) == "" {
		return .75
	}
	task := tokenSet(meaningfulTokens(trace.Task.TextSafe))
	if len(task) == 0 {
		return .75
	}
	texts := make([]string, len(trace.Plan.Steps))
	for i, step := range trace.Plan.Steps {
		texts[i] = step.TextSafe
	}
	plan := tokenSet(meaningfulTokens(strings.Join(texts, " ")))
	overlap := 0
	for token := range task {
		if _, ok := plan[token]; ok {
			overlap++
		}
	}
	denom := len(task)
	if denom > 5 {
		denom = 5
	}
	score := float64(overlap) / float64(denom)
	if score == 0 && planCompletenessScore(trace.Plan.Steps) > 0 {
		score = .4
	}
	return math.Max(0, math.Min(1, score))
}
func meaningfulTokens(text string) []string {
	stop := map[string]struct{}{"about": {}, "after": {}, "again": {}, "also": {}, "and": {}, "answer": {}, "are": {}, "ask": {}, "for": {}, "from": {}, "into": {}, "the": {}, "then": {}, "this": {}, "that": {}, "task": {}, "their": {}, "them": {}, "they": {}, "with": {}, "will": {}, "you": {}, "your": {}}
	parts := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') })
	out := make([]string, 0)
	for _, part := range parts {
		if len(part) < 3 {
			continue
		}
		if _, ok := stop[part]; !ok {
			out = append(out, part)
		}
	}
	return out
}
func genericPlanStep(text string) bool {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "", "do it", "complete task", "solve task", "answer", "respond", "think", "plan":
		return true
	default:
		return false
	}
}
func tokenSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}
func firstNonEmptyString(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}
func firstPresentValue(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
func firstInt64(values ...any) int64 {
	for _, value := range values {
		if value == nil {
			continue
		}
		if number, ok := scorerFloat(value); ok {
			return int64(number)
		}
	}
	return 0
}
