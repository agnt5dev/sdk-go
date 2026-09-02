package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// TraceEvent is the cross-SDK journal event shape used by eval helpers.
type TraceEvent struct {
	EventType           string         `json:"event_type"`
	EventID             string         `json:"event_id,omitempty"`
	CorrelationID       string         `json:"correlation_id,omitempty"`
	ParentCorrelationID string         `json:"parent_correlation_id,omitempty"`
	TimestampNS         int64          `json:"timestamp_ns,omitempty"`
	Data                map[string]any `json:"data,omitempty"`
	Name                string         `json:"name,omitempty"`
}

// UnmarshalJSON accepts both runtime snake_case and SDK camelCase event fields.
func (e *TraceEvent) UnmarshalJSON(data []byte) error {
	type wireEvent TraceEvent
	var raw struct {
		wireEvent
		EventTypeCamel           string `json:"eventType"`
		EventIDCamel             string `json:"eventId"`
		CorrelationIDCamel       string `json:"correlationId"`
		ParentCorrelationIDCamel string `json:"parentCorrelationId"`
		TimestampNSCamel         int64  `json:"timestampNs"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*e = TraceEvent(raw.wireEvent)
	if e.EventType == "" {
		e.EventType = raw.EventTypeCamel
	}
	if e.EventID == "" {
		e.EventID = raw.EventIDCamel
	}
	if e.CorrelationID == "" {
		e.CorrelationID = raw.CorrelationIDCamel
	}
	if e.ParentCorrelationID == "" {
		e.ParentCorrelationID = raw.ParentCorrelationIDCamel
	}
	if e.TimestampNS == 0 {
		e.TimestampNS = raw.TimestampNSCamel
	}
	return nil
}

// EvalContext contains the input/output pair and trace evidence for custom eval logic.
type EvalContext struct {
	Input    any
	Output   any
	Expected any
	RunID    string
	TraceID  string
	Events   []TraceEvent
}

func (c EvalContext) EventsByType(eventType string) []TraceEvent {
	out := make([]TraceEvent, 0)
	for _, event := range c.Events {
		if event.EventType == eventType {
			out = append(out, event)
		}
	}
	return out
}

func (c EvalContext) LMCalls() []TraceEvent {
	return c.EventsByType("lm.completed")
}

func (c EvalContext) TotalTokens() int64 {
	var total int64
	for _, event := range c.LMCalls() {
		total += int64Value(event.Data["total_tokens"])
	}
	return total
}

func (c EvalContext) StepEvents(stepName string) []TraceEvent {
	out := make([]TraceEvent, 0)
	for _, event := range c.Events {
		if event.Name == stepName {
			out = append(out, event)
		}
	}
	return out
}

func (c EvalContext) ToolCalls() []ToolCall {
	return ExtractToolCallsFromEvents(c.Events)
}

func (c EvalContext) ToolCallNames() []string {
	return ToolCallNames(c.ToolCalls())
}

func (c EvalContext) ToolTrajectoryMatches(expected []string, mode ToolTrajectoryMode) bool {
	return ToolTrajectoryMatches(c.ToolCallNames(), expected, mode)
}

// BatchEvalItem is one input/expected pair in a local concurrent batch eval.
type BatchEvalItem struct {
	Input    map[string]any `json:"input"`
	Expected any            `json:"expected,omitempty"`
	ItemID   string         `json:"item_id,omitempty"`
	Index    *int           `json:"index,omitempty"`
}

// NormalizeBatchEvalItems pairs plain input maps with optional expected values.
func NormalizeBatchEvalItems(inputs []map[string]any, expected []any) []BatchEvalItem {
	out := make([]BatchEvalItem, len(inputs))
	for index, input := range inputs {
		itemIndex := index
		out[index] = BatchEvalItem{Input: cloneAnyMap(input), Index: &itemIndex}
		if index < len(expected) {
			out[index].Expected = expected[index]
		}
	}
	return out
}

// BatchEvalOptions controls batch evaluation behavior.
type BatchEvalOptions struct {
	Scorers        []EvalScorerSpec
	Expected       []any
	ComponentType  ComponentType
	DeploymentID   string
	MaxConcurrency int
	Timeout        time.Duration
}

// BatchEvalItemResult is the result of one batch item.
type BatchEvalItemResult struct {
	Index      int             `json:"index"`
	RunID      string          `json:"run_id"`
	Output     json.RawMessage `json:"output,omitempty"`
	Scores     []EvalScore     `json:"scores"`
	Passed     bool            `json:"passed"`
	DurationMS int64           `json:"duration_ms"`
	ItemID     string          `json:"item_id,omitempty"`
	TraceID    string          `json:"trace_id,omitempty"`
	Error      string          `json:"error,omitempty"`
}

func (r BatchEvalItemResult) IsSuccess() bool { return r.Error == "" }
func (r BatchEvalItemResult) IsFailed() bool  { return r.Error != "" }

func (r BatchEvalItemResult) GetScore(name string) (EvalScore, bool) {
	for _, score := range r.Scores {
		if score.Scorer == name {
			return score, true
		}
	}
	return EvalScore{}, false
}

// BatchEvalStats summarizes a batch evaluation.
type BatchEvalStats struct {
	TotalItems     int   `json:"total_items"`
	CompletedItems int   `json:"completed_items"`
	FailedItems    int   `json:"failed_items"`
	PassedItems    int   `json:"passed_items"`
	AvgDurationMS  int64 `json:"avg_duration_ms"`
	DurationMS     int64 `json:"duration_ms"`
}

// BatchEvalResult contains ordered item results and aggregate statistics.
type BatchEvalResult struct {
	BatchID string                `json:"batch_id"`
	Status  string                `json:"status"`
	Results []BatchEvalItemResult `json:"results"`
	Stats   BatchEvalStats        `json:"stats"`
}

func (r BatchEvalResult) PassRate() float64 {
	if r.Stats.TotalItems == 0 {
		return 0
	}
	return float64(r.Stats.PassedItems) / float64(r.Stats.TotalItems)
}

func (r BatchEvalResult) IsSuccess() bool        { return r.Status == "completed" }
func (r BatchEvalResult) IsPartialFailure() bool { return r.Status == "partial_failure" }

func (r BatchEvalResult) Outputs() []json.RawMessage {
	out := make([]json.RawMessage, len(r.Results))
	for i := range r.Results {
		out[i] = append(json.RawMessage(nil), r.Results[i].Output...)
	}
	return out
}

func (r BatchEvalResult) FailedItems() []BatchEvalItemResult {
	return filterBatchEvalResults(r.Results, func(item BatchEvalItemResult) bool { return item.IsFailed() })
}

func (r BatchEvalResult) FailingItems() []BatchEvalItemResult {
	return filterBatchEvalResults(r.Results, func(item BatchEvalItemResult) bool { return !item.Passed })
}

func (r BatchEvalResult) PassingItems() []BatchEvalItemResult {
	return filterBatchEvalResults(r.Results, func(item BatchEvalItemResult) bool { return item.Passed })
}

func filterBatchEvalResults(results []BatchEvalItemResult, keep func(BatchEvalItemResult) bool) []BatchEvalItemResult {
	out := make([]BatchEvalItemResult, 0)
	for _, result := range results {
		if keep(result) {
			out = append(out, result)
		}
	}
	return out
}

// BatchEval runs Client.Eval for each item with a bounded concurrency limit.
func (c *Client) BatchEval(ctx context.Context, component string, items []BatchEvalItem, options BatchEvalOptions, runOpts ...RunOption) *BatchEvalResult {
	started := time.Now()
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	if len(items) > 0 && maxConcurrency > len(items) {
		maxConcurrency = len(items)
	}
	results := make([]BatchEvalItemResult, len(items))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < maxConcurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				item := items[index]
				expected := item.Expected
				if expected == nil && index < len(options.Expected) {
					expected = options.Expected[index]
				}
				request := EvalRequest{
					Component:     component,
					ComponentType: options.ComponentType,
					Input:         item.Input,
					Expected:      expected,
					Scorers:       append([]EvalScorerSpec(nil), options.Scorers...),
				}
				itemOpts := append([]RunOption(nil), runOpts...)
				if options.Timeout > 0 {
					itemOpts = append(itemOpts, WithRunTimeout(options.Timeout))
				}
				if options.DeploymentID != "" {
					itemOpts = append(itemOpts, WithRunHeader("X-Deployment-ID", options.DeploymentID))
				}
				response, err := c.Eval(ctx, request, itemOpts...)
				resultIndex := index
				if item.Index != nil {
					resultIndex = *item.Index
				}
				if err != nil {
					results[index] = BatchEvalItemResult{Index: resultIndex, ItemID: item.ItemID, Error: err.Error()}
					continue
				}
				result := BatchEvalItemResult{
					Index:      resultIndex,
					RunID:      response.RunID,
					Output:     append(json.RawMessage(nil), response.Output...),
					Scores:     append([]EvalScore(nil), response.Scores...),
					Passed:     response.Passed,
					DurationMS: response.DurationMS,
					ItemID:     item.ItemID,
					TraceID:    response.TraceID,
				}
				if response.Error != nil {
					result.Error = response.Error.Message
				}
				results[index] = result
			}
		}()
	}
	for index := range items {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	sort.SliceStable(results, func(i, j int) bool { return results[i].Index < results[j].Index })
	status := "completed"
	failed := 0
	passed := 0
	var durationSum int64
	for _, result := range results {
		if result.IsFailed() {
			failed++
		} else {
			durationSum += result.DurationMS
		}
		if result.Passed && result.IsSuccess() {
			passed++
		}
	}
	if len(results) > 0 && failed == len(results) {
		status = "failed"
	} else if failed > 0 {
		status = "partial_failure"
	}
	average := int64(0)
	if completed := len(results) - failed; completed > 0 {
		average = durationSum / int64(completed)
	}
	return &BatchEvalResult{
		BatchID: fmt.Sprintf("batch_eval_%d", time.Now().UnixMilli()),
		Status:  status,
		Results: results,
		Stats: BatchEvalStats{
			TotalItems:     len(results),
			CompletedItems: len(results) - failed,
			FailedItems:    failed,
			PassedItems:    passed,
			AvgDurationMS:  average,
			DurationMS:     time.Since(started).Milliseconds(),
		},
	}
}

// AssertionResult is one trace assertion verdict.
type AssertionResult struct {
	Name        string `json:"name"`
	Passed      bool   `json:"passed"`
	Explanation string `json:"explanation"`
}

// TraceAssertion checks journal events.
type TraceAssertion struct {
	check func([]TraceEvent) AssertionResult
}

func (a TraceAssertion) Check(trace []TraceEvent) AssertionResult {
	if a.check == nil {
		return AssertionResult{Name: "invalid_assertion", Passed: false, Explanation: "Trace assertion has no check"}
	}
	return a.check(trace)
}

func MaxTokens(max int64) TraceAssertion {
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		var total int64
		for _, event := range trace {
			if event.EventType == "lm.completed" {
				total += int64Value(event.Data["total_tokens"])
			}
		}
		return AssertionResult{Name: fmt.Sprintf("max_tokens(%d)", max), Passed: total <= max, Explanation: fmt.Sprintf("Token usage: %d (max: %d)", total, max)}
	}}
}

func MaxLMCalls(max int) TraceAssertion {
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		count := 0
		for _, event := range trace {
			if event.EventType == "lm.completed" {
				count++
			}
		}
		return AssertionResult{Name: fmt.Sprintf("max_lm_calls(%d)", max), Passed: count <= max, Explanation: fmt.Sprintf("LLM calls: %d (max: %d)", count, max)}
	}}
}

func EventSequence(expected []string) TraceAssertion {
	expected = append([]string(nil), expected...)
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		index := 0
		for _, eventType := range expected {
			for index < len(trace) && trace[index].EventType != eventType {
				index++
			}
			if index >= len(trace) {
				return AssertionResult{Name: "event_sequence", Passed: false, Explanation: fmt.Sprintf("Missing event %q in sequence", eventType)}
			}
			index++
		}
		return AssertionResult{Name: "event_sequence", Passed: true, Explanation: "All events found in expected order"}
	}}
}

func StepMemoized(stepName string) TraceAssertion {
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		memoized := false
		for _, event := range trace {
			if event.EventType == "workflow.step.completed" && event.Name == stepName && stepEventMemoized(event) {
				memoized = true
				break
			}
		}
		explanation := fmt.Sprintf("Step %q was NOT memoized", stepName)
		if memoized {
			explanation = fmt.Sprintf("Step %q was memoized", stepName)
		}
		return AssertionResult{Name: fmt.Sprintf("step_memoized(%s)", stepName), Passed: memoized, Explanation: explanation}
	}}
}

// stepEventMemoized recognizes both the legacy is_memoized flag and the
// durable activation record's replay decision.
func stepEventMemoized(event TraceEvent) bool {
	if boolValue(event.Data["is_memoized"]) {
		return true
	}
	decision, _ := event.Data["decision"].(string)
	return decision == "replay"
}

func NoErrors() TraceAssertion {
	errors := map[string]struct{}{
		"run.failed": {}, "workflow.step.failed": {}, "agent.failed": {},
		"lm.failed": {}, "function.failed": {},
	}
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		count := 0
		for _, event := range trace {
			if _, ok := errors[event.EventType]; ok {
				count++
			}
		}
		explanation := "No error events found"
		if count > 0 {
			explanation = fmt.Sprintf("Found %d error event(s)", count)
		}
		return AssertionResult{Name: "no_errors", Passed: count == 0, Explanation: explanation}
	}}
}

func DurationUnder(max time.Duration) TraceAssertion {
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		if len(trace) == 0 {
			return AssertionResult{Name: fmt.Sprintf("duration_under(%dms)", max.Milliseconds()), Passed: true, Explanation: "Duration: 0ms (no events)"}
		}
		first, last := trace[0].TimestampNS, trace[0].TimestampNS
		for _, event := range trace[1:] {
			if event.TimestampNS < first {
				first = event.TimestampNS
			}
			if event.TimestampNS > last {
				last = event.TimestampNS
			}
		}
		duration := time.Duration(last - first)
		return AssertionResult{Name: fmt.Sprintf("duration_under(%dms)", max.Milliseconds()), Passed: duration <= max, Explanation: fmt.Sprintf("Duration: %dms (max: %dms)", duration.Milliseconds(), max.Milliseconds())}
	}}
}

func EventCount(eventType string, min int) TraceAssertion {
	return TraceAssertion{check: func(trace []TraceEvent) AssertionResult {
		count := 0
		for _, event := range trace {
			if event.EventType == eventType {
				count++
			}
		}
		return AssertionResult{Name: fmt.Sprintf("event_count(%s, min=%d)", eventType, min), Passed: count >= min, Explanation: fmt.Sprintf("Event %q occurred %d times (min: %d)", eventType, count, min)}
	}}
}

// TraceScorer applies multiple assertions and returns their aggregate score.
func TraceScorer(trace []TraceEvent, assertions []TraceAssertion) ScorerResult {
	if len(assertions) == 0 {
		return ScorerResult{Score: 1, Passed: true, Label: "pass", Explanation: "No assertions to check"}
	}
	failed := make([]AssertionResult, 0)
	passed := 0
	for _, assertion := range assertions {
		result := assertion.Check(trace)
		if result.Passed {
			passed++
		} else {
			failed = append(failed, result)
		}
	}
	allPassed := len(failed) == 0
	explanation := "All assertions passed"
	if !allPassed {
		lines := make([]string, len(failed))
		for i, result := range failed {
			lines[i] = fmt.Sprintf("- %s: %s", result.Name, result.Explanation)
		}
		explanation = "Failed assertions:\n" + strings.Join(lines, "\n")
	}
	label := "fail"
	if allPassed {
		label = "pass"
	}
	return ScorerResult{Score: float64(passed) / float64(len(assertions)), Passed: allPassed, Label: label, Explanation: explanation}
}

type TraceScorerResult = ScorerResult

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		out, _ := typed.Int64()
		return out
	default:
		return 0
	}
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}
