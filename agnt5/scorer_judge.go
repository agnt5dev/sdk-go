package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const llmJudgeDefaultSystemPrompt = `You are an expert evaluator. Your task is to evaluate the given output based on the provided criteria.

Respond with a JSON object containing:
- "score": a number between 0.0 and 1.0
- "passed": boolean (true if score >= 0.7)
- "explanation": brief explanation of your evaluation

Respond ONLY with the JSON object, no other text.`

const (
	correctnessJudgeCriteria  = "Evaluate whether the output correctly answers the input and matches the expected output. Score 1.0 for fully correct answers, 0.5 for partially correct answers, and 0.0 for incorrect or unsupported answers."
	faithfulnessJudgeCriteria = "Evaluate whether the output is faithful to the provided context. Penalize claims that are unsupported, contradicted by context, or omit critical context needed for the answer."
	goalSuccessJudgeCriteria  = "Evaluate whether the overall session achieved the user's goal. Use available trace-eval context, journal events, session state, input, output, and expected result when provided. Penalize incomplete task completion, missing required actions, tool failures that affected the outcome, and unsupported success claims."
	agentJudgeDefaultCriteria = "Investigate the provided evidence before scoring. Check factual correctness, grounding in the trace and tool evidence, appropriate tool usage, and whether the final output is supported by the observed execution. Penalize unsupported claims, missing evidence, tool misuse, and reasoning that conflicts with the trace."
	agentJudgeSystemPrompt    = "You are an AGNT5 agent-as-a-judge evaluator. Inspect the structured evidence, trace-eval context, tool-call trajectory, peer scores, and task input before returning a verdict. Do not assume facts that are not present in the provided evidence. If evidence is missing or inconclusive, lower the score and explain the gap. Return only the requested JSON verdict."
)

type judgeModelContextKey struct{}

// WithLLMJudgeModel injects a deterministic or custom model for built-in judge scorers.
func WithLLMJudgeModel(ctx context.Context, model LanguageModel) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, judgeModelContextKey{}, model)
}

func runJudgeBuiltIn(ctx context.Context, name string, request ScorerRequest) ScorerResult {
	switch name {
	case "correctness":
		return runCorrectnessJudge(ctx, request)
	case "faithfulness":
		return runFaithfulnessJudge(ctx, request)
	case "goal_success":
		return runGoalSuccessJudge(ctx, request)
	case "agent_judge":
		return runAgentJudge(ctx, request)
	default:
		return runLLMJudge(ctx, request)
	}
}

func runLLMJudge(ctx context.Context, request ScorerRequest) ScorerResult {
	criteria := stringConfigDefault(request.Config, "criteria", "")
	promptTemplate := stringConfigDefault(request.Config, "prompt_template", "")
	if criteria == "" && promptTemplate == "" {
		return scorerConfigError("llm_judge requires `config.criteria` or `config.prompt_template`")
	}
	provider := strings.ToLower(stringConfigDefault(request.Config, "provider", "openai"))
	modelName := stringConfigDefault(request.Config, "model", "")
	if modelName == "" {
		return scorerConfigError("llm_judge requires `config.model`")
	}
	temperature, ok := floatConfig(request.Config, "temperature")
	if !ok {
		temperature = 0
	}
	choiceScores, failure := parseJudgeChoiceScores(request.Config["choice_scores"])
	if failure != nil {
		return *failure
	}
	userPrompt, failure := buildJudgePrompt(request, criteria, promptTemplate, choiceScores)
	if failure != nil {
		return *failure
	}
	model, failure := judgeLanguageModel(ctx, provider, modelName)
	if failure != nil {
		return *failure
	}
	systemPrompt := stringConfigDefault(request.Config, "system_prompt", llmJudgeDefaultSystemPrompt)
	generateRequest := GenerateRequest{
		Model: modelName,
		Messages: []Message{
			{Role: MessageRoleSystem, Content: systemPrompt},
			{Role: MessageRoleUser, Content: userPrompt},
		},
		Temperature: &temperature,
	}
	var response GenerateResponse
	var err error
	if runContext, ok := ctx.(*Context); ok {
		response, err = runContext.Generate(model, generateRequest)
	} else {
		response, err = model.Generate(ctx, generateRequest)
	}
	if err != nil {
		return ScorerResult{Score: 0, Passed: false, Label: "error", Explanation: "LLM call failed: " + err.Error()}
	}
	result := parseLLMJudgeResponse(response.Content)
	return applyJudgeChoiceScores(result, choiceScores)
}

func buildJudgePrompt(request ScorerRequest, criteria, promptTemplate string, choiceScores map[string]float64) (string, *ScorerResult) {
	var builder strings.Builder
	if promptTemplate != "" {
		rendered, err := renderJudgePromptTemplate(promptTemplate, map[string]any{
			"input": request.Input, "output": request.Output, "expected": request.Expected,
			"context":  firstPresentValue(request.Config["context_data"], request.Config["context"]),
			"metadata": request.Config["metadata"], "tags": request.Config["tags"],
		})
		if err != nil {
			result := scorerConfigError("prompt_template variable not found: " + err.Error())
			return "", &result
		}
		builder.WriteString(strings.TrimRight(rendered, " \n\t"))
		builder.WriteString("\n\n")
		if !judgeTemplateReferences(promptTemplate, "output") {
			builder.WriteString("## Output to Evaluate\n" + formatJudgeValue(request.Output) + "\n\n")
		}
	} else {
		builder.WriteString("## Evaluation Criteria\n" + criteria + "\n\n")
		if boolConfigDefault(request.Config, "include_input", false) && request.Input != nil {
			builder.WriteString("## Input\n" + formatJudgeValue(request.Input) + "\n\n")
		}
		contextData := firstPresentValue(request.Config["context_data"], request.Config["context"])
		if contextData != nil {
			builder.WriteString("## Context\n" + formatJudgeValue(contextData) + "\n\n")
		}
		builder.WriteString("## Output to Evaluate\n" + formatJudgeValue(request.Output) + "\n\n")
		if request.Expected != nil {
			builder.WriteString("## Expected Output (Reference)\n" + formatJudgeValue(request.Expected) + "\n\n")
		}
	}
	if len(choiceScores) > 0 {
		labels := sortedChoiceLabels(choiceScores)
		builder.WriteString("Choose exactly one label from: " + strings.Join(labels, ", ") + ". Return that label in the JSON `label` field. The platform will map labels to scores.\n\n")
	}
	if boolConfigDefault(request.Config, "use_cot", false) {
		builder.WriteString("Reason through the rubric before deciding, but do not include hidden chain-of-thought. Put only a concise rationale in the JSON `explanation` field.\n\n")
	}
	if schema, ok := request.Config["output_schema"].(map[string]any); ok {
		builder.WriteString("Return a JSON object matching this requested output shape:\n" + formatJudgeValue(schema) + "\nFor experiment scoring, the JSON should include `score` (0.0 to 1.0), `label`, and `explanation` fields.\n\n")
	}
	builder.WriteString("Please evaluate the output and respond with a JSON object.")
	return builder.String(), nil
}

func judgeLanguageModel(ctx context.Context, provider, modelName string) (LanguageModel, *ScorerResult) {
	if ctx != nil {
		if model, ok := ctx.Value(judgeModelContextKey{}).(LanguageModel); ok && model != nil {
			return model, nil
		}
	}
	missingKey := func(variable string) (LanguageModel, *ScorerResult) {
		result := scorerConfigError(fmt.Sprintf("llm_judge: %s is required for provider %q", variable, provider))
		return nil, &result
	}
	openAICompatible := func(baseURL, envKey string) (LanguageModel, *ScorerResult) {
		key := os.Getenv(envKey)
		if key == "" && provider != "ollama" {
			return missingKey(envKey)
		}
		return NewOpenAIModel(OpenAIConfig{BaseURL: baseURL, APIKey: key, Model: modelName}), nil
	}
	switch provider {
	case "openai":
		return openAICompatible("https://api.openai.com", "OPENAI_API_KEY")
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			return missingKey("ANTHROPIC_API_KEY")
		}
		return NewAnthropicModel(AnthropicConfig{APIKey: key, Model: modelName}), nil
	case "google", "gemini":
		key := os.Getenv("GOOGLE_API_KEY")
		if key == "" {
			key = os.Getenv("GEMINI_API_KEY")
		}
		if key == "" {
			return missingKey("GOOGLE_API_KEY or GEMINI_API_KEY")
		}
		return NewGoogleModel(GoogleConfig{APIKey: key, Model: modelName}), nil
	case "mistral":
		return openAICompatible("https://api.mistral.ai", "MISTRAL_API_KEY")
	case "baseten":
		key := os.Getenv("BASETEN_API_KEY")
		if key == "" {
			return missingKey("BASETEN_API_KEY")
		}
		baseURL := os.Getenv("BASETEN_BASE_URL")
		if baseURL == "" {
			baseURL = "https://inference.baseten.co/v1"
		}
		authScheme := os.Getenv("BASETEN_AUTH_SCHEME")
		if authScheme == "" {
			authScheme = "Api-Key"
		}
		path := ""
		if strings.HasSuffix(strings.TrimRight(baseURL, "/"), "/v1") {
			path = "/chat/completions"
		}
		return NewOpenAIModel(OpenAIConfig{BaseURL: baseURL, APIKey: key, AuthScheme: authScheme, Model: modelName, Path: path}), nil
	case "fireworks":
		return openAICompatible("https://api.fireworks.ai/inference", "FIREWORKS_API_KEY")
	case "groq":
		return openAICompatible("https://api.groq.com/openai", "GROQ_API_KEY")
	case "deepseek":
		return openAICompatible("https://api.deepseek.com", "DEEPSEEK_API_KEY")
	case "openrouter":
		return openAICompatible("https://openrouter.ai/api", "OPENROUTER_API_KEY")
	case "lepton":
		key := os.Getenv("LEPTON_API_KEY")
		if key == "" {
			key = os.Getenv("LEPTON_API_TOKEN")
		}
		if key == "" {
			return missingKey("LEPTON_API_KEY or LEPTON_API_TOKEN")
		}
		baseURL := os.Getenv("LEPTON_BASE_URL")
		if baseURL == "" {
			baseURL = os.Getenv("LEPTON_API_BASE")
		}
		if baseURL == "" {
			result := scorerConfigError("llm_judge: LEPTON_BASE_URL is required for provider \"lepton\"")
			return nil, &result
		}
		path := ""
		if strings.HasSuffix(strings.TrimRight(baseURL, "/"), "/v1") {
			path = "/chat/completions"
		}
		return NewOpenAIModel(OpenAIConfig{BaseURL: baseURL, APIKey: key, Model: modelName, Path: path}), nil
	case "together":
		return openAICompatible("https://api.together.xyz", "TOGETHER_API_KEY")
	case "moonshot":
		return openAICompatible("https://api.moonshot.ai", "MOONSHOT_API_KEY")
	case "ollama":
		return openAICompatible("http://localhost:11434", "")
	default:
		result := scorerConfigError(fmt.Sprintf("llm_judge: unsupported provider %q", provider))
		return nil, &result
	}
}

func parseLLMJudgeResponse(content string) ScorerResult {
	jsonText := extractJudgeJSON(content)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(jsonText), &parsed); err != nil {
		return ScorerResult{Score: 0, Passed: false, Label: "parse_error", Explanation: "Could not parse LLM response: " + content, Metadata: map[string]any{"raw_response": content, "error": err.Error()}}
	}
	score, _ := scorerFloat(parsed["score"])
	score = maxFloat(0, minFloat(1, score))
	passed, hasPassed := parsed["passed"].(bool)
	if !hasPassed {
		passed = score >= .7
	}
	result := ScorerResult{Score: score, Passed: passed}
	result.Explanation, _ = parsed["explanation"].(string)
	result.Label, _ = parsed["label"].(string)
	extras := make(map[string]any)
	for key, value := range parsed {
		if key != "score" && key != "passed" && key != "explanation" && key != "label" {
			extras[key] = value
		}
	}
	if len(extras) > 0 {
		result.Metadata = extras
	}
	return result
}

func extractJudgeJSON(content string) string {
	trimmed := strings.TrimSpace(content)
	if match := regexp.MustCompile("(?s)```(?:json)?\\s*(.*?)\\s*```").FindStringSubmatch(trimmed); len(match) > 1 {
		return strings.TrimSpace(match[1])
	}
	start, end := strings.Index(trimmed, "{"), strings.LastIndex(trimmed, "}")
	if start >= 0 && end >= start {
		return trimmed[start : end+1]
	}
	return trimmed
}

func parseJudgeChoiceScores(raw any) (map[string]float64, *ScorerResult) {
	if raw == nil {
		return nil, nil
	}
	var values map[string]any
	switch typed := raw.(type) {
	case map[string]any:
		values = typed
	case map[string]float64:
		values = make(map[string]any, len(typed))
		for key, value := range typed {
			values[key] = value
		}
	default:
		result := scorerConfigError("llm_judge `config.choice_scores` must be an object mapping label to score")
		return nil, &result
	}
	if len(values) == 0 {
		result := scorerConfigError("llm_judge `config.choice_scores` must include at least one label")
		return nil, &result
	}
	out := make(map[string]float64, len(values))
	for label, rawScore := range values {
		if strings.TrimSpace(label) == "" {
			result := scorerConfigError("llm_judge `config.choice_scores` labels must be non-empty")
			return nil, &result
		}
		score, ok := scorerFloat(rawScore)
		if !ok || score < 0 || score > 1 {
			result := scorerConfigError(fmt.Sprintf("llm_judge choice score for label %q must be between 0 and 1", label))
			return nil, &result
		}
		out[label] = score
	}
	return out, nil
}

func applyJudgeChoiceScores(result ScorerResult, choiceScores map[string]float64) ScorerResult {
	if len(choiceScores) == 0 || result.Label == "parse_error" || result.Label == "config_error" {
		return result
	}
	label := result.Label
	if label == "" {
		matches := make([]string, 0)
		for candidate, score := range choiceScores {
			if absFloat(score-result.Score) < 1e-9 {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 1 {
			label = matches[0]
		}
	}
	score, ok := choiceScores[label]
	if !ok {
		labels := sortedChoiceLabels(choiceScores)
		return ScorerResult{Score: 0, Passed: false, Label: "invalid_label", Explanation: fmt.Sprintf("Judge returned label %q; expected one of: %s", result.Label, strings.Join(labels, ", ")), Metadata: mergeAnyMaps(result.Metadata, map[string]any{"allowed_labels": labels, "invalid_label": result.Label})}
	}
	score = maxFloat(0, minFloat(1, score))
	return ScorerResult{Score: score, Passed: score >= .7, Label: label, Explanation: result.Explanation, Metadata: mergeAnyMaps(result.Metadata, map[string]any{"choice_scores": choiceScores, "selected_label": label})}
}

func runCorrectnessJudge(ctx context.Context, request ScorerRequest) ScorerResult {
	config := cloneAnyMap(request.Config)
	output, err := optionalJudgeSelector(request, config, "answer_field", request.Output)
	if err != nil {
		return scorerConfigError("correctness field selector not found: " + err.Error())
	}
	expected, err := optionalJudgeSelector(request, config, "reference_field", request.Expected)
	if err != nil {
		return scorerConfigError("correctness field selector not found: " + err.Error())
	}
	request.Output, request.Expected = output, expected
	request.Config = judgePresetConfig(config, correctnessJudgeCriteria, true)
	result := runLLMJudge(ctx, request)
	result.Metadata = mergeAnyMaps(result.Metadata, map[string]any{"judge_preset": "correctness"})
	return result
}
func runFaithfulnessJudge(ctx context.Context, request ScorerRequest) ScorerResult {
	config := cloneAnyMap(request.Config)
	fields := configStringList(config, "context_field", "context_fields")
	if len(fields) == 0 {
		return scorerConfigError("faithfulness requires config.context_fields or config.context_field")
	}
	contextData := make(map[string]any, len(fields))
	for _, field := range fields {
		value, err := judgeSelectedValue(request, field)
		if err != nil {
			return scorerConfigError("faithfulness field selector not found: " + err.Error())
		}
		contextData[field] = value
	}
	output, err := optionalJudgeSelector(request, config, "answer_field", request.Output)
	if err != nil {
		return scorerConfigError("faithfulness field selector not found: " + err.Error())
	}
	request.Output = output
	request.Config = judgePresetConfig(config, faithfulnessJudgeCriteria, false)
	request.Config["context_data"] = contextData
	result := runLLMJudge(ctx, request)
	result.Metadata = mergeAnyMaps(result.Metadata, map[string]any{"judge_preset": "faithfulness", "context_fields": fields})
	return result
}
func runGoalSuccessJudge(ctx context.Context, request ScorerRequest) ScorerResult {
	config := cloneAnyMap(request.Config)
	evidence := map[string]any{}
	if value := firstPresentValue(config["context_data"], config["context"]); value != nil {
		evidence["provided_context"] = value
	}
	traceEvalContext := firstPresentValue(request.TraceEvalContext, config["trace_eval_context"])
	if traceEvalContext != nil {
		evidence["trace_eval_context"] = traceEvalContext
	}
	if boolConfigDefault(config, "include_trace", true) && len(request.Trace) > 0 {
		evidence["trace"] = request.Trace
	}
	if len(request.PeerScores) > 0 {
		evidence["peer_scores"] = request.PeerScores
	}
	for _, key := range []string{"session_fields", "journal_event_fields"} {
		if values, ok := stringSliceConfig(config, key); ok && len(values) > 0 {
			evidence[key] = values
		}
	}
	sources := sortedMapKeys(evidence)
	request.Config = judgePresetConfig(config, goalSuccessJudgeCriteria, true)
	if len(sources) > 0 {
		request.Config["context_data"] = map[string]any{"goal_success_evidence": evidence}
	}
	result := runLLMJudge(ctx, request)
	result.Metadata = mergeAnyMaps(result.Metadata, map[string]any{"judge_preset": "goal_success", "evidence_sources": sources})
	return result
}
func runAgentJudge(ctx context.Context, request ScorerRequest) ScorerResult {
	config := cloneAnyMap(request.Config)
	evidence := map[string]any{}
	if value := firstPresentValue(config["context_data"], config["context"]); value != nil {
		evidence["provided_context"] = value
	}
	traceEvalContext := firstPresentValue(request.TraceEvalContext, config["trace_eval_context"])
	if boolConfigDefault(config, "include_trace_eval_context", true) && traceEvalContext != nil {
		evidence["trace_eval_context"] = traceEvalContext
	}
	if boolConfigDefault(config, "include_trace", false) && len(request.Trace) > 0 {
		evidence["trace"] = request.Trace
	}
	if boolConfigDefault(config, "include_tool_calls", true) {
		calls := ExtractToolCallsFromEvents(request.Trace)
		if len(calls) > 0 {
			evidence["tool_calls"] = calls
		}
	}
	if boolConfigDefault(config, "include_peer_scores", true) && len(request.PeerScores) > 0 {
		evidence["peer_scores"] = request.PeerScores
	}
	if allowedTools := configStringList(config, "allowed_tools", "tools"); len(allowedTools) > 0 {
		evidence["allowed_tools"] = allowedTools
	}
	maxEvidenceChars := 20_000
	if value, ok := nonnegativeIntConfig(config, "max_evidence_chars"); ok {
		maxEvidenceChars = int(value)
	}
	if maxEvidenceChars < 1_000 {
		maxEvidenceChars = 1_000
	}
	if maxEvidenceChars > 200_000 {
		maxEvidenceChars = 200_000
	}
	sources := sortedMapKeys(evidence)
	criteria := stringConfigDefault(config, "criteria", agentJudgeDefaultCriteria)
	request.Config = judgePresetConfig(config, criteria, true)
	request.Config["system_prompt"] = stringConfigDefault(config, "system_prompt", agentJudgeSystemPrompt)
	request.Config["context_data"] = map[string]any{"agent_judge_evidence": truncateJudgeEvidence(evidence, maxEvidenceChars)}
	result := runLLMJudge(ctx, request)
	result.Metadata = mergeAnyMaps(result.Metadata, map[string]any{"judge_preset": "agent_judge", "judge_mode": "evidence_inspection", "agent_judge_version": "evidence_inspection_v1", "evidence_sources": sources})
	return result
}

func judgePresetConfig(config map[string]any, criteria string, includeInput bool) map[string]any {
	out := map[string]any{"provider": stringConfigDefault(config, "provider", "openai"), "model": stringConfigDefault(config, "model", "gpt-4o-mini"), "criteria": criteria, "include_input": boolConfigDefault(config, "include_input", includeInput), "temperature": 0.0}
	if value, ok := floatConfig(config, "temperature"); ok {
		out["temperature"] = value
	}
	return out
}
func optionalJudgeSelector(request ScorerRequest, config map[string]any, key string, fallback any) (any, error) {
	selector, ok := config[key].(string)
	if !ok || strings.TrimSpace(selector) == "" {
		return fallback, nil
	}
	return judgeSelectedValue(request, strings.TrimSpace(selector))
}
func judgeSelectedValue(request ScorerRequest, selector string) (any, error) {
	roots := map[string]any{"input": request.Input, "output": request.Output, "expected": request.Expected, "trace_eval_context": request.TraceEvalContext, "metadata": request.Metadata}
	parts := strings.Split(selector, ".")
	value, ok := roots[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%s", selector)
	}
	for _, part := range parts[1:] {
		switch typed := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = typed[part]
			if !ok {
				return nil, fmt.Errorf("%s", selector)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("%s", selector)
			}
			value = typed[index]
		default:
			return nil, fmt.Errorf("%s", selector)
		}
	}
	return value, nil
}
func configStringList(config map[string]any, keys ...string) []string {
	out := make([]string, 0)
	for _, key := range keys {
		if value, ok := config[key].(string); ok && strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
		if values, ok := stringSliceConfig(config, key); ok {
			out = append(out, values...)
		}
	}
	return out
}
func renderJudgePromptTemplate(template string, values map[string]any) (string, error) {
	pattern := regexp.MustCompile(`{{\s*([^{}]+?)\s*}}`)
	var renderErr error
	out := pattern.ReplaceAllStringFunc(template, func(match string) string {
		selector := strings.TrimSpace(pattern.FindStringSubmatch(match)[1])
		value, err := selectTemplateValue(values, selector)
		if err != nil {
			renderErr = err
			return ""
		}
		return formatJudgeValue(value)
	})
	return out, renderErr
}
func selectTemplateValue(values map[string]any, selector string) (any, error) {
	parts := strings.Split(selector, ".")
	value, ok := values[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%s", selector)
	}
	for _, part := range parts[1:] {
		switch typed := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = typed[part]
			if !ok {
				return nil, fmt.Errorf("%s", selector)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("%s", selector)
			}
			value = typed[index]
		default:
			return nil, fmt.Errorf("%s", selector)
		}
	}
	return value, nil
}
func judgeTemplateReferences(template, root string) bool {
	pattern := regexp.MustCompile(`{{\s*([^{}]+?)\s*}}`)
	for _, match := range pattern.FindAllStringSubmatch(template, -1) {
		selector := strings.TrimSpace(match[1])
		if selector == root || strings.HasPrefix(selector, root+".") {
			return true
		}
	}
	return false
}
func formatJudgeValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		return string(encoded)
	}
	return fmt.Sprint(value)
}
func sortedChoiceLabels(values map[string]float64) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
func sortedMapKeys(values map[string]any) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func truncateJudgeEvidence(evidence map[string]any, maxChars int) map[string]any {
	encoded, err := json.Marshal(evidence)
	if err != nil || len(encoded) <= maxChars {
		return evidence
	}
	return map[string]any{"truncated": true, "max_chars": maxChars, "evidence_excerpt": string(encoded[:maxChars])}
}
