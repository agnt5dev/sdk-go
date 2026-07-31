package agnt5

import (
	"context"
	"encoding/json"
	"net/http"
)

// WorkflowProxy is a fluent client wrapper for one workflow component.
type WorkflowProxy struct {
	client *Client
	name   string
}

// Workflow returns a proxy for invoking one workflow.
func (c *Client) Workflow(name string) *WorkflowProxy {
	return &WorkflowProxy{client: c, name: name}
}

func (p *WorkflowProxy) Run(ctx context.Context, input any, opts ...RunOption) (*RunResponse, error) {
	opts = append([]RunOption{WithRunComponentType(ComponentTypeWorkflow)}, opts...)
	return p.client.Run(ctx, p.name, input, opts...)
}

func (p *WorkflowProxy) Submit(ctx context.Context, input any, opts ...SubmitOption) (*SubmitResponse, error) {
	opts = append([]SubmitOption{WithSubmitComponentType(ComponentTypeWorkflow)}, opts...)
	return p.client.Submit(ctx, p.name, input, opts...)
}

// SessionProxy adds a default session ID to client calls.
type SessionProxy struct {
	client    *Client
	sessionID string
	userID    string
}

// Session returns a proxy that sends X-Session-ID.
func (c *Client) Session(sessionID string) *SessionProxy {
	return &SessionProxy{client: c, sessionID: sessionID}
}

// WithUser returns a copy of the proxy that also sends X-User-ID.
func (p *SessionProxy) WithUser(userID string) *SessionProxy {
	return &SessionProxy{client: p.client, sessionID: p.sessionID, userID: userID}
}

func (p *SessionProxy) Run(ctx context.Context, component string, input any, opts ...RunOption) (*RunResponse, error) {
	prefix := []RunOption{WithRunSessionID(p.sessionID)}
	if p.userID != "" {
		prefix = append(prefix, WithRunUserID(p.userID))
	}
	opts = append(prefix, opts...)
	return p.client.Run(ctx, component, input, opts...)
}

func (p *SessionProxy) Workflow(name string) *SessionWorkflowProxy {
	return &SessionWorkflowProxy{session: p, name: name}
}

// SessionWorkflowProxy combines workflow and session defaults.
type SessionWorkflowProxy struct {
	session *SessionProxy
	name    string
}

func (p *SessionWorkflowProxy) Run(ctx context.Context, input any, opts ...RunOption) (*RunResponse, error) {
	opts = append([]RunOption{WithRunComponentType(ComponentTypeWorkflow)}, opts...)
	return p.session.Run(ctx, p.name, input, opts...)
}

// EvalScorerSpec is one scorer requested for a gateway eval.
type EvalScorerSpec struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config,omitempty"`
}

// EvalRequest is the gateway eval request shape.
type EvalRequest struct {
	Component     string           `json:"component"`
	ComponentType ComponentType    `json:"component_type,omitempty"`
	Input         any              `json:"input"`
	Expected      any              `json:"expected,omitempty"`
	Scorers       []EvalScorerSpec `json:"scorers,omitempty"`
	Metadata      map[string]any   `json:"metadata,omitempty"`
}

// EvalScore is one scorer result returned by the gateway.
type EvalScore struct {
	Scorer      string         `json:"scorer"`
	Score       float64        `json:"score"`
	Passed      bool           `json:"passed"`
	Explanation string         `json:"explanation,omitempty"`
	Label       string         `json:"label,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type ScorerResultSummary = EvalScore

// EvalError describes a component or scorer failure embedded in an eval response.
type EvalError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// EvalResponse is returned by Client.Eval.
type EvalResponse struct {
	Output     json.RawMessage `json:"output,omitempty"`
	Scores     []EvalScore     `json:"scores,omitempty"`
	Passed     bool            `json:"passed"`
	RunID      string          `json:"run_id,omitempty"`
	TraceID    string          `json:"trace_id,omitempty"`
	DurationMS int64           `json:"duration_ms,omitempty"`
	Error      *EvalError      `json:"error,omitempty"`
	Raw        map[string]any  `json:"-"`
}

func (r *EvalResponse) IsSuccess() bool { return r != nil && r.Error == nil }
func (r *EvalResponse) IsError() bool   { return r != nil && r.Error != nil }

func (r *EvalResponse) GetScore(name string) (EvalScore, bool) {
	if r == nil {
		return EvalScore{}, false
	}
	for _, score := range r.Scores {
		if score.Scorer == name {
			return score, true
		}
	}
	return EvalScore{}, false
}

// DecodeOutput unmarshals the component output into target.
func (r *EvalResponse) DecodeOutput(target any) error {
	if r == nil || len(r.Output) == 0 {
		return nil
	}
	return json.Unmarshal(r.Output, target)
}

// RaiseForStatus returns a typed error when the eval response embeds a failure.
func (r *EvalResponse) RaiseForStatus() error {
	if r == nil || r.Error == nil {
		return nil
	}
	return &ScorerError{Code: r.Error.Code, Message: r.Error.Message, Details: cloneAnyMap(r.Error.Details)}
}

// ResumeWorkflowResponse is returned after asking the gateway to resume a paused workflow.
type ResumeWorkflowResponse struct {
	RunID  string         `json:"run_id"`
	Status string         `json:"status"`
	Offset int64          `json:"offset,omitempty"`
	Raw    map[string]any `json:"-"`
}

// CancelRunResponse is returned after asking the gateway to cancel a run.
type CancelRunResponse struct {
	RunID  string         `json:"run_id"`
	Status string         `json:"status"`
	Offset int64          `json:"offset,omitempty"`
	Raw    map[string]any `json:"-"`
}

// Eval evaluates a component output using gateway scorers.
func (c *Client) Eval(ctx context.Context, request EvalRequest, opts ...RunOption) (*EvalResponse, error) {
	if request.ComponentType == "" {
		request.ComponentType = ComponentTypeFunction
	}
	if request.Expected != nil && len(request.Scorers) == 0 {
		request.Scorers = []EvalScorerSpec{{Name: "exact_match"}}
	}
	config := newRunConfig(opts...)
	headers := c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers)
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{"v1", "eval"}, request, headers, config.timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(decoded["output"])
	var scores []EvalScore
	scoreValue := decoded["scores"]
	if scoreValue == nil {
		scoreValue = decoded["scorer_results"]
	}
	scoreRaw, _ := json.Marshal(scoreValue)
	_ = json.Unmarshal(scoreRaw, &scores)
	for index := range scores {
		if rawScores, ok := scoreValue.([]any); ok && index < len(rawScores) {
			if rawScore, ok := rawScores[index].(map[string]any); ok {
				if scores[index].Scorer == "" {
					scores[index].Scorer = firstString(rawScore, "name")
				}
				if _, hasPassed := rawScore["passed"]; !hasPassed {
					scores[index].Passed = scores[index].Score >= 0.5
				}
			}
		}
	}
	var evalError *EvalError
	if rawError, ok := decoded["error"]; ok && rawError != nil {
		switch typed := rawError.(type) {
		case string:
			evalError = &EvalError{Code: "EVAL_FAILED", Message: typed}
		case map[string]any:
			evalError = &EvalError{Code: firstString(typed, "code"), Message: firstString(typed, "message")}
			if evalError.Code == "" {
				evalError.Code = "EVAL_FAILED"
			}
			if details, ok := typed["details"].(map[string]any); ok {
				evalError.Details = cloneAnyMap(details)
			}
		}
	}
	passed, hasPassed := boolFieldPresent(decoded, "passed")
	if !hasPassed {
		passed = evalError == nil
		for _, score := range scores {
			if !score.Passed {
				passed = false
				break
			}
		}
	}
	return &EvalResponse{
		Output:     raw,
		Scores:     scores,
		Passed:     passed,
		RunID:      firstString(decoded, "run_id", "runId"),
		TraceID:    firstString(decoded, "trace_id", "traceId"),
		DurationMS: fieldInt64(decoded, "duration_ms", "durationMs"),
		Error:      evalError,
		Raw:        decoded,
	}, nil
}

// Chat sends a chat message to an agent chat endpoint.
func (c *Client) Chat(ctx context.Context, agent string, message ChatMessage, opts ...RunOption) (*ChatResponse, error) {
	config := newRunConfig(opts...)
	headers := c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers)
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{"v1", "agents", agent, "chat"}, message, headers, config.timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	var out ChatResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ResumeWorkflow resumes a workflow paused by Context.AskUser or RequestApproval.
func (c *Client) ResumeWorkflow(ctx context.Context, runID string, userResponse any, opts ...RunOption) (*ResumeWorkflowResponse, error) {
	config := newRunConfig(opts...)
	headers := c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers)
	payload := map[string]any{"user_response": userResponse}
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{"v1", "workflows", "resume", runID}, payload, headers, config.timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return &ResumeWorkflowResponse{
		RunID:  firstString(decoded, "run_id", "runId"),
		Status: firstString(decoded, "status", "state"),
		Offset: fieldInt64(decoded, "offset"),
		Raw:    decoded,
	}, nil
}

// CancelRun requests cancellation of an in-flight run.
func (c *Client) CancelRun(ctx context.Context, runID string, reason string, opts ...RunOption) (*CancelRunResponse, error) {
	config := newRunConfig(opts...)
	headers := c.requestHeaders(config.sessionID, config.userID, config.tenant, config.headers)
	payload := map[string]any{}
	if reason != "" {
		payload["reason"] = reason
	}
	statusCode, body, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{"v1", "runs", runID, "cancel"}, payload, headers, config.timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(body)}
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return &CancelRunResponse{
		RunID:  firstString(decoded, "run_id", "runId"),
		Status: firstString(decoded, "status", "state"),
		Offset: fieldInt64(decoded, "offset"),
		Raw:    decoded,
	}, nil
}

func fieldBool(data map[string]any, keys ...string) bool {
	value, _ := boolFieldPresent(data, keys...)
	return value
}

func boolFieldPresent(data map[string]any, keys ...string) (bool, bool) {
	for _, key := range keys {
		value, ok := data[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case bool:
			return typed, true
		case string:
			return typed == "true", true
		}
	}
	return false, false
}
