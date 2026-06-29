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
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// EvalResponse is returned by Client.Eval.
type EvalResponse struct {
	Output json.RawMessage `json:"output,omitempty"`
	Scores []EvalScore     `json:"scores,omitempty"`
	Passed bool            `json:"passed"`
	RunID  string          `json:"run_id,omitempty"`
	Raw    map[string]any  `json:"-"`
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
	scoreRaw, _ := json.Marshal(decoded["scores"])
	_ = json.Unmarshal(scoreRaw, &scores)
	return &EvalResponse{
		Output: raw,
		Scores: scores,
		Passed: fieldBool(decoded, "passed"),
		RunID:  firstString(decoded, "run_id", "runId"),
		Raw:    decoded,
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
	for _, key := range keys {
		value, ok := data[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case bool:
			return typed
		case string:
			return typed == "true"
		}
	}
	return false
}
