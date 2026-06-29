package agnt5

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// HITLInputType describes the requested user input shape.
type HITLInputType string

const (
	HITLText        HITLInputType = "text"
	HITLSelect      HITLInputType = "select"
	HITLMultiSelect HITLInputType = "multiselect"
	HITLApproval    HITLInputType = "approval"
)

// HITLOption is one selectable user input option.
type HITLOption struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// UserInputRequest is emitted when a workflow pauses for user input.
type UserInputRequest struct {
	ID          string         `json:"id"`
	Prompt      string         `json:"prompt"`
	Type        HITLInputType  `json:"type"`
	Options     []HITLOption   `json:"options,omitempty"`
	AllowCustom bool           `json:"allow_custom,omitempty"`
	Skippable   bool           `json:"skippable,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// WaitingForUserInputError marks an invocation as waiting for user input.
type WaitingForUserInputError struct {
	Request UserInputRequest
}

func (e *WaitingForUserInputError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("agnt5: waiting for user input %s", e.Request.ID)
}

// IsWaitingForUserInput reports whether err carries a user-input request.
func IsWaitingForUserInput(err error) bool {
	var target *WaitingForUserInputError
	return errors.As(err, &target)
}

// AskUser returns a replayed response on workflow resume, otherwise emits
// approval.requested and workflow.paused before returning WaitingForUserInputError.
func (c *Context) AskUser(request UserInputRequest) (string, error) {
	if request.ID == "" {
		request.ID = c.RunID() + ":user-input"
	}
	if request.Type == "" {
		request.Type = HITLText
	}
	pauseIndex := c.nextPauseIndex()
	if response, ok := c.userResponse(pauseIndex); ok {
		return response, nil
	}
	stepName := fmt.Sprintf("wait_for_user_%d", pauseIndex)
	metadata := c.pauseMetadata(request, pauseIndex, stepName)
	_ = c.Emit(Event{Type: "approval.requested", Data: request, Metadata: cloneStringMap(metadata)})
	_ = c.Emit(Event{
		Type: "workflow.paused",
		Data: map[string]any{
			"reason": "user_input_required",
			"pause_data": map[string]any{
				"question":     request.Prompt,
				"input_type":   request.Type,
				"options":      request.Options,
				"pause_index":  pauseIndex,
				"step_name":    stepName,
				"allow_custom": request.AllowCustom,
				"skippable":    request.Skippable,
			},
			"metadata": metadata,
		},
		Metadata: cloneStringMap(metadata),
	})
	return "", &WaitingForUserInputError{Request: request}
}

// RequestApproval asks for a yes/no approval.
func (c *Context) RequestApproval(prompt string, metadata map[string]any) (bool, error) {
	response, err := c.AskUser(UserInputRequest{
		Prompt:   prompt,
		Type:     HITLApproval,
		Metadata: cloneAnyMap(metadata),
		Options: []HITLOption{
			{Label: "Approve", Value: "approve"},
			{Label: "Reject", Value: "reject"},
		},
	})
	if err != nil {
		return false, err
	}
	return strings.EqualFold(response, "approve") || strings.EqualFold(response, "approved") || strings.EqualFold(response, "yes"), nil
}

func (c *Context) nextPauseIndex() int {
	c.hitlMu.Lock()
	defer c.hitlMu.Unlock()
	idx := c.pauseIndex
	c.pauseIndex++
	return idx
}

func (c *Context) userResponse(pauseIndex int) (string, bool) {
	c.hitlMu.Lock()
	defer c.hitlMu.Unlock()
	response, ok := c.userResponses[pauseIndex]
	if !ok {
		return "", false
	}
	if response == nil {
		return "", true
	}
	return *response, true
}

func (c *Context) loadReplayMetadata(metadata map[string]string) {
	if len(metadata) == 0 {
		return
	}
	c.loadStepEvents(metadata["step_events"])
	c.loadCompletedSteps(metadata["completed_steps"])
	if raw, ok := metadata["user_response"]; ok {
		pauseIndex := 0
		if parsed, err := strconv.Atoi(metadata["pause_index"]); err == nil && parsed >= 0 {
			pauseIndex = parsed
		}
		c.userResponses[pauseIndex] = decodeUserResponse(raw)
	}
}

func (c *Context) loadStepEvents(raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	var responses map[string]*string
	if err := json.Unmarshal([]byte(raw), &responses); err == nil {
		for key, value := range responses {
			idx, err := strconv.Atoi(key)
			if err == nil && idx >= 0 {
				c.userResponses[idx] = value
			}
		}
		return
	}
	var events []map[string]any
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		return
	}
	for _, event := range events {
		stepName, _ := event["step_name"].(string)
		result := event["result"]
		if strings.HasPrefix(stepName, "wait_for_user_") {
			idx, err := strconv.Atoi(strings.TrimPrefix(stepName, "wait_for_user_"))
			if err != nil {
				continue
			}
			text := fmt.Sprint(result)
			c.userResponses[idx] = &text
			continue
		}
		if stepName != "" && result != nil {
			if payload, err := json.Marshal(result); err == nil {
				c.completedSteps[stepName] = payload
			}
		}
	}
}

func (c *Context) loadCompletedSteps(raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	var completed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &completed); err != nil {
		return
	}
	for key, value := range completed {
		c.completedSteps[key] = append([]byte(nil), value...)
	}
}

func decodeUserResponse(raw string) *string {
	switch {
	case raw == "__skipped__" || raw == "__skip__":
		return nil
	case strings.HasPrefix(raw, "__custom__:"):
		value := strings.TrimPrefix(raw, "__custom__:")
		return &value
	default:
		return &raw
	}
}

func (c *Context) pauseMetadata(request UserInputRequest, pauseIndex int, stepName string) map[string]string {
	metadata := map[string]string{
		"pause_reason": "user_input_required",
		"pause_index":  strconv.Itoa(pauseIndex),
		"step_name":    stepName,
		"question":     request.Prompt,
	}
	if request.Type != "" {
		metadata["input_type"] = string(request.Type)
	}
	if request.AllowCustom {
		metadata["allow_custom"] = "true"
	}
	if request.Skippable {
		metadata["skippable"] = "true"
	}
	if len(request.Options) > 0 {
		if payload, err := json.Marshal(request.Options); err == nil {
			metadata["options"] = string(payload)
		}
	}
	if payload := c.userResponsesJSON(); payload != "" {
		metadata["step_events"] = payload
	}
	if payload := c.completedStepsJSON(); payload != "" {
		metadata["completed_steps"] = payload
	}
	return metadata
}

func (c *Context) userResponsesJSON() string {
	c.hitlMu.Lock()
	defer c.hitlMu.Unlock()
	if len(c.userResponses) == 0 {
		return ""
	}
	out := make(map[string]*string, len(c.userResponses))
	for idx, response := range c.userResponses {
		out[strconv.Itoa(idx)] = response
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(payload)
}

func (c *Context) completedStepPayload(stepKey string) ([]byte, bool) {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	payload, ok := c.completedSteps[stepKey]
	return append([]byte(nil), payload...), ok
}

func (c *Context) recordCompletedStep(stepKey string, payload []byte) {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	c.completedSteps[stepKey] = append([]byte(nil), payload...)
}

func (c *Context) completedStepsJSON() string {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	if len(c.completedSteps) == 0 {
		return ""
	}
	out := make(map[string]json.RawMessage, len(c.completedSteps))
	for key, payload := range c.completedSteps {
		out[key] = append([]byte(nil), payload...)
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(payload)
}
