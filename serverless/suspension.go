package serverless

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

type UserInputOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}
type UserInput struct {
	Question    string
	Type        string
	Options     []UserInputOption
	AllowCustom bool
	Skippable   bool
}

func WaitForSignal[T any](ctx *Context, signalName, waitingStep string) (T, error) {
	var zero T
	if ctx == nil || strings.TrimSpace(signalName) == "" {
		return zero, errors.New("serverless signal name is required")
	}
	if waitingStep == "" {
		waitingStep = signalName
	}
	if ctx.Metadata["signal_name"] == signalName && defaultString(ctx.Metadata["waiting_step"], signalName) == waitingStep {
		payload := ctx.Metadata["signal_payload"]
		if payload == "" {
			return zero, nil
		}
		if err := json.Unmarshal([]byte(payload), &zero); err != nil {
			if text, ok := any(&zero).(*string); ok {
				*text = payload
				return zero, nil
			}
			return zero, err
		}
		return zero, nil
	}
	return zero, &Suspension{Reason: "signal", SignalName: signalName, WaitingStep: waitingStep}
}

func (c *Context) WaitForUser(input UserInput) (string, error) {
	if c == nil || strings.TrimSpace(input.Question) == "" {
		return "", errors.New("serverless user-input question is required")
	}
	index := c.pauseIndex
	c.pauseIndex++
	if raw := c.Metadata["step_events"]; raw != "" {
		var values map[string]*string
		if json.Unmarshal([]byte(raw), &values) == nil {
			if value, ok := values[jsonNumber(index)]; ok {
				if value == nil {
					return "", nil
				}
				return decodeUserResponse(*value), nil
			}
		}
	}
	if c.Metadata["user_response"] != "" && c.Metadata["pause_index"] == jsonNumber(index) {
		return decodeUserResponse(c.Metadata["user_response"]), nil
	}
	typeName := defaultString(input.Type, "text")
	stepName := "wait_for_user_" + jsonNumber(index)
	return "", &Suspension{Reason: "user_input_required", PauseIndex: index, StepName: stepName, Question: input.Question, InputType: typeName, Options: input.Options, AllowCustom: input.AllowCustom, Skippable: input.Skippable}
}

func jsonNumber(value int) string { return strconv.Itoa(value) }
func decodeUserResponse(value string) string {
	if value == "__skipped__" || value == "__skip__" {
		return ""
	}
	return strings.TrimPrefix(value, "__custom__:")
}
