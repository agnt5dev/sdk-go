package serverless

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

func RegisterFunction[In any, Out any](h *Handler, name string, fn func(*Context, In) (Out, error)) error {
	return registerTyped(h, "function", name, fn)
}

type Tool struct {
	Name        string
	Description string
	Schema      map[string]any
	Handler     func(context.Context, map[string]any) (any, error)
}

func RegisterTool(h *Handler, tool Tool) error {
	if h == nil || strings.TrimSpace(tool.Name) == "" || tool.Handler == nil {
		return errors.New("serverless tool name and handler are required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	key := "tool:" + strings.TrimSpace(tool.Name)
	if _, exists := h.components[key]; exists {
		return errors.New("serverless tool is already registered")
	}
	h.components[key] = component{name: strings.TrimSpace(tool.Name), kind: "tool", metadata: map[string]any{"description": tool.Description, "input_schema": tool.Schema}, invoke: func(ctx *Context, raw json.RawMessage) (any, error) {
		args := map[string]any{}
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &args); err != nil {
				return nil, err
			}
		}
		return tool.Handler(ctx, args)
	}}
	return nil
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type AgentInput struct {
	Message   string    `json:"message,omitempty"`
	Prompt    string    `json:"prompt,omitempty"`
	Input     string    `json:"input,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	History   []Message `json:"history,omitempty"`
}
type AgentResult struct {
	Output   string    `json:"output"`
	Messages []Message `json:"messages,omitempty"`
}
type Agent struct {
	Name        string
	Description string
	Run         func(*Context, AgentInput) (AgentResult, error)
}
type AgentSessionCheckpoint struct {
	Messages    []Message `json:"messages"`
	UpdatedAtMS int64     `json:"updated_at_ms,omitempty"`
}

func RegisterAgent(h *Handler, agent Agent) error {
	if h == nil || strings.TrimSpace(agent.Name) == "" || agent.Run == nil {
		return errors.New("serverless agent name and runner are required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	key := "agent:" + strings.TrimSpace(agent.Name)
	if _, exists := h.components[key]; exists {
		return errors.New("serverless agent is already registered")
	}
	h.components[key] = component{name: strings.TrimSpace(agent.Name), kind: "agent", metadata: map[string]any{"description": agent.Description}, invoke: func(ctx *Context, raw json.RawMessage) (any, error) {
		var input AgentInput
		if len(raw) > 0 && string(raw) != "null" {
			if raw[0] == '"' {
				if err := json.Unmarshal(raw, &input.Message); err != nil {
					return nil, err
				}
			} else if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
		}
		sessionID := input.SessionID
		if sessionID == "" {
			sessionID = ctx.Metadata["session_id"]
		}
		if sessionID == "" {
			sessionID = ctx.RunID
		}
		if saved, ok := ctx.agentSessions[sessionID]; ok {
			input.History = append([]Message(nil), saved.Messages...)
		}
		message := input.Message
		if message == "" {
			message = input.Prompt
		}
		if message == "" {
			message = input.Input
		}
		input.Message = message
		result, err := agent.Run(ctx, input)
		if err != nil {
			return nil, err
		}
		messages := result.Messages
		if len(messages) == 0 {
			messages = append(append([]Message{}, input.History...), Message{Role: "user", Content: message}, Message{Role: "assistant", Content: result.Output})
		}
		ctx.agentSessions[sessionID] = AgentSessionCheckpoint{Messages: messages, UpdatedAtMS: time.Now().UnixMilli()}
		ctx.Emit(Event{EventType: "session.created", Data: map[string]any{"session_id": sessionID, "agent_name": agent.Name, "turn_count": len(messages)}, Metadata: map[string]string{"session_id": sessionID, "agent_name": agent.Name, "session_type": "agent"}})
		return result.Output, nil
	}}
	return nil
}
