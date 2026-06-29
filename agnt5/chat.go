package agnt5

import "encoding/json"

// ChatMessage is a user-facing chat message.
type ChatMessage struct {
	SessionID string         `json:"session_id,omitempty"`
	UserID    string         `json:"user_id,omitempty"`
	Role      MessageRole    `json:"role"`
	Content   string         `json:"content"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// ChatResponse is returned by ChatBot.
type ChatResponse struct {
	SessionID string      `json:"session_id,omitempty"`
	Message   ChatMessage `json:"message"`
}

// ChatBot wraps an Agent with conversation memory.
type ChatBot struct {
	Name  string
	Agent *Agent
}

func NewChatBot(name string, agent *Agent) (*ChatBot, error) {
	if name == "" {
		return nil, ErrInvalidComponentName
	}
	if agent == nil {
		return nil, ErrNilHandler
	}
	return &ChatBot{Name: name, Agent: agent}, nil
}

func (b *ChatBot) Handle(ctx *Context, message ChatMessage) (ChatResponse, error) {
	if err := ctx.Memory().Conversation().Append(ctx, MemoryMessage{
		Role:     string(message.Role),
		Content:  message.Content,
		Metadata: message.Metadata,
	}); err != nil {
		return ChatResponse{}, err
	}
	result, err := b.Agent.Run(ctx, AgentInput{Message: message.Content})
	if err != nil {
		return ChatResponse{}, err
	}
	reply := ChatMessage{
		SessionID: message.SessionID,
		UserID:    message.UserID,
		Role:      MessageRoleAssistant,
		Content:   result.Response,
	}
	if err := ctx.Memory().Conversation().Append(ctx, MemoryMessage{
		Role:    string(reply.Role),
		Content: reply.Content,
	}); err != nil {
		return ChatResponse{}, err
	}
	return ChatResponse{SessionID: message.SessionID, Message: reply}, nil
}

// RegisterChatBot registers a ChatBot as an agent-routed component.
func RegisterChatBot(w *Worker, bot *ChatBot, opts ...ComponentOption) error {
	if bot == nil {
		return ErrNilHandler
	}
	return RegisterRaw(w, bot.Name, ComponentTypeChat, func(ctx *Context, input []byte) ([]byte, error) {
		var message ChatMessage
		if len(input) > 0 {
			if err := json.Unmarshal(input, &message); err != nil {
				return nil, err
			}
		}
		out, err := bot.Handle(ctx, message)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
}
