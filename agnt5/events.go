package agnt5

import "strings"

const (
	EventTypeOutputDelta = "output.delta"
	EventTypeLogDebug    = "log.debug"
	EventTypeLogInfo     = "log.info"
	EventTypeLogWarn     = "log.warn"
	EventTypeLogError    = "log.error"
)

// Event is the language-level event representation used before transport delivery.
type Event struct {
	RunID               string
	Type                string
	Data                any
	Metadata            map[string]string
	CorrelationID       string
	ParentCorrelationID string
	ContentIndex        int
	Sequence            int64
	SourceTimestampNS   int64
}

// IsSSEOnlyEventType mirrors the SDK event-classification contract.
func IsSSEOnlyEventType(eventType string) bool {
	switch {
	case strings.HasPrefix(eventType, "output."):
		return true
	case strings.HasPrefix(eventType, "lm.stream."):
		return true
	case strings.HasPrefix(eventType, "lm.message."):
		return true
	case strings.HasPrefix(eventType, "lm.thinking."):
		return true
	case strings.HasPrefix(eventType, "lm.tool_call."):
		return true
	case strings.HasPrefix(eventType, "progress."):
		return true
	case eventType == "log" || strings.HasPrefix(eventType, "log."):
		return true
	default:
		return false
	}
}
