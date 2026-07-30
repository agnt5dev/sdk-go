package agnt5

import (
	"strings"
	"time"
)

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

func lifecycleEvent(
	eventType string,
	name string,
	componentType string,
	correlationID string,
	parentCorrelationID string,
	fields map[string]any,
) Event {
	timestampNS := time.Now().UnixNano()
	eventID := newEventID()
	data := make(map[string]any, len(fields)+6)
	for key, value := range fields {
		data[key] = value
	}
	data["event_id"] = eventID
	data["name"] = name
	data["component_type"] = componentType
	data["correlation_id"] = correlationID
	data["parent_correlation_id"] = parentCorrelationID
	data["timestamp_ns"] = timestampNS

	metadata := map[string]string{
		"name":           name,
		"component_type": componentType,
		"cid":            correlationID,
		"correlation_id": correlationID,
	}
	if parentCorrelationID != "" {
		metadata["pcid"] = parentCorrelationID
		metadata["parent_correlation_id"] = parentCorrelationID
	}

	return Event{
		Type:                eventType,
		Data:                data,
		Metadata:            metadata,
		CorrelationID:       correlationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   timestampNS,
	}
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
