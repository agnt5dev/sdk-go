package agnt5

// Logger emits log events tied to a run context.
type Logger struct {
	ctx *Context
}

// Debug emits a debug log event.
func (l *Logger) Debug(message string, keyvals ...any) {
	l.log("DEBUG", EventTypeLogDebug, message, keyvals...)
}

// Info emits an info log event.
func (l *Logger) Info(message string, keyvals ...any) {
	l.log("INFO", EventTypeLogInfo, message, keyvals...)
}

// Warn emits a warning log event.
func (l *Logger) Warn(message string, keyvals ...any) {
	l.log("WARN", EventTypeLogWarn, message, keyvals...)
}

// Error emits an error log event.
func (l *Logger) Error(message string, keyvals ...any) {
	l.log("ERROR", EventTypeLogError, message, keyvals...)
}

func (l *Logger) log(level, eventType, message string, keyvals ...any) {
	if l == nil || l.ctx == nil {
		return
	}
	_ = l.ctx.Emit(Event{
		Type: eventType,
		Data: map[string]any{
			"level":   level,
			"message": message,
			"fields":  keyvalsToMap(keyvals),
		},
	})
}

func keyvalsToMap(keyvals []any) map[string]any {
	fields := make(map[string]any)
	for i := 0; i+1 < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok || key == "" {
			continue
		}
		fields[key] = keyvals[i+1]
	}
	return fields
}
