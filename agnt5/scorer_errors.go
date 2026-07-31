package agnt5

import "fmt"

// ScorerError is a typed scorer execution error returned by the eval runtime.
type ScorerError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Scorer  string         `json:"scorer,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *ScorerError) Error() string {
	if e == nil {
		return ""
	}
	if e.Scorer != "" {
		return fmt.Sprintf("agnt5: scorer %s: %s", e.Scorer, e.Message)
	}
	return "agnt5: scorer: " + e.Message
}

// ScorerNotFoundError reports an unknown custom scorer name.
type ScorerNotFoundError struct {
	Name string
}

func (e *ScorerNotFoundError) Error() string {
	return "agnt5: scorer not found: " + e.Name
}

// ScorerNameCollisionError reports duplicate or reserved scorer names.
type ScorerNameCollisionError struct {
	Name    string
	BuiltIn bool
}

func (e *ScorerNameCollisionError) Error() string {
	if e.BuiltIn {
		return fmt.Sprintf("agnt5: scorer name collision: %q is an AGNT5 built-in scorer", e.Name)
	}
	return fmt.Sprintf("agnt5: scorer name collision: %q is already registered", e.Name)
}

func (e *ScorerNameCollisionError) Unwrap() error {
	return ErrDuplicateComponent
}
