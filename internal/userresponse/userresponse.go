// Package userresponse decodes the answer a person gives a paused workflow.
package userresponse

import (
	"bytes"
	"encoding/json"
	"strings"
)

const customPrefix = "__custom__:"

// multiselectTypes are the question types answered with a JSON array of
// ticked options. "choice" is a legacy name for "multiselect", and
// "pick_many" is the one the playground uses.
var multiselectTypes = map[string]bool{"multiselect": true, "choice": true, "pick_many": true}

// Decode returns the answer a workflow sees, and whether the person skipped
// the question ("__skipped__" or "__skip__"). A typed answer,
// "__custom__:<text>", becomes its text. For a multiselect question the
// answer is a JSON-array string; text typed beside the ticked options is an
// entry of its own, "__custom__:<text>", and becomes that text, so
// `["ai","__custom__:Rust"]` returns `["ai","Rust"]`. Anything else returns
// unchanged: an array with no such entry, and an array answering any other
// kind of question, or one whose kind is unknown, which is text the person
// typed.
func Decode(raw, inputType string) (value string, skipped bool) {
	switch {
	case raw == "__skipped__" || raw == "__skip__":
		return "", true
	case strings.HasPrefix(raw, customPrefix):
		return strings.TrimPrefix(raw, customPrefix), false
	case multiselectTypes[inputType] && strings.HasPrefix(raw, "[") && strings.Contains(raw, customPrefix):
		return decodeTypedEntries(raw), false
	default:
		return raw, false
	}
}

func decodeTypedEntries(raw string) string {
	var entries []json.RawMessage
	if json.Unmarshal([]byte(raw), &entries) != nil {
		return raw
	}
	typed := false
	values := make([]any, len(entries))
	for index, entry := range entries {
		values[index] = entry
		var text string
		if json.Unmarshal(entry, &text) == nil && strings.HasPrefix(text, customPrefix) {
			values[index] = strings.TrimPrefix(text, customPrefix)
			typed = true
		}
	}
	if !typed {
		return raw
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(values) != nil {
		return raw
	}
	return strings.TrimSuffix(out.String(), "\n")
}
