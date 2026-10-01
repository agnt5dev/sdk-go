package agnt5

import (
	"regexp"
	"strconv"
	"strings"
)

// Model rules mirroring sdk-core's lm::model_caps. Newer reasoning models
// reject sampling parameters (`temperature`, `top_p`) with a 400 instead of
// ignoring them (AGNT5-1403, AGNT5-1456).

var (
	bedrockVersionSuffix = regexp.MustCompile(`-v\d+:\d+$`)
	openAIGPTMajor       = regexp.MustCompile(`^gpt-(\d+)`)
	openAIOSeries        = regexp.MustCompile(`^o\d+(-|$)`)
	claudeVersionPart    = regexp.MustCompile(`^\d{1,2}$`)
)

// bareModelName strips `provider/` prefixes, Bedrock region and vendor
// prefixes (`us.anthropic.`), a Vertex `@version` suffix and a Bedrock
// `-v1:0` suffix.
func bareModelName(model string) string {
	name := strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if _, rest, ok := strings.Cut(name, "anthropic."); ok {
		name = rest
	}
	name, _, _ = strings.Cut(name, "@")
	return bedrockVersionSuffix.ReplaceAllString(name, "")
}

// isOpenAIReasoningModel reports whether an OpenAI model rejects sampling
// parameters and takes `max_completion_tokens` instead of `max_tokens`:
// gpt-5 and every later `gpt-N`, and the o-series. gpt-4o, gpt-4.1 and
// gpt-oss still accept them.
func isOpenAIReasoningModel(model string) bool {
	name := bareModelName(model)
	if match := openAIGPTMajor.FindStringSubmatch(name); match != nil {
		major, err := strconv.Atoi(match[1])
		return err == nil && major >= 5
	}
	return openAIOSeries.MatchString(name)
}

// claudeRejectsSamplingParams reports whether a Claude model rejects
// `temperature` and `top_p`: everything after Opus 4.6 / Sonnet 4.6 / Haiku
// 4.5, including Fable. New or unrecognised Claude models count as rejecting,
// since dropping a sampling parameter degrades quietly while sending one fails
// the call.
func claudeRejectsSamplingParams(model string) bool {
	name := bareModelName(model)
	rest, ok := strings.CutPrefix(name, "claude-")
	if !ok {
		return false
	}
	var version []int
	for _, token := range strings.FieldsFunc(rest, func(r rune) bool { return r == '-' || r == '.' }) {
		if claudeVersionPart.MatchString(token) {
			part, _ := strconv.Atoi(token)
			version = append(version, part)
			continue
		}
		if len(version) > 0 {
			break
		}
		switch token {
		case "opus", "sonnet", "haiku", "instant":
		default:
			return true
		}
	}
	switch len(version) {
	case 0:
		return true
	case 1:
		return version[0] > 4
	default:
		return version[0] > 4 || (version[0] == 4 && version[1] > 6)
	}
}

// claudeDefaultMaxTokens is the output budget for a Claude call that sets
// none. Thinking counts toward it, so thinking models get more room.
func claudeDefaultMaxTokens(model string) int {
	if claudeRejectsSamplingParams(model) {
		return 16384
	}
	return 4096
}

// openAIToolsNeedNoReasoning reports whether Chat Completions only accepts
// tools on this model with `reasoning_effort: "none"` (gpt-6 and later).
func openAIToolsNeedNoReasoning(model string) bool {
	match := openAIGPTMajor.FindStringSubmatch(bareModelName(model))
	if match == nil {
		return false
	}
	major, err := strconv.Atoi(match[1])
	return err == nil && major >= 6
}
