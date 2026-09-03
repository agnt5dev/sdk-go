package agnt5

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const agentsMDFileName = "AGENTS.md"

// DiscoverAgentsMD walks upward from startDir and returns AGENTS.md paths
// outermost-first. When stopAtGit is true, discovery stops after including the
// nearest directory containing .git.
func DiscoverAgentsMD(startDir string, stopAtGit bool) ([]string, error) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return nil, fmt.Errorf("agnt5: resolve AGENTS.md start directory %q: %w", startDir, err)
	}
	found := make([]string, 0)
	for {
		candidate := filepath.Join(dir, agentsMDFileName)
		if info, statErr := os.Stat(candidate); statErr == nil && info.Mode().IsRegular() {
			found = append(found, candidate)
		} else if statErr != nil && !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("agnt5: inspect %q: %w", candidate, statErr)
		}
		if stopAtGit {
			if _, statErr := os.Stat(filepath.Join(dir, ".git")); statErr == nil {
				break
			} else if !os.IsNotExist(statErr) {
				return nil, fmt.Errorf("agnt5: inspect git boundary %q: %w", dir, statErr)
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	for left, right := 0, len(found)-1; left < right; left, right = left+1, right-1 {
		found[left], found[right] = found[right], found[left]
	}
	return found, nil
}

// LoadAgentsMD loads file or directory sources in order. Directories resolve
// to their AGENTS.md; missing sources are ignored.
func LoadAgentsMD(sources ...string) (string, error) {
	parts := make([]string, 0, len(sources))
	for _, source := range sources {
		info, err := os.Stat(source)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("agnt5: inspect AGENTS.md source %q: %w", source, err)
		}
		fileName := source
		if info.IsDir() {
			fileName = filepath.Join(source, agentsMDFileName)
			if _, err := os.Stat(fileName); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return "", fmt.Errorf("agnt5: inspect %q: %w", fileName, err)
			}
		}
		contents, err := os.ReadFile(fileName)
		if err != nil {
			return "", fmt.Errorf("agnt5: read %q: %w", fileName, err)
		}
		if text := strings.TrimSpace(string(contents)); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n"), nil
}

// RenderProjectGuidance wraps always-on guidance for prompt composition.
func RenderProjectGuidance(guidance string) string {
	if guidance == "" {
		return ""
	}
	return "<project-guidance>\n" + guidance + "\n</project-guidance>"
}
