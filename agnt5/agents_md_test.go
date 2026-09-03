package agnt5

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeGuidanceFile(t *testing.T, fileName, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fileName, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadAgentsMDLoadsFilesAndDirectoriesInOrder(t *testing.T) {
	root := t.TempDir()
	writeGuidanceFile(t, filepath.Join(root, agentsMDFileName), "Root rules.\n")
	writeGuidanceFile(t, filepath.Join(root, "sub", agentsMDFileName), "Sub rules.\n")
	guidance, err := LoadAgentsMD(
		filepath.Join(root, agentsMDFileName),
		filepath.Join(root, "missing", agentsMDFileName),
		filepath.Join(root, "sub"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if guidance != "Root rules.\n\nSub rules." {
		t.Fatalf("guidance = %q", guidance)
	}
}

func TestDiscoverAgentsMDStopsAtGitBoundary(t *testing.T) {
	outer := t.TempDir()
	root := filepath.Join(outer, "repo")
	leaf := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeGuidanceFile(t, filepath.Join(outer, agentsMDFileName), "outside")
	writeGuidanceFile(t, filepath.Join(root, agentsMDFileName), "root")
	writeGuidanceFile(t, filepath.Join(leaf, agentsMDFileName), "leaf")
	found, err := DiscoverAgentsMD(leaf, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(root, agentsMDFileName),
		filepath.Join(leaf, agentsMDFileName),
	}
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("found = %#v, want %#v", found, want)
	}
}

func TestRenderProjectGuidance(t *testing.T) {
	if RenderProjectGuidance("") != "" {
		t.Fatal("empty guidance changed the prompt")
	}
	if got := RenderProjectGuidance("Rules."); got != "<project-guidance>\nRules.\n</project-guidance>" {
		t.Fatalf("guidance = %q", got)
	}
}
