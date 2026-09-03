package agnt5

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTestSkill(t *testing.T, root, folder, name, description, body string) string {
	t.Helper()
	dir := filepath.Join(root, folder)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	contents := "---\nname: " + name + "\ndescription: " + description + "\n---\n\n" + body + "\n"
	if err := os.WriteFile(filepath.Join(dir, skillFileName), []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSkillFromPathParsesFrontmatterAndBody(t *testing.T) {
	dir := writeTestSkill(t, t.TempDir(), "pdf", "pdf-extraction", "Extract from PDFs", "# Body\nRun scripts/x.py.")
	skill, err := SkillFromPath(dir)
	if err != nil {
		t.Fatal(err)
	}
	if skill.Name != "pdf-extraction" || skill.Description != "Extract from PDFs" {
		t.Fatalf("skill = %#v", skill)
	}
	if !strings.Contains(skill.Instructions, "scripts/x.py") || skill.ResourcesDir != dir {
		t.Fatalf("skill = %#v", skill)
	}
}

func TestSkillFromPathAcceptsFileAndQuotedValues(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, skillFileName)
	if err := os.WriteFile(fileName, []byte("---\nname: \"quoted\"\ndescription: 'has: colon'\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	skill, err := SkillFromPath(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if skill.Name != "quoted" || skill.Description != "has: colon" || skill.Instructions != "body" {
		t.Fatalf("skill = %#v", skill)
	}
}

func TestSkillFromPathRequiresDescription(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, skillFileName), []byte("---\nname: incomplete\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := SkillFromPath(dir); err == nil || !strings.Contains(err.Error(), "description") {
		t.Fatalf("err = %v", err)
	}
}

func TestDiscoverAndResolveSkills(t *testing.T) {
	root := t.TempDir()
	writeTestSkill(t, root, "a", "a", "Skill A", "A body")
	writeTestSkill(t, root, "folder-b", "b", "Skill B", "B body")
	bad := filepath.Join(root, "bad")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, skillFileName), []byte("---\nname: bad\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	discovered, err := DiscoverSkills(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(discovered) != 2 || discovered["a"].Name != "a" || discovered["b"].Name != "b" {
		t.Fatalf("discovered = %#v", discovered)
	}
	all, err := ResolveSkills(nil, root)
	if err != nil || len(all) != 2 {
		t.Fatalf("all = %#v, err = %v", all, err)
	}
	selected, err := ResolveSkills([]string{"folder-b"}, root)
	if err != nil || len(selected) != 1 || selected["b"].Name != "b" {
		t.Fatalf("selected = %#v, err = %v", selected, err)
	}
	if _, err := ResolveSkills([]string{"missing"}, root); err == nil || !strings.Contains(err.Error(), "available: a, b") {
		t.Fatalf("err = %v", err)
	}
	empty, err := ResolveSkills([]string{}, "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty = %#v, err = %v", empty, err)
	}
}

func TestRenderSkillsCatalogDefersInstructions(t *testing.T) {
	catalog := RenderSkillsCatalog(map[string]Skill{
		"pdf": {Name: "pdf", Description: "Extract PDFs", Instructions: "SECRET BODY"},
		"sql": {Name: "sql", Description: "Run SQL", Instructions: "OTHER SECRET"},
	})
	if !strings.Contains(catalog, "- pdf: Extract PDFs") || !strings.Contains(catalog, "- sql: Run SQL") {
		t.Fatalf("catalog = %q", catalog)
	}
	if strings.Contains(catalog, "SECRET") {
		t.Fatalf("catalog disclosed instructions: %q", catalog)
	}
}

type skillJourneyModel struct {
	requests []GenerateRequest
}

func (m *skillJourneyModel) Generate(_ context.Context, request GenerateRequest) (GenerateResponse, error) {
	copied := request
	copied.Messages = cloneMessages(request.Messages)
	m.requests = append(m.requests, copied)
	if len(m.requests) == 1 {
		return GenerateResponse{ToolCalls: []ToolCall{{
			ID:        "skill-call",
			Name:      "load_skill",
			Arguments: map[string]any{"skill_name": "capability"},
		}}}, nil
	}
	return GenerateResponse{Content: "done", FinishReason: "stop"}, nil
}

func TestAgentSkillsLoadBodyMaterializeResourcesAndEmitEvent(t *testing.T) {
	root := t.TempDir()
	writeGuidanceFile(t, filepath.Join(root, agentsMDFileName), "PROJECT_GUIDANCE_SENTINEL")
	dir := writeTestSkill(t, root, "capability", "capability", "Run the capability", "BODY_SENTINEL")
	resources := filepath.Join(dir, "resources")
	if err := os.MkdirAll(resources, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(resources, "value.txt"), []byte("resource-42"), 0o644); err != nil {
		t.Fatal(err)
	}

	model := &skillJourneyModel{}
	sandbox := NewInMemorySandbox()
	agent, err := NewAgent(
		"skills-agent",
		WithAgentModel(model),
		WithAgentInstructions("Base instructions."),
		WithAgentGuidance(root),
		WithAgentSandbox(sandbox),
		WithAgentSkillsFromDir(root, "capability"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newContext(context.Background(), Invocation{ID: "run-1", RunID: "run-1", ComponentType: ComponentTypeFunction}, nil, "")
	result, err := agent.Run(ctx, AgentInput{Message: "use the capability"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Response != "done" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
	if len(model.requests) != 2 || len(model.requests[0].Messages) == 0 {
		t.Fatalf("requests = %#v", model.requests)
	}
	prompt := model.requests[0].Messages[0].Content
	if !strings.Contains(prompt, "PROJECT_GUIDANCE_SENTINEL") ||
		!strings.Contains(prompt, "- capability: Run the capability") ||
		strings.Contains(prompt, "BODY_SENTINEL") ||
		strings.Index(prompt, "<project-guidance>") > strings.Index(prompt, "<skills>") {
		t.Fatalf("initial prompt = %q", prompt)
	}
	var loaded string
	for _, message := range model.requests[1].Messages {
		if message.ToolCallID == "skill-call" {
			loaded = message.Content
		}
	}
	if !strings.Contains(loaded, "BODY_SENTINEL") || !strings.Contains(loaded, "skills/capability/resources/value.txt") {
		t.Fatalf("loaded = %q", loaded)
	}
	resource, err := sandbox.ReadFile(context.Background(), "skills/capability/resources/value.txt")
	if err != nil || string(resource.Content) != "resource-42" {
		t.Fatalf("resource = %#v, err = %v", resource, err)
	}
	if !hasEventType(ctx.Events(), "skill.loaded") {
		t.Fatalf("events = %#v", ctx.Events())
	}
	wantTools := []string{
		"load_skill",
		"sandbox_execute_code",
		"sandbox_list_files",
		"sandbox_read_file",
		"sandbox_write_file",
	}
	for _, name := range wantTools {
		found := false
		for _, tool := range agent.Tools {
			if tool.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing tool %q in %#v", name, agent.Tools)
		}
	}
}

func TestAgentWithoutSkillsPreservesInstructions(t *testing.T) {
	model := &skillJourneyModel{}
	agent, err := NewAgent(
		"plain-agent",
		WithAgentModel(model),
		WithAgentInstructions("Base instructions."),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Run(nil, AgentInput{Message: "hello"}); err != nil {
		t.Fatal(err)
	}
	if got := model.requests[0].Messages[0].Content; got != "Base instructions." {
		t.Fatalf("system instructions = %q", got)
	}
	if len(agent.Tools) != 0 {
		t.Fatalf("tools = %#v", agent.Tools)
	}
}
