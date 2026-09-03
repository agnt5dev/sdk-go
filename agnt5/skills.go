package agnt5

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const skillFileName = "SKILL.md"

// Skill is an on-demand agent capability loaded from a SKILL.md directory.
// Only Name and Description are placed in the model's initial context;
// Instructions and bundled resources are disclosed by the load_skill tool.
type Skill struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Instructions string `json:"instructions"`
	ResourcesDir string `json:"resources_dir,omitempty"`
}

// SkillFromPath parses a skill directory or a direct SKILL.md path.
func SkillFromPath(skillPath string) (Skill, error) {
	info, err := os.Stat(skillPath)
	if err != nil {
		return Skill{}, fmt.Errorf("agnt5: load skill %q: %w", skillPath, err)
	}
	fileName := skillPath
	if info.IsDir() {
		fileName = filepath.Join(skillPath, skillFileName)
	}
	fileInfo, err := os.Stat(fileName)
	if err != nil || !fileInfo.Mode().IsRegular() {
		if err == nil {
			err = fmt.Errorf("not a regular file")
		}
		return Skill{}, fmt.Errorf("agnt5: no %s found at %q: %w", skillFileName, skillPath, err)
	}
	contents, err := os.ReadFile(fileName)
	if err != nil {
		return Skill{}, fmt.Errorf("agnt5: read skill %q: %w", fileName, err)
	}
	metadata, body := parseSkillFrontmatter(string(contents))
	name := strings.TrimSpace(metadata["name"])
	if name == "" {
		return Skill{}, fmt.Errorf("agnt5: %s frontmatter is missing required name", fileName)
	}
	description := strings.TrimSpace(metadata["description"])
	if description == "" {
		return Skill{}, fmt.Errorf("agnt5: %s frontmatter is missing required description", fileName)
	}
	return Skill{
		Name:         name,
		Description:  description,
		Instructions: strings.TrimSpace(body),
		ResourcesDir: filepath.Dir(fileName),
	}, nil
}

func parseSkillFrontmatter(contents string) (map[string]string, string) {
	normalized := strings.ReplaceAll(contents, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return map[string]string{}, contents
	}
	metadata := make(map[string]string)
	bodyStart := len(lines)
	for index := 1; index < len(lines); index++ {
		line := strings.TrimSpace(lines[index])
		if line == "---" {
			bodyStart = index + 1
			break
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		metadata[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "'\"")
	}
	return metadata, strings.Join(lines[bodyStart:], "\n")
}

// DiscoverSkills parses immediate child directories containing SKILL.md.
// Malformed entries are ignored so an unrelated bad skill does not prevent an
// agent from using the rest of the pool.
func DiscoverSkills(skillsDir string) (map[string]Skill, error) {
	entries, err := os.ReadDir(skillsDir)
	if err != nil {
		return nil, fmt.Errorf("agnt5: read skills directory %q: %w", skillsDir, err)
	}
	skills := make(map[string]Skill)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		folder := filepath.Join(skillsDir, entry.Name())
		if _, err := os.Stat(filepath.Join(folder, skillFileName)); err != nil {
			continue
		}
		skill, err := SkillFromPath(folder)
		if err != nil {
			continue
		}
		skills[skill.Name] = skill
	}
	return skills, nil
}

// ResolveSkills selects named skills from a directory. A nil selection loads
// the full pool; an empty, non-nil selection loads none.
func ResolveSkills(names []string, skillsDir string) (map[string]Skill, error) {
	if names == nil {
		if strings.TrimSpace(skillsDir) == "" {
			return map[string]Skill{}, nil
		}
		return DiscoverSkills(skillsDir)
	}
	if len(names) == 0 {
		return map[string]Skill{}, nil
	}
	if strings.TrimSpace(skillsDir) == "" {
		return nil, fmt.Errorf("agnt5: named skills require a skills directory")
	}
	available, err := DiscoverSkills(skillsDir)
	if err != nil {
		return nil, err
	}
	resolved := make(map[string]Skill, len(names))
	for _, name := range names {
		skill, ok := available[name]
		if !ok {
			folder := filepath.Join(skillsDir, name)
			if _, statErr := os.Stat(filepath.Join(folder, skillFileName)); statErr == nil {
				skill, err = SkillFromPath(folder)
				ok = err == nil
			}
		}
		if !ok {
			availableNames := make([]string, 0, len(available))
			for availableName := range available {
				availableNames = append(availableNames, availableName)
			}
			sort.Strings(availableNames)
			availableText := strings.Join(availableNames, ", ")
			if availableText == "" {
				availableText = "(none)"
			}
			return nil, fmt.Errorf("agnt5: skill %q not found in %s; available: %s", name, skillsDir, availableText)
		}
		resolved[skill.Name] = skill
	}
	return resolved, nil
}

// RenderSkillsCatalog returns the progressive-disclosure catalog injected into
// the system prompt. Skill bodies and resource contents are intentionally absent.
func RenderSkillsCatalog(skills map[string]Skill) string {
	if len(skills) == 0 {
		return ""
	}
	names := make([]string, 0, len(skills))
	for name := range skills {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := []string{
		"<skills>",
		"You have access to the following skills. When a task matches a skill's purpose, call load_skill(skill_name) to load its full instructions before proceeding.",
		"",
	}
	for _, name := range names {
		skill := skills[name]
		lines = append(lines, "- "+skill.Name+": "+skill.Description)
	}
	lines = append(lines, "</skills>")
	return strings.Join(lines, "\n")
}

// NewLoadSkillTool creates the progressive-disclosure tool for a resolved set
// of skills. Bundled files are materialized into the configured sandbox.
func NewLoadSkillTool(skills map[string]Skill, sandbox SandboxRunner) (Tool, error) {
	return NewTool(
		"load_skill",
		func(ctx context.Context, input map[string]any) (any, error) {
			skillName, _ := input["skill_name"].(string)
			skill, ok := skills[skillName]
			if !ok {
				names := make([]string, 0, len(skills))
				for name := range skills {
					names = append(names, name)
				}
				sort.Strings(names)
				available := strings.Join(names, ", ")
				if available == "" {
					available = "(none)"
				}
				return fmt.Sprintf("Unknown skill %q. Available skills: %s", skillName, available), nil
			}

			materialized, err := materializeSkillResources(ctx, skill, sandbox)
			if err != nil {
				return nil, err
			}
			emitSkillLoaded(ctx, skill, len(materialized))
			if len(materialized) == 0 {
				return skill.Instructions, nil
			}
			base := path.Join("skills", skill.Name)
			lines := make([]string, 0, len(materialized))
			for _, fileName := range materialized {
				lines = append(lines, "- "+fileName)
			}
			return skill.Instructions + "\n\n---\n" +
				"Bundled resources are available in the sandbox under '" + base + "':\n" +
				strings.Join(lines, "\n"), nil
		},
		WithToolDescription("Load the full instructions for a skill by name. Call this when a task matches a skill listed in the <skills> catalog before proceeding."),
		WithToolSchema(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"skill_name": map[string]any{
					"type":        "string",
					"description": "Name of the skill to load (as shown in the catalog).",
				},
			},
			"required": []string{"skill_name"},
		}),
	)
}

func materializeSkillResources(ctx context.Context, skill Skill, sandbox SandboxRunner) ([]string, error) {
	if sandbox == nil || skill.ResourcesDir == "" {
		return nil, nil
	}
	files := make([]string, 0)
	err := filepath.WalkDir(skill.ResourcesDir, func(fileName string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() == skillFileName {
			return nil
		}
		files = append(files, fileName)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("agnt5: walk resources for skill %q: %w", skill.Name, err)
	}
	sort.Strings(files)
	written := make([]string, 0, len(files))
	for _, fileName := range files {
		contents, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("agnt5: read resource %q: %w", fileName, err)
		}
		relative, err := filepath.Rel(skill.ResourcesDir, fileName)
		if err != nil {
			return nil, fmt.Errorf("agnt5: resolve resource %q: %w", fileName, err)
		}
		destination := path.Join("skills", skill.Name, filepath.ToSlash(relative))
		if _, err := sandbox.WriteFile(ctx, destination, contents); err != nil {
			return nil, fmt.Errorf("agnt5: materialize resource %q: %w", destination, err)
		}
		written = append(written, destination)
	}
	return written, nil
}

func emitSkillLoaded(ctx context.Context, skill Skill, resourceCount int) {
	runCtx, ok := ctx.(*Context)
	if !ok || runCtx == nil {
		return
	}
	_ = runCtx.Emit(lifecycleEvent(
		"skill.loaded",
		"load_skill",
		"skill",
		newCorrelationID("skill"),
		runCtx.parentCorrelationID(),
		map[string]any{
			"skill_name":             skill.Name,
			"instructions_length":    len(skill.Instructions),
			"resources_materialized": resourceCount,
			"output_data": map[string]any{
				"skill":                  skill.Name,
				"resources_materialized": resourceCount,
			},
		},
	))
}
