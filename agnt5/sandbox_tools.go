package agnt5

import (
	"context"
	"errors"
)

// SandboxTools exposes the standard sandbox operations to an Agent. A nil
// sandbox resolves from the active AGNT5 Context at invocation time.
func SandboxTools(sandbox SandboxRunner) ([]Tool, error) {
	definitions := []struct {
		name        string
		description string
		schema      map[string]any
		handler     ToolHandler
	}{
		{
			name:        "sandbox_execute_code",
			description: "Execute code in a sandboxed environment. Returns stdout, stderr, and exit code.",
			schema: objectSchema(map[string]any{
				"code":     map[string]any{"type": "string", "description": "Source code to execute."},
				"language": map[string]any{"type": "string", "description": "Programming language: python, javascript, or bash."},
			}, "code"),
			handler: func(ctx context.Context, input map[string]any) (any, error) {
				active, err := activeSandbox(ctx, sandbox)
				if err != nil {
					return nil, err
				}
				language, _ := input["language"].(string)
				if language == "" {
					language = "python"
				}
				code, _ := input["code"].(string)
				result, err := active.ExecuteCode(ctx, language, code)
				if err != nil {
					return nil, err
				}
				return map[string]any{
					"stdout":    result.Stdout,
					"stderr":    result.Stderr,
					"exit_code": result.ExitCode,
				}, nil
			},
		},
		{
			name:        "sandbox_write_file",
			description: "Write content to a file in the sandbox workspace.",
			schema: objectSchema(map[string]any{
				"path":    map[string]any{"type": "string", "description": "File path to write."},
				"content": map[string]any{"type": "string", "description": "Text content to write."},
			}, "path", "content"),
			handler: func(ctx context.Context, input map[string]any) (any, error) {
				active, err := activeSandbox(ctx, sandbox)
				if err != nil {
					return nil, err
				}
				fileName, _ := input["path"].(string)
				content, _ := input["content"].(string)
				result, err := active.WriteFile(ctx, fileName, []byte(content))
				if err != nil {
					return nil, err
				}
				return map[string]any{
					"success": true,
					"path":    result.Path,
					"size":    result.Bytes,
				}, nil
			},
		},
		{
			name:        "sandbox_read_file",
			description: "Read the contents of a file from the sandbox workspace.",
			schema: objectSchema(map[string]any{
				"path": map[string]any{"type": "string", "description": "File path to read."},
			}, "path"),
			handler: func(ctx context.Context, input map[string]any) (any, error) {
				active, err := activeSandbox(ctx, sandbox)
				if err != nil {
					return nil, err
				}
				fileName, _ := input["path"].(string)
				result, err := active.ReadFile(ctx, fileName)
				if err != nil {
					return nil, err
				}
				return map[string]any{
					"path":    result.Path,
					"content": string(result.Content),
					"size":    len(result.Content),
					"is_dir":  false,
				}, nil
			},
		},
		{
			name:        "sandbox_list_files",
			description: "List files and directories in the sandbox workspace.",
			schema: objectSchema(map[string]any{
				"path":      map[string]any{"type": "string", "description": "Directory path to list."},
				"recursive": map[string]any{"type": "boolean", "description": "Whether to list recursively."},
			}),
			handler: func(ctx context.Context, input map[string]any) (any, error) {
				active, err := activeSandbox(ctx, sandbox)
				if err != nil {
					return nil, err
				}
				fileName, _ := input["path"].(string)
				if fileName == "" {
					fileName = "."
				}
				result, err := active.ListFiles(ctx, fileName)
				if err != nil {
					return nil, err
				}
				return map[string]any{
					"path":  result.Path,
					"total": len(result.Files),
					"files": result.Files,
				}, nil
			},
		},
	}

	tools := make([]Tool, 0, len(definitions))
	for _, definition := range definitions {
		tool, err := NewTool(
			definition.name,
			definition.handler,
			WithToolDescription(definition.description),
			WithToolSchema(definition.schema),
		)
		if err != nil {
			return nil, err
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

func activeSandbox(ctx context.Context, configured SandboxRunner) (SandboxRunner, error) {
	if configured != nil {
		return configured, nil
	}
	if runCtx, ok := ctx.(*Context); ok && runCtx != nil && runCtx.Sandbox() != nil {
		return runCtx.Sandbox(), nil
	}
	return nil, errors.New("agnt5: no sandbox available")
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = append([]string(nil), required...)
	}
	return schema
}
