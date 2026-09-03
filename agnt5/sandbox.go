package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// SandboxRunner is the common sandbox operation boundary.
type SandboxRunner interface {
	ExecuteCode(ctx context.Context, language, code string) (ExecuteCodeResult, error)
	RunCommand(ctx context.Context, command []string) (RunCommandResult, error)
	WriteFile(ctx context.Context, path string, content []byte) (WriteFileResult, error)
	ReadFile(ctx context.Context, path string) (ReadFileResult, error)
	ListFiles(ctx context.Context, path string) (ListFilesResult, error)
}

// SandboxFileDeleter is the additive file-deletion capability implemented by
// AGNT5 sandbox workspaces. Keeping it separate preserves custom SandboxRunner
// adapters that predate deletion support.
type SandboxFileDeleter interface {
	DeleteFile(ctx context.Context, path string, recursive bool) (bool, error)
}

// SandboxWorkspace is the complete AGNT5 sandbox file and execution surface.
type SandboxWorkspace interface {
	SandboxRunner
	SandboxFileDeleter
}

type ExecuteCodeResult struct {
	Stdout   string `json:"stdout,omitempty"`
	Stderr   string `json:"stderr,omitempty"`
	ExitCode int    `json:"exit_code"`
}

type RunCommandResult struct {
	Stdout   string `json:"stdout,omitempty"`
	Stderr   string `json:"stderr,omitempty"`
	ExitCode int    `json:"exit_code"`
}

type WriteFileResult struct {
	Path  string `json:"path"`
	Bytes int    `json:"bytes"`
}

type ReadFileResult struct {
	Path    string `json:"path"`
	Content []byte `json:"content"`
}

type FileInfo struct {
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size,omitempty"`
}

type ListFilesResult struct {
	Path  string     `json:"path"`
	Files []FileInfo `json:"files"`
}

// HTTPSandbox talks to an AGNT5-compatible sandbox HTTP endpoint.
type HTTPSandbox struct {
	Endpoint   string
	APIKey     string
	HTTPClient *http.Client
}

func NewHTTPSandbox(endpoint string, apiKey string) *HTTPSandbox {
	return &HTTPSandbox{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (s *HTTPSandbox) ExecuteCode(ctx context.Context, language, code string) (ExecuteCodeResult, error) {
	emitSandboxEvent(ctx, "sandbox.execute.started", map[string]any{"language": language})
	var out ExecuteCodeResult
	err := s.post(ctx, "/execute", map[string]any{"language": language, "code": code}, &out)
	emitSandboxResult(ctx, "sandbox.execute", out.ExitCode, err)
	return out, err
}

func (s *HTTPSandbox) RunCommand(ctx context.Context, command []string) (RunCommandResult, error) {
	emitSandboxEvent(ctx, "sandbox.command.started", map[string]any{"command": command})
	var out RunCommandResult
	err := s.post(ctx, "/commands", map[string]any{"command": command}, &out)
	emitSandboxResult(ctx, "sandbox.command", out.ExitCode, err)
	return out, err
}

func (s *HTTPSandbox) WriteFile(ctx context.Context, path string, content []byte) (WriteFileResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.write.started", map[string]any{"path": path, "bytes": len(content)})
	var out WriteFileResult
	err := s.post(ctx, "/files/write", map[string]any{"path": path, "content": string(content)}, &out)
	emitSandboxResult(ctx, "sandbox.file.write", 0, err)
	return out, err
}

func (s *HTTPSandbox) ReadFile(ctx context.Context, path string) (ReadFileResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.read.started", map[string]any{"path": path})
	var out ReadFileResult
	err := s.post(ctx, "/files/read", map[string]any{"path": path}, &out)
	emitSandboxResult(ctx, "sandbox.file.read", 0, err)
	return out, err
}

func (s *HTTPSandbox) ListFiles(ctx context.Context, path string) (ListFilesResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.list.started", map[string]any{"path": path})
	var out ListFilesResult
	err := s.post(ctx, "/files/list", map[string]any{"path": path}, &out)
	emitSandboxResult(ctx, "sandbox.file.list", 0, err)
	return out, err
}

func (s *HTTPSandbox) DeleteFile(ctx context.Context, path string, recursive bool) (bool, error) {
	emitSandboxEvent(ctx, "sandbox.file.delete.started", map[string]any{"path": path, "recursive": recursive})
	var raw json.RawMessage
	err := s.post(ctx, "/files/delete", map[string]any{"path": path, "recursive": recursive}, &raw)
	if err != nil {
		emitSandboxResult(ctx, "sandbox.file.delete", 0, err)
		return false, err
	}
	deleted, err := sandboxDeleteResult(raw)
	emitSandboxResult(ctx, "sandbox.file.delete", 0, err)
	return deleted, err
}

func sandboxDeleteResult(raw json.RawMessage) (bool, error) {
	var deleted bool
	if err := json.Unmarshal(raw, &deleted); err == nil {
		return deleted, nil
	}
	var result struct {
		Deleted *bool `json:"deleted"`
		Success *bool `json:"success"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return false, fmt.Errorf("agnt5: decode sandbox delete response: %w", err)
	}
	if result.Deleted != nil {
		return *result.Deleted, nil
	}
	if result.Success != nil {
		return *result.Success, nil
	}
	return false, errors.New("agnt5: sandbox delete response is missing deleted or success")
}

func (s *HTTPSandbox) post(ctx context.Context, path string, payload any, target any) error {
	if s == nil || s.Endpoint == "" {
		return errors.New("agnt5: sandbox endpoint is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.APIKey)
	}
	client := s.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return errors.New("agnt5: sandbox returned HTTP " + intString(resp.StatusCode))
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

// InMemorySandbox is a deterministic sandbox for tests and local examples.
type InMemorySandbox struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func NewInMemorySandbox() *InMemorySandbox {
	return &InMemorySandbox{files: make(map[string][]byte)}
}

func (s *InMemorySandbox) ExecuteCode(ctx context.Context, language, code string) (ExecuteCodeResult, error) {
	emitSandboxEvent(ctx, "sandbox.execute.started", map[string]any{"language": language})
	emitSandboxResult(ctx, "sandbox.execute", 0, nil)
	return ExecuteCodeResult{Stdout: "[" + language + "] " + code, ExitCode: 0}, nil
}

func (s *InMemorySandbox) RunCommand(ctx context.Context, command []string) (RunCommandResult, error) {
	emitSandboxEvent(ctx, "sandbox.command.started", map[string]any{"command": command})
	emitSandboxResult(ctx, "sandbox.command", 0, nil)
	return RunCommandResult{Stdout: strings.Join(command, " "), ExitCode: 0}, nil
}

func (s *InMemorySandbox) WriteFile(ctx context.Context, path string, content []byte) (WriteFileResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.write.started", map[string]any{"path": path, "bytes": len(content)})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files == nil {
		s.files = make(map[string][]byte)
	}
	s.files[path] = append([]byte(nil), content...)
	emitSandboxResult(ctx, "sandbox.file.write", 0, nil)
	return WriteFileResult{Path: path, Bytes: len(content)}, nil
}

func (s *InMemorySandbox) ReadFile(ctx context.Context, path string) (ReadFileResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.read.started", map[string]any{"path": path})
	s.mu.RLock()
	defer s.mu.RUnlock()
	content, ok := s.files[path]
	if !ok {
		err := errors.New("agnt5: sandbox file not found")
		emitSandboxResult(ctx, "sandbox.file.read", 0, err)
		return ReadFileResult{}, err
	}
	emitSandboxResult(ctx, "sandbox.file.read", 0, nil)
	return ReadFileResult{Path: path, Content: append([]byte(nil), content...)}, nil
}

func (s *InMemorySandbox) ListFiles(ctx context.Context, path string) (ListFilesResult, error) {
	emitSandboxEvent(ctx, "sandbox.file.list.started", map[string]any{"path": path})
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := make([]FileInfo, 0, len(s.files))
	for filePath, content := range s.files {
		if path == "" || strings.HasPrefix(filePath, path) {
			files = append(files, FileInfo{Path: filePath, Size: int64(len(content))})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	emitSandboxResult(ctx, "sandbox.file.list", 0, nil)
	return ListFilesResult{Path: path, Files: files}, nil
}

func (s *InMemorySandbox) DeleteFile(ctx context.Context, path string, recursive bool) (bool, error) {
	emitSandboxEvent(ctx, "sandbox.file.delete.started", map[string]any{"path": path, "recursive": recursive})
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := false
	if recursive {
		prefix := strings.TrimSuffix(path, "/") + "/"
		for fileName := range s.files {
			if fileName == path || strings.HasPrefix(fileName, prefix) {
				delete(s.files, fileName)
				deleted = true
			}
		}
	} else if _, ok := s.files[path]; ok {
		delete(s.files, path)
		deleted = true
	}
	emitSandboxResult(ctx, "sandbox.file.delete", 0, nil)
	return deleted, nil
}

func emitSandboxEvent(ctx context.Context, eventType string, data map[string]any) {
	if runCtx, ok := ctx.(*Context); ok && runCtx != nil {
		_ = runCtx.Emit(Event{Type: eventType, Data: data})
	}
}

func emitSandboxResult(ctx context.Context, prefix string, exitCode int, err error) {
	data := map[string]any{"exit_code": exitCode}
	eventType := prefix + ".completed"
	if err != nil {
		eventType = prefix + ".failed"
		data["error"] = err.Error()
	}
	emitSandboxEvent(ctx, eventType, data)
}
