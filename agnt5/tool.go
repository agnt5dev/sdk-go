package agnt5

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

// ToolHandler executes a tool call.
type ToolHandler func(context.Context, map[string]any) (any, error)

// Tool describes a callable tool.
type Tool struct {
	Name                     string         `json:"name"`
	Description              string         `json:"description,omitempty"`
	Schema                   map[string]any `json:"schema,omitempty"`
	Metadata                 map[string]any `json:"metadata,omitempty"`
	RecoveryPolicy           RecoveryPolicy `json:"recovery_policy,omitempty"`
	DisableDurableActivation bool           `json:"-"`
	Handler                  ToolHandler    `json:"-"`
}

// ToolOption mutates Tool construction.
type ToolOption func(*Tool)

func WithToolDescription(description string) ToolOption {
	return func(t *Tool) { t.Description = description }
}

func WithToolSchema(schema map[string]any) ToolOption {
	return func(t *Tool) { t.Schema = cloneAnyMap(schema) }
}

func WithToolMetadata(metadata map[string]any) ToolOption {
	return func(t *Tool) { t.Metadata = cloneAnyMap(metadata) }
}

// WithToolRecoveryPolicy selects how interrupted calls are recovered.
func WithToolRecoveryPolicy(policy RecoveryPolicy) ToolOption {
	return func(t *Tool) { t.RecoveryPolicy = policy }
}

// WithoutDurableToolActivation preserves suspension-native or legacy tool behavior.
func WithoutDurableToolActivation() ToolOption {
	return func(t *Tool) { t.DisableDurableActivation = true }
}

// NewTool creates a Tool.
func NewTool(name string, handler ToolHandler, opts ...ToolOption) (Tool, error) {
	if strings.TrimSpace(name) == "" {
		return Tool{}, ErrInvalidComponentName
	}
	if handler == nil {
		return Tool{}, ErrNilHandler
	}
	tool := Tool{
		Name:           name,
		Handler:        handler,
		Schema:         map[string]any{},
		Metadata:       map[string]any{},
		RecoveryPolicy: RecoveryPolicyUnknownOutcome,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&tool)
		}
	}
	if _, err := recoveryPolicyProto(tool.RecoveryPolicy); err != nil {
		return Tool{}, err
	}
	return tool, nil
}

// ToolRegistry stores tools by name.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewToolRegistry creates an empty tool registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

var defaultToolRegistry = NewToolRegistry()

// DefaultToolRegistry returns the process-global tool registry.
func DefaultToolRegistry() *ToolRegistry {
	return defaultToolRegistry
}

func (r *ToolRegistry) Register(tool Tool) error {
	if strings.TrimSpace(tool.Name) == "" {
		return ErrInvalidComponentName
	}
	if tool.Handler == nil {
		return ErrNilHandler
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[tool.Name]; exists {
		return ErrDuplicateComponent
	}
	tool.Schema = cloneAnyMap(tool.Schema)
	tool.Metadata = cloneAnyMap(tool.Metadata)
	r.tools[tool.Name] = tool
	return nil
}

func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[name]
	if !ok {
		return Tool{}, false
	}
	tool.Schema = cloneAnyMap(tool.Schema)
	tool.Metadata = cloneAnyMap(tool.Metadata)
	return tool, true
}

func (r *ToolRegistry) List() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Tool, 0, len(names))
	for _, name := range names {
		tool := r.tools[name]
		tool.Schema = cloneAnyMap(tool.Schema)
		tool.Metadata = cloneAnyMap(tool.Metadata)
		out = append(out, tool)
	}
	return out
}

func (r *ToolRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools = make(map[string]Tool)
}

// CallTool executes a tool from this registry.
func (r *ToolRegistry) CallTool(ctx context.Context, name string, input map[string]any) (any, error) {
	tool, ok := r.Get(name)
	if !ok {
		return nil, errors.New("agnt5: tool not found: " + name)
	}
	return tool.Handler(ctx, input)
}

func invokeAgentTool(ctx *Context, tool Tool, stableKey string, input map[string]any) (any, error) {
	if tool.DisableDurableActivation || ctx.Metadata(durableActivationV1Capability) != "true" {
		return tool.Handler(ctx, input)
	}
	if ctx.activationWriter == nil {
		return nil, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"runtime negotiated durable_activation_v1 but no activation writer is configured",
			"",
			0,
			nil,
		)
	}
	policy := tool.RecoveryPolicy
	if policy == "" {
		policy = RecoveryPolicyUnknownOutcome
	}
	protoPolicy, err := recoveryPolicyProto(policy)
	if err != nil {
		return nil, err
	}
	canonicalInput, err := canonicalActivationValue(map[string]any{
		"name":      tool.Name,
		"arguments": input,
	})
	if err != nil {
		return nil, err
	}
	definitionDigest, err := activationDefinitionDigestFromContext(ctx)
	if err != nil {
		return nil, err
	}
	inputDigest := sha256.Sum256(canonicalInput)
	workerSessionID := ctx.Metadata("worker_session_id")
	if workerSessionID == "" {
		workerSessionID = ctx.Metadata("worker_id")
	}
	runAuthority := ctx.Metadata("run_authority")
	if runAuthority == "" {
		runAuthority = ctx.InvocationID()
	}
	leaseAuthority := ctx.Metadata("lease_authority")
	if leaseAuthority == "" {
		leaseAuthority = ctx.LeaseID()
	}
	if ctx.projectID == "" || ctx.RunID() == "" || workerSessionID == "" || runAuthority == "" || leaseAuthority == "" {
		return nil, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"durable tool activation requires project, run, worker-session, run, and lease authority",
			"",
			0,
			nil,
		)
	}
	logicalKey := "tool:" + tool.Name + ":" + stableKey
	parentActivationID := ctx.Metadata("parent_activation_id")
	expectedActivationID := activationID(
		ctx.projectID,
		ctx.RunID(),
		parentActivationID,
		pb.ActivationKind_ACTIVATION_KIND_TOOL,
		logicalKey,
	)
	begin, err := ctx.activationWriter.BeginActivation(ctx, &pb.BeginActivationRequest{
		ProjectId:          ctx.projectID,
		RunId:              ctx.RunID(),
		ParentActivationId: parentActivationID,
		Kind:               pb.ActivationKind_ACTIVATION_KIND_TOOL,
		StableKey:          logicalKey,
		InputDigest:        inputDigest[:],
		DefinitionDigest:   cloneBytes(definitionDigest),
		RecoveryPolicy:     protoPolicy,
		WorkerSessionId:    workerSessionID,
		RunAuthority:       []byte(runAuthority),
		LeaseAuthority:     []byte(leaseAuthority),
	})
	if err != nil {
		return nil, err
	}
	if begin.GetActivationId() != expectedActivationID {
		return nil, newActivationError(
			ActivationErrorUnknownOutcome,
			fmt.Sprintf("runtime returned activation ID %q, want %q", begin.GetActivationId(), expectedActivationID),
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	if begin.GetOutcome() == pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY {
		payload, payloadErr := inlineActivationBytes(begin.GetReplayResult())
		if payloadErr != nil {
			return nil, payloadErr
		}
		var replayed any
		if err := json.Unmarshal(payload, &replayed); err != nil {
			return nil, fmt.Errorf("agnt5: decode replayed tool %q: %w", tool.Name, err)
		}
		return replayed, nil
	}
	if begin.GetOutcome() != pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE {
		return nil, activationBeginDecisionError(begin, "tool")
	}
	if begin.GetAttempt() == 0 || len(begin.GetFenceToken()) == 0 {
		return nil, newActivationError(
			ActivationErrorUnknownOutcome,
			"EXECUTE receipt is missing fenced authority",
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	execution := ActivationExecution{
		ActivationID:   begin.GetActivationId(),
		Attempt:        begin.GetAttempt(),
		IdempotencyKey: "agnt5:" + begin.GetActivationId(),
	}
	startedAt := time.Now()
	output, userErr := tool.Handler(ctx.withActivationExecution(execution), cloneAnyMap(input))
	if userErr != nil {
		errorData, _ := json.Marshal(map[string]string{
			"message": userErr.Error(),
			"type":    fmt.Sprintf("%T", userErr),
		})
		retryable := policy == RecoveryPolicyIdempotentRetry || policy == RecoveryPolicyDurableSteps
		failed, failErr := ctx.activationWriter.FailActivation(ctx, &pb.FailActivationRequest{
			ProjectId:                ctx.projectID,
			RunId:                    ctx.RunID(),
			ActivationId:             begin.GetActivationId(),
			Attempt:                  begin.GetAttempt(),
			FenceToken:               cloneBytes(begin.GetFenceToken()),
			ErrorCode:                "TOOL_FAILED",
			ErrorData:                inlineActivationPayload(errorData),
			Retryable:                retryable,
			ExternalOutcomeCertainty: pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN,
		})
		if failErr != nil {
			return nil, fmt.Errorf("agnt5: record failure for tool %q: %w (tool error: %v)", tool.Name, failErr, userErr)
		}
		if !failed.GetAccepted() || failed.GetActivationId() != begin.GetActivationId() || failed.GetAttempt() != begin.GetAttempt() {
			return nil, newActivationError(
				ActivationErrorUnknownOutcome,
				"runtime returned an invalid tool failure receipt",
				begin.GetActivationId(),
				begin.GetAttempt(),
				userErr,
			)
		}
		return nil, userErr
	}
	payload, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("agnt5: encode tool %q output: %w", tool.Name, err)
	}
	outputDigest := sha256.Sum256(payload)
	completed, err := ctx.activationWriter.CompleteActivation(ctx, &pb.CompleteActivationRequest{
		ProjectId:    ctx.projectID,
		RunId:        ctx.RunID(),
		ActivationId: begin.GetActivationId(),
		Attempt:      begin.GetAttempt(),
		FenceToken:   cloneBytes(begin.GetFenceToken()),
		Output:       inlineActivationPayload(payload),
		OutputDigest: outputDigest[:],
		Usage:        &pb.ActivationUsage{LatencyMs: time.Since(startedAt).Milliseconds()},
	})
	if err != nil {
		return nil, err
	}
	if !completed.GetAccepted() || completed.GetActivationId() != begin.GetActivationId() || completed.GetAttempt() != begin.GetAttempt() {
		return nil, newActivationError(
			ActivationErrorUnknownOutcome,
			"runtime returned an invalid tool completion receipt",
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	return output, nil
}

// RegisterTool registers a Tool both in the global tool registry and on a worker.
func RegisterTool(w *Worker, tool Tool, opts ...ComponentOption) error {
	if err := DefaultToolRegistry().Register(tool); err != nil {
		return err
	}
	componentOpts := make([]ComponentOption, 0, len(opts)+1)
	if len(tool.Schema) > 0 {
		componentOpts = append(componentOpts, WithInputSchema(tool.Schema))
	}
	componentOpts = append(componentOpts, opts...)
	return RegisterRaw(w, tool.Name, ComponentTypeTool, func(ctx *Context, input []byte) ([]byte, error) {
		var args map[string]any
		if len(input) > 0 {
			if err := json.Unmarshal(input, &args); err != nil {
				return nil, err
			}
		}
		out, err := tool.Handler(ctx, args)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, componentOpts...)
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
