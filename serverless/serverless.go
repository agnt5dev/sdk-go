// Package serverless exposes AGNT5 workflows as signed HTTP endpoints without
// running a persistent worker process.
package serverless

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ProtocolVersion  = "workerless.v1"
	SignatureVersion = "workerless-hmac-sha256.v1"
	ManifestPath     = "/.well-known/agnt5"
	InvokePath       = "/agnt5/invoke"
	maxBodyBytes     = 8 << 20
	maxPayloadBytes  = 64 << 20
	maxSignatureSkew = 5 * time.Minute
)

type Options struct {
	ServiceName    string
	ServiceVersion string
	SigningSecret  func(*http.Request) string
	Enabled        func(*http.Request) bool
	HTTPClient     *http.Client
}

type Handler struct {
	opts       Options
	mu         sync.RWMutex
	components map[string]component
}

type component struct {
	name     string
	kind     string
	invoke   func(*Context, json.RawMessage) (any, error)
	metadata map[string]any
}

type manifest struct {
	ProtocolVersion string              `json:"protocol_version"`
	ServiceName     string              `json:"service_name,omitempty"`
	ServiceVersion  string              `json:"service_version,omitempty"`
	Components      []manifestComponent `json:"components"`
}

type manifestComponent struct {
	Name          string         `json:"name"`
	Type          string         `json:"type"`
	ComponentType string         `json:"component_type"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type invokePayload struct {
	ProtocolVersion string             `json:"protocol_version"`
	RunID           string             `json:"run_id"`
	ComponentType   string             `json:"component_type"`
	ComponentName   string             `json:"component_name"`
	Attempt         int                `json:"attempt"`
	Input           json.RawMessage    `json:"input"`
	InputRef        *PayloadRef        `json:"input_ref"`
	OutputUpload    *OutputUpload      `json:"output_upload"`
	Metadata        map[string]string  `json:"metadata"`
	Checkpoint      checkpointEnvelope `json:"checkpoint"`
	Budget          budget             `json:"budget"`
}

type checkpointEnvelope struct {
	Steps         map[string]json.RawMessage        `json:"steps,omitempty"`
	AgentSessions map[string]AgentSessionCheckpoint `json:"agent_sessions,omitempty"`
}

type budget struct {
	DeadlineMS           int64 `json:"deadline_ms"`
	YieldBeforeTimeoutMS int64 `json:"yield_before_timeout_ms"`
}

type Event struct {
	EventType string            `json:"event_type"`
	Data      any               `json:"data,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp int64             `json:"timestamp_ns,omitempty"`
}

type Context struct {
	context.Context
	RunID         string
	Attempt       int
	ComponentName string
	Metadata      map[string]string
	deadline      time.Time
	yieldMargin   time.Duration
	steps         map[string]json.RawMessage
	events        []Event
	agentSessions map[string]AgentSessionCheckpoint
	pauseIndex    int
}

type Suspension struct {
	Reason      string
	ReadyAtMS   int64
	TimerKey    string
	SignalName  string
	WaitingStep string
	PauseIndex  int
	StepName    string
	Question    string
	InputType   string
	Options     []UserInputOption
	AllowCustom bool
	Skippable   bool
}

func (s *Suspension) Error() string { return "serverless workflow suspended: " + s.Reason }

func New(opts Options) *Handler {
	return &Handler{opts: opts, components: make(map[string]component)}
}

func RegisterWorkflow[In any, Out any](h *Handler, name string, fn func(*Context, In) (Out, error)) error {
	return registerTyped(h, "workflow", name, fn)
}

func registerTyped[In any, Out any](h *Handler, kind, name string, fn func(*Context, In) (Out, error)) error {
	if h == nil || fn == nil {
		return errors.New("serverless handler and component are required")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("serverless component name is required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	key := kind + ":" + name
	if _, exists := h.components[key]; exists {
		return fmt.Errorf("serverless %s %q is already registered", kind, name)
	}
	h.components[key] = component{name: name, kind: kind, invoke: func(ctx *Context, raw json.RawMessage) (any, error) {
		var input In
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, fmt.Errorf("decode workflow input: %w", err)
			}
		}
		return fn(ctx, input)
	}}
	return nil
}

func Step[T any](ctx *Context, name string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if ctx == nil || fn == nil || strings.TrimSpace(name) == "" {
		return zero, errors.New("serverless step requires a context, name, and function")
	}
	key := "step:" + strings.TrimSpace(name)
	if raw, ok := ctx.steps[key]; ok {
		if err := json.Unmarshal(raw, &zero); err != nil {
			return zero, fmt.Errorf("decode checkpoint %q: %w", key, err)
		}
		return zero, nil
	}
	value, err := fn(ctx)
	if err != nil {
		return zero, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("encode checkpoint %q: %w", key, err)
	}
	ctx.steps[key] = raw
	return value, nil
}

func (c *Context) YieldIfNeeded() error {
	if c == nil || c.deadline.IsZero() || time.Now().Add(c.yieldMargin).Before(c.deadline) {
		return nil
	}
	return &Suspension{Reason: "budget"}
}

func (c *Context) Sleep(duration time.Duration, name string) error {
	if duration < 0 {
		return errors.New("serverless sleep duration must be non-negative")
	}
	if duration == 0 {
		return nil
	}
	if strings.TrimSpace(name) == "" {
		name = "sleep_" + strconv.FormatInt(duration.Milliseconds(), 10) + "ms"
	}
	started, err := Step(c, name, func(context.Context) (int64, error) { return time.Now().UnixMilli(), nil })
	if err != nil {
		return err
	}
	ready := started + duration.Milliseconds()
	if time.Now().UnixMilli() >= ready {
		return nil
	}
	return &Suspension{Reason: "timer", ReadyAtMS: ready, TimerKey: name}
}

func (c *Context) Emit(event Event) {
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}
	c.events = append(c.events, event)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.opts.Enabled != nil && !h.opts.Enabled(r) {
		writeFailure(w, http.StatusServiceUnavailable, "WORKERLESS_DISABLED", "AGNT5 serverless endpoint is disabled")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == ManifestPath:
		h.serveManifest(w)
	case r.Method == http.MethodPost && r.URL.Path == InvokePath:
		h.serveInvoke(w, r)
	default:
		writeFailure(w, http.StatusNotFound, "WORKERLESS_NOT_FOUND", "AGNT5 serverless route not found")
	}
}

func (h *Handler) serveManifest(w http.ResponseWriter) {
	h.mu.RLock()
	components := make([]manifestComponent, 0, len(h.components))
	for _, item := range h.components {
		components = append(components, manifestComponent{Name: item.name, Type: item.kind, ComponentType: item.kind, Metadata: item.metadata})
	}
	h.mu.RUnlock()
	sort.Slice(components, func(i, j int) bool {
		if components[i].ComponentType == components[j].ComponentType {
			return components[i].Name < components[j].Name
		}
		return components[i].ComponentType < components[j].ComponentType
	})
	writeJSON(w, http.StatusOK, manifest{ProtocolVersion: ProtocolVersion, ServiceName: h.opts.ServiceName, ServiceVersion: h.opts.ServiceVersion, Components: components})
}

func (h *Handler) serveInvoke(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeFailure(w, http.StatusBadRequest, "WORKERLESS_INVALID_REQUEST", "request body could not be read")
		return
	}
	if signatureErr := h.verifySignature(r, body); signatureErr != nil {
		writeFailure(w, signatureErr.Status, signatureErr.Code, signatureErr.Message)
		return
	}
	var payload invokePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		writeFailure(w, http.StatusBadRequest, "WORKERLESS_INVALID_REQUEST", "request body must be JSON")
		return
	}
	if payload.ProtocolVersion != "" && payload.ProtocolVersion != ProtocolVersion {
		writeFailure(w, http.StatusBadRequest, "WORKERLESS_PROTOCOL_MISMATCH", "unsupported protocol_version")
		return
	}
	input, protocolErr := h.resolveInput(r.Context(), payload.Input, payload.InputRef)
	if protocolErr != nil {
		writeFailure(w, protocolErr.Status, protocolErr.Code, protocolErr.Message)
		return
	}
	h.mu.RLock()
	item, ok := h.components[payload.ComponentType+":"+payload.ComponentName]
	h.mu.RUnlock()
	if !ok {
		writeFailure(w, http.StatusNotFound, "WORKERLESS_COMPONENT_NOT_FOUND", "serverless component was not found")
		return
	}
	ctx := &Context{Context: r.Context(), RunID: payload.RunID, Attempt: payload.Attempt, ComponentName: payload.ComponentName, Metadata: payload.Metadata, steps: payload.Checkpoint.Steps, agentSessions: payload.Checkpoint.AgentSessions}
	if ctx.steps == nil {
		ctx.steps = make(map[string]json.RawMessage)
	}
	if ctx.agentSessions == nil {
		ctx.agentSessions = make(map[string]AgentSessionCheckpoint)
	}
	if payload.Budget.DeadlineMS > 0 {
		ctx.deadline = time.UnixMilli(payload.Budget.DeadlineMS)
	}
	ctx.yieldMargin = time.Duration(payload.Budget.YieldBeforeTimeoutMS) * time.Millisecond
	output, invokeErr := item.invoke(ctx, input)
	checkpoint := checkpointEnvelope{Steps: ctx.steps, AgentSessions: ctx.agentSessions}
	var suspension *Suspension
	if errors.As(invokeErr, &suspension) {
		response := map[string]any{"status": "suspended", "reason": suspension.Reason, "checkpoint": checkpoint, "events": ctx.events}
		if suspension.ReadyAtMS > 0 {
			response["ready_at_ms"] = suspension.ReadyAtMS
		}
		if suspension.TimerKey != "" {
			response["timer_key"] = suspension.TimerKey
		}
		if suspension.SignalName != "" {
			response["signal_name"] = suspension.SignalName
		}
		if suspension.WaitingStep != "" {
			response["waiting_step"] = suspension.WaitingStep
		}
		if suspension.Reason == "user_input_required" {
			response["pause_index"] = suspension.PauseIndex
			response["step_name"] = suspension.StepName
			response["question"] = suspension.Question
			response["input_type"] = suspension.InputType
			response["options"] = suspension.Options
			response["allow_custom"] = suspension.AllowCustom
			response["skippable"] = suspension.Skippable
		}
		writeJSON(w, http.StatusOK, response)
		return
	}
	if invokeErr != nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "failed", "error": map[string]any{"code": "WORKERLESS_HANDLER_ERROR", "message": invokeErr.Error()}, "events": ctx.events})
		return
	}
	completion, protocolErr := h.completeOutput(r.Context(), output, payload.OutputUpload, checkpoint, ctx.events)
	if protocolErr != nil {
		writeFailure(w, protocolErr.Status, protocolErr.Code, protocolErr.Message)
		return
	}
	writeJSON(w, http.StatusOK, completion)
}

func (h *Handler) verifySignature(r *http.Request, body []byte) *protocolError {
	if h.opts.SigningSecret == nil {
		return nil
	}
	secret := h.opts.SigningSecret(r)
	if secret == "" {
		return nil
	}
	timestamp, attemptID := r.Header.Get("X-AGNT5-Timestamp"), r.Header.Get("X-AGNT5-Attempt-ID")
	signature := r.Header.Get("X-AGNT5-Signature")
	if timestamp == "" || attemptID == "" || signature == "" {
		return perr(http.StatusUnauthorized, "WORKERLESS_SIGNATURE_MISSING", "workerless invoke signature headers are required")
	}
	if version := r.Header.Get("X-AGNT5-Signature-Version"); version != SignatureVersion {
		return perr(http.StatusUnauthorized, "WORKERLESS_SIGNATURE_VERSION_UNSUPPORTED", "workerless invoke signature version is unsupported")
	}
	timestampMS, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return perr(http.StatusUnauthorized, "WORKERLESS_SIGNATURE_TIMESTAMP_INVALID", "workerless invoke signature timestamp is invalid")
	}
	if time.Since(time.UnixMilli(timestampMS)).Abs() > maxSignatureSkew {
		return perr(http.StatusUnauthorized, "WORKERLESS_SIGNATURE_EXPIRED", "workerless invoke signature timestamp is outside the allowed window")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + attemptID + "."))
	_, _ = mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return perr(http.StatusUnauthorized, "WORKERLESS_SIGNATURE_INVALID", "workerless invoke signature is invalid")
	}
	return nil
}

func writeFailure(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"status": "failed", "error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
