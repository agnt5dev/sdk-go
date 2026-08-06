package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"time"
)

// BatchStatus is the gateway-visible lifecycle status for a batch.
type BatchStatus string

const (
	BatchStatusPending        BatchStatus = "pending"
	BatchStatusQueued         BatchStatus = "queued"
	BatchStatusStarted        BatchStatus = "started"
	BatchStatusRunning        BatchStatus = "running"
	BatchStatusCompleted      BatchStatus = "completed"
	BatchStatusPartialFailure BatchStatus = "partial_failure"
	BatchStatusFailed         BatchStatus = "failed"
	BatchStatusCancelled      BatchStatus = "cancelled"
	BatchStatusUnknown        BatchStatus = "unknown"
)

// BatchItemInput describes one batch item with optional per-item metadata.
type BatchItemInput struct {
	Input     any               `json:"input"`
	Index     *int              `json:"index,omitempty"`
	ItemID    string            `json:"item_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
}

// NewBatchItem constructs one Python-compatible batch item envelope.
func NewBatchItem(input any, opts ...BatchItemOption) BatchItemInput {
	item := BatchItemInput{Input: input}
	for _, opt := range opts {
		if opt != nil {
			opt(&item)
		}
	}
	return item
}

// BatchItemOption mutates a BatchItemInput.
type BatchItemOption func(*BatchItemInput)

// WithBatchItemIndex sets the item index.
func WithBatchItemIndex(index int) BatchItemOption {
	return func(item *BatchItemInput) {
		item.Index = &index
	}
}

// WithBatchItemID sets the item identifier.
func WithBatchItemID(itemID string) BatchItemOption {
	return func(item *BatchItemInput) {
		item.ItemID = itemID
	}
}

// WithBatchItemMetadata sets per-item metadata.
func WithBatchItemMetadata(metadata map[string]string) BatchItemOption {
	return func(item *BatchItemInput) {
		item.Metadata = cloneStringMap(metadata)
	}
}

// WithBatchItemTimeoutMS sets a per-item timeout.
func WithBatchItemTimeoutMS(timeoutMS int64) BatchItemOption {
	return func(item *BatchItemInput) {
		if timeoutMS > 0 {
			item.TimeoutMS = timeoutMS
		}
	}
}

type batchConfig struct {
	componentType        ComponentType
	maxConcurrency       int
	continueOnFailure    bool
	batchTimeoutMS       int64
	defaultItemTimeoutMS int64
	metadata             map[string]string
	tenant               string
	timeout              time.Duration
	rawItems             bool
	idempotencyKey       *string
}

// BatchOption mutates Batch request configuration.
type BatchOption func(*batchConfig)

// WithBatchComponentType sets the component kind for Batch.
func WithBatchComponentType(componentType ComponentType) BatchOption {
	return func(config *batchConfig) {
		if componentType != "" {
			config.componentType = componentType
		}
	}
}

// WithBatchMaxConcurrency sets the advertised batch concurrency.
func WithBatchMaxConcurrency(maxConcurrency int) BatchOption {
	return func(config *batchConfig) {
		if maxConcurrency > 0 {
			config.maxConcurrency = maxConcurrency
		}
	}
}

// WithBatchContinueOnFailure controls whether the batch should continue after item failure.
func WithBatchContinueOnFailure(continueOnFailure bool) BatchOption {
	return func(config *batchConfig) {
		config.continueOnFailure = continueOnFailure
	}
}

// WithBatchTimeoutMS sets the advertised whole-batch timeout.
func WithBatchTimeoutMS(timeoutMS int64) BatchOption {
	return func(config *batchConfig) {
		if timeoutMS > 0 {
			config.batchTimeoutMS = timeoutMS
		}
	}
}

// WithBatchDefaultItemTimeoutMS sets the advertised default per-item timeout.
func WithBatchDefaultItemTimeoutMS(timeoutMS int64) BatchOption {
	return func(config *batchConfig) {
		if timeoutMS > 0 {
			config.defaultItemTimeoutMS = timeoutMS
		}
	}
}

// WithBatchMetadata sets batch-level metadata.
func WithBatchMetadata(metadata map[string]string) BatchOption {
	return func(config *batchConfig) {
		if config.metadata == nil {
			config.metadata = make(map[string]string, len(metadata))
		}
		for key, value := range metadata {
			config.metadata[key] = value
		}
	}
}

// WithBatchTenant overrides the default X-TENANT-ID for this request.
func WithBatchTenant(tenantID string) BatchOption {
	return func(config *batchConfig) {
		config.tenant = tenantID
	}
}

// WithBatchHTTPTimeout sets a per-request HTTP timeout.
func WithBatchHTTPTimeout(timeout time.Duration) BatchOption {
	return func(config *batchConfig) {
		if timeout > 0 {
			config.timeout = timeout
		}
	}
}

// WithBatchRawItems sends each item as the component input directly. Without
// this option, plain inputs are wrapped in Python-compatible BatchItemInput
// envelopes containing input and index fields.
func WithBatchRawItems() BatchOption {
	return func(config *batchConfig) {
		config.rawItems = true
	}
}

// WithBatchIdempotencyKey sets the stable caller key used to deduplicate a
// Batch or BatchStream admission.
func WithBatchIdempotencyKey(key string) BatchOption {
	return func(config *batchConfig) {
		config.idempotencyKey = &key
	}
}

// BatchItemError is per-item failure information in a batch response.
type BatchItemError struct {
	Code    string         `json:"code,omitempty"`
	Message string         `json:"message,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// BatchItemResult is one item result in a batch response.
type BatchItemResult struct {
	Index       int             `json:"index"`
	ItemID      string          `json:"item_id,omitempty"`
	RunID       string          `json:"run_id"`
	Status      RunStatus       `json:"status"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       *BatchItemError `json:"error,omitempty"`
	DurationMS  *int64          `json:"duration_ms,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Raw         map[string]any  `json:"-"`
}

// IsSuccess reports whether this batch item completed successfully.
func (r BatchItemResult) IsSuccess() bool {
	return r.Status == RunStatusCompleted
}

// IsFailed reports whether this batch item failed.
func (r BatchItemResult) IsFailed() bool {
	return r.Status == RunStatusFailed
}

// DecodeOutput unmarshals the item output into target.
func (r BatchItemResult) DecodeOutput(target any) error {
	if len(r.Output) == 0 || string(r.Output) == "null" {
		return errors.New("agnt5: batch item has no output")
	}
	return json.Unmarshal(r.Output, target)
}

// BatchStats summarizes batch item states and duration.
type BatchStats struct {
	TotalItems        int   `json:"total_items"`
	CompletedItems    int   `json:"completed_items"`
	FailedItems       int   `json:"failed_items"`
	CancelledItems    int   `json:"cancelled_items"`
	PendingItems      int   `json:"pending_items"`
	DurationMS        int64 `json:"duration_ms"`
	AvgItemDurationMS int64 `json:"avg_item_duration_ms"`
}

// BatchResult is returned by Batch.
type BatchResult struct {
	BatchID    string            `json:"batch_id"`
	Status     BatchStatus       `json:"status"`
	RunIDs     []string          `json:"run_ids,omitempty"`
	TotalItems int               `json:"total_items,omitempty"`
	Results    []BatchItemResult `json:"results,omitempty"`
	Stats      *BatchStats       `json:"stats,omitempty"`
	TraceID    string            `json:"trace_id,omitempty"`
	CreatedAt  *time.Time        `json:"created_at,omitempty"`
	Raw        map[string]any    `json:"-"`
}

// IsSuccess reports whether all batch items completed successfully.
func (r *BatchResult) IsSuccess() bool {
	return r != nil && r.Status == BatchStatusCompleted
}

// IsPartialFailure reports whether some batch items failed and some succeeded.
func (r *BatchResult) IsPartialFailure() bool {
	return r != nil && r.Status == BatchStatusPartialFailure
}

// Outputs returns item outputs sorted by index. Failed items return nil.
func (r *BatchResult) Outputs() []json.RawMessage {
	if r == nil {
		return nil
	}
	results := append([]BatchItemResult(nil), r.Results...)
	sort.SliceStable(results, func(i, j int) bool { return results[i].Index < results[j].Index })
	outputs := make([]json.RawMessage, len(results))
	for i, result := range results {
		outputs[i] = result.Output
	}
	return outputs
}

// SuccessfulOutputs returns successful item outputs sorted by index.
func (r *BatchResult) SuccessfulOutputs() []json.RawMessage {
	if r == nil {
		return nil
	}
	results := append([]BatchItemResult(nil), r.Results...)
	sort.SliceStable(results, func(i, j int) bool { return results[i].Index < results[j].Index })
	outputs := make([]json.RawMessage, 0, len(results))
	for _, result := range results {
		if result.IsSuccess() {
			outputs = append(outputs, result.Output)
		}
	}
	return outputs
}

// FailedItems returns failed item results.
func (r *BatchResult) FailedItems() []BatchItemResult {
	if r == nil {
		return nil
	}
	failed := make([]BatchItemResult, 0)
	for _, result := range r.Results {
		if result.IsFailed() {
			failed = append(failed, result)
		}
	}
	return failed
}

// BatchStatusResponse is returned by GetBatchStatus.
type BatchStatusResponse struct {
	BatchID     string            `json:"batch_id"`
	Found       bool              `json:"found,omitempty"`
	Status      BatchStatus       `json:"status"`
	Results     []BatchItemResult `json:"results,omitempty"`
	Stats       *BatchStats       `json:"stats,omitempty"`
	TraceID     string            `json:"trace_id,omitempty"`
	SubmittedAt *time.Time        `json:"submitted_at,omitempty"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Raw         map[string]any    `json:"-"`
}

// BatchStreamEvent is one SSE event from BatchStream.
type BatchStreamEvent struct {
	EventType string          `json:"event_type"`
	BatchID   string          `json:"batch_id,omitempty"`
	RunID     string          `json:"run_id,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
	Raw       map[string]any  `json:"-"`
}

// IsRunning reports whether the batch is still running.
func (r *BatchStatusResponse) IsRunning() bool {
	if r == nil {
		return false
	}
	return r.Status == BatchStatusPending || r.Status == BatchStatusQueued ||
		r.Status == BatchStatusStarted || r.Status == BatchStatusRunning
}

// IsCompleted reports whether the batch reached a terminal status.
func (r *BatchStatusResponse) IsCompleted() bool {
	if r == nil {
		return false
	}
	return r.Status == BatchStatusCompleted || r.Status == BatchStatusPartialFailure ||
		r.Status == BatchStatusFailed || r.Status == BatchStatusCancelled
}

// CancelBatchResponse is returned by CancelBatch.
type CancelBatchResponse struct {
	BatchID        string         `json:"batch_id"`
	Status         BatchStatus    `json:"status"`
	CancelledItems int            `json:"cancelled_items,omitempty"`
	CompletedItems int            `json:"completed_items,omitempty"`
	Raw            map[string]any `json:"-"`
}

// Batch submits a batch of component invocations through /v1/{type}/{component}/batch.
func (c *Client) Batch(ctx context.Context, component string, items any, opts ...BatchOption) (*BatchResult, error) {
	config := newBatchConfig(opts...)
	normalized, err := normalizeBatchItems(items, config.rawItems)
	if err != nil {
		return nil, err
	}
	headers := c.requestHeaders("", "", config.tenant, nil)
	if config.idempotencyKey != nil {
		headers.Set("Idempotency-Key", *config.idempotencyKey)
	}
	statusCode, responseBody, endpoint, err := c.doJSON(ctx, http.MethodPost, []string{
		"v1", componentCollection(config.componentType), component, "batch",
	}, batchRequestBody(config, normalized), headers, config.timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodPost, URL: endpoint, StatusCode: statusCode, Body: string(responseBody)}
	}
	return parseBatchResult(responseBody)
}

// BatchStream submits a batch and streams batch/run events until a terminal
// batch event is delivered.
func (c *Client) BatchStream(ctx context.Context, component string, items any, handle func(BatchStreamEvent) error, opts ...BatchOption) error {
	if handle == nil {
		return errors.New("agnt5: nil batch stream handler")
	}
	config := newBatchConfig(opts...)
	normalized, err := normalizeBatchItems(items, config.rawItems)
	if err != nil {
		return err
	}
	headers := c.requestHeaders("", "", config.tenant, nil)
	if config.idempotencyKey != nil {
		headers.Set("Idempotency-Key", *config.idempotencyKey)
	}
	headers.Set("Accept", "text/event-stream")
	statusCode, responseBody, err := c.doStream(ctx, []string{
		"v1", componentCollection(config.componentType), component, "batch", "stream",
	}, batchRequestBody(config, normalized), headers, config.timeout, func(event ReceivedEvent) error {
		batchEvent := parseBatchStreamEvent(event)
		if err := handle(batchEvent); err != nil {
			return err
		}
		if batchEvent.EventType == "batch.completed" || batchEvent.EventType == "batch.cancelled" {
			return errSSEDone
		}
		return nil
	})
	if err != nil {
		return err
	}
	if statusCode >= http.StatusBadRequest {
		runErr := parseRunErrorMap(decodeJSONMapOrEmpty(responseBody), "")
		if runErr.Message != "" {
			return runErr
		}
		return &ClientError{Method: http.MethodPost, URL: c.endpoint("v1", componentCollection(config.componentType), component, "batch", "stream"), StatusCode: statusCode, Body: string(responseBody)}
	}
	return nil
}

// GetBatchStatus returns current batch status.
func (c *Client) GetBatchStatus(ctx context.Context, batchID string, includeResults bool, timeout time.Duration) (*BatchStatusResponse, error) {
	endpoint, err := c.endpointWithQuery([]string{"v1", "batches", batchID}, url.Values{
		"include_results": []string{fmt.Sprintf("%t", includeResults)},
	})
	if err != nil {
		return nil, err
	}
	statusCode, responseBody, endpoint, err := c.doJSONEndpoint(ctx, http.MethodGet, endpoint, nil, c.requestHeaders("", "", "", nil), timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodGet, URL: endpoint, StatusCode: statusCode, Body: string(responseBody)}
	}
	return parseBatchStatusResponse(responseBody)
}

// CancelBatch cancels a running batch.
func (c *Client) CancelBatch(ctx context.Context, batchID, reason string, timeout time.Duration) (*CancelBatchResponse, error) {
	values := url.Values{}
	if reason != "" {
		values.Set("reason", reason)
	}
	endpoint, err := c.endpointWithQuery([]string{"v1", "batches", batchID}, values)
	if err != nil {
		return nil, err
	}
	statusCode, responseBody, endpoint, err := c.doJSONEndpoint(ctx, http.MethodDelete, endpoint, nil, c.requestHeaders("", "", "", nil), timeout)
	if err != nil {
		return nil, err
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &ClientError{Method: http.MethodDelete, URL: endpoint, StatusCode: statusCode, Body: string(responseBody)}
	}
	return parseCancelBatchResponse(responseBody)
}

func newBatchConfig(opts ...BatchOption) batchConfig {
	config := batchConfig{
		componentType:        ComponentTypeFunction,
		maxConcurrency:       10,
		continueOnFailure:    true,
		batchTimeoutMS:       3_600_000,
		defaultItemTimeoutMS: 30_000,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return config
}

func batchRequestBody(config batchConfig, items []any) map[string]any {
	body := map[string]any{
		"items": items,
		"config": map[string]any{
			"max_concurrency":         config.maxConcurrency,
			"continue_on_failure":     config.continueOnFailure,
			"batch_timeout_ms":        config.batchTimeoutMS,
			"default_item_timeout_ms": config.defaultItemTimeoutMS,
		},
	}
	if len(config.metadata) > 0 {
		body["metadata"] = config.metadata
	}
	return body
}

func normalizeBatchItems(items any, raw bool) ([]any, error) {
	if items == nil {
		return []any{}, nil
	}
	value := reflect.ValueOf(items)
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return nil, fmt.Errorf("agnt5: batch items must be a slice or array, got %T", items)
	}
	out := make([]any, 0, value.Len())
	for i := 0; i < value.Len(); i++ {
		item := value.Index(i).Interface()
		switch typed := item.(type) {
		case BatchItemInput:
			out = append(out, typed.requestValue(i))
		case *BatchItemInput:
			if typed == nil {
				return nil, fmt.Errorf("agnt5: batch item %d is nil", i)
			}
			out = append(out, typed.requestValue(i))
		default:
			if raw {
				out = append(out, typed)
			} else {
				out = append(out, BatchItemInput{Input: typed}.requestValue(i))
			}
		}
	}
	return out, nil
}

func (i BatchItemInput) requestValue(defaultIndex int) map[string]any {
	index := defaultIndex
	if i.Index != nil {
		index = *i.Index
	}
	out := map[string]any{
		"input": i.Input,
		"index": index,
	}
	if i.ItemID != "" {
		out["item_id"] = i.ItemID
	}
	if len(i.Metadata) > 0 {
		out["metadata"] = i.Metadata
	}
	if i.TimeoutMS > 0 {
		out["timeout_ms"] = i.TimeoutMS
	}
	return out
}

func parseBatchStreamEvent(event ReceivedEvent) BatchStreamEvent {
	eventType := firstNonEmpty(firstString(event.Data, "event_type", "eventType"), event.EventType)
	metadata := fieldMap(event.Data, "metadata")
	return BatchStreamEvent{
		EventType: eventType,
		BatchID:   firstNonEmpty(firstString(metadata, "batch_id", "batchId"), firstString(event.Data, "batch_id", "batchId")),
		RunID:     firstString(event.Data, "run_id", "runId"),
		Data:      rawJSONValue(event.Data["data"]),
		Metadata:  metadata,
		Raw:       event.Data,
	}
}

func parseBatchResult(body []byte) (*BatchResult, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	result := &BatchResult{
		BatchID:    firstString(data, "batch_id", "batchId"),
		Status:     parseBatchStatus(fieldString(data, "status")),
		RunIDs:     fieldStringSlice(data, "run_ids", "runIds"),
		TotalItems: fieldInt(data, "total_items", "totalItems"),
		Results:    parseBatchItemResults(data["results"]),
		Stats:      parseBatchStats(data["stats"]),
		TraceID:    firstString(data, "trace_id", "traceId"),
		CreatedAt:  fieldTime(data, "created_at", "createdAt"),
		Raw:        data,
	}
	if result.TotalItems == 0 {
		result.TotalItems = len(result.RunIDs)
	}
	if result.Stats == nil && result.TotalItems > 0 {
		result.Stats = &BatchStats{TotalItems: result.TotalItems}
	}
	return result, nil
}

func parseBatchStatusResponse(body []byte) (*BatchStatusResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	return &BatchStatusResponse{
		BatchID:     firstString(data, "batch_id", "batchId"),
		Found:       boolField(data, "found"),
		Status:      parseBatchStatus(fieldString(data, "status")),
		Results:     parseBatchItemResults(data["results"]),
		Stats:       parseBatchStats(data["stats"]),
		TraceID:     firstString(data, "trace_id", "traceId"),
		SubmittedAt: fieldTime(data, "submitted_at", "submittedAt"),
		StartedAt:   fieldTime(data, "started_at", "startedAt"),
		CompletedAt: fieldTime(data, "completed_at", "completedAt"),
		Raw:         data,
	}, nil
}

func parseCancelBatchResponse(body []byte) (*CancelBatchResponse, error) {
	data, err := decodeJSONMap(body)
	if err != nil {
		return nil, err
	}
	return &CancelBatchResponse{
		BatchID:        firstString(data, "batch_id", "batchId"),
		Status:         parseBatchStatus(fieldString(data, "status")),
		CancelledItems: fieldInt(data, "cancelled_items", "cancelledItems"),
		CompletedItems: fieldInt(data, "completed_items", "completedItems"),
		Raw:            data,
	}, nil
}

func parseBatchItemResults(value any) []BatchItemResult {
	rawItems, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]BatchItemResult, 0, len(rawItems))
	for _, rawItem := range rawItems {
		itemMap, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, BatchItemResult{
			Index:       fieldInt(itemMap, "index"),
			ItemID:      firstString(itemMap, "item_id", "itemId"),
			RunID:       firstString(itemMap, "run_id", "runId"),
			Status:      parseRunStatus(fieldString(itemMap, "status")),
			Output:      rawJSONValue(itemMap["output"]),
			Error:       parseBatchItemError(itemMap["error"]),
			DurationMS:  fieldInt64Ptr(itemMap, "duration_ms", "durationMs"),
			StartedAt:   fieldTime(itemMap, "started_at", "startedAt"),
			CompletedAt: fieldTime(itemMap, "completed_at", "completedAt"),
			Raw:         itemMap,
		})
	}
	return out
}

func parseBatchItemError(value any) *BatchItemError {
	if value == nil {
		return nil
	}
	errorMap, ok := value.(map[string]any)
	if !ok {
		return &BatchItemError{Message: fmt.Sprint(value)}
	}
	return &BatchItemError{
		Code:    firstString(errorMap, "code"),
		Message: firstString(errorMap, "message"),
		Details: fieldMap(errorMap, "details"),
	}
}

func parseBatchStats(value any) *BatchStats {
	statsMap, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return &BatchStats{
		TotalItems:        fieldInt(statsMap, "total_items", "totalItems"),
		CompletedItems:    fieldInt(statsMap, "completed_items", "completedItems"),
		FailedItems:       fieldInt(statsMap, "failed_items", "failedItems"),
		CancelledItems:    fieldInt(statsMap, "cancelled_items", "cancelledItems"),
		PendingItems:      fieldInt(statsMap, "pending_items", "pendingItems"),
		DurationMS:        fieldInt64(statsMap, "duration_ms", "durationMs"),
		AvgItemDurationMS: fieldInt64(statsMap, "avg_item_duration_ms", "avgItemDurationMs"),
	}
}

func parseBatchStatus(value string) BatchStatus {
	switch BatchStatus(value) {
	case BatchStatusPending, BatchStatusQueued, BatchStatusStarted, BatchStatusRunning,
		BatchStatusCompleted, BatchStatusPartialFailure, BatchStatusFailed, BatchStatusCancelled:
		return BatchStatus(value)
	default:
		return BatchStatusUnknown
	}
}

func fieldStringSlice(data map[string]any, keys ...string) []string {
	for _, key := range keys {
		value, ok := data[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case []string:
			return append([]string(nil), typed...)
		case []any:
			out := make([]string, 0, len(typed))
			for _, item := range typed {
				if item != nil {
					out = append(out, fmt.Sprint(item))
				}
			}
			return out
		}
	}
	return nil
}

func (c *Client) endpointWithQuery(path []string, values url.Values) (string, error) {
	if c == nil {
		return "", errors.New("agnt5: nil client")
	}
	parsed, err := url.Parse(c.endpoint(path...))
	if err != nil {
		return "", err
	}
	if len(values) > 0 {
		parsed.RawQuery = values.Encode()
	}
	return parsed.String(), nil
}
