package agnt5

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

type eventWriter interface {
	WriteEvent(context.Context, journalEvent) error
}

type batchEventWriter interface {
	WriteEvents(context.Context, []journalEvent) error
}

type eventStreamer interface {
	StreamEvents(context.Context, []streamEvent) error
}

type eventStreamFlusher interface {
	FlushEvents(context.Context) error
}

type stepCheckpointWriter interface {
	Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error)
}

type stepActivationWriter interface {
	BeginActivation(context.Context, *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error)
	CompleteActivation(context.Context, *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error)
	FailActivation(context.Context, *pb.FailActivationRequest) (*pb.FailActivationResponse, error)
}

type journalEvent struct {
	RunID               string
	EventType           string
	Data                []byte
	Metadata            map[string]string
	CorrelationID       string
	ParentCorrelationID string
	SourceTimestampNS   int64
}

type streamEvent struct {
	RunID             string
	EventType         string
	Data              []byte
	Metadata          map[string]string
	TraceID           string
	SpanID            string
	ContentIndex      int
	Sequence          int64
	Attempt           int32
	SourceTimestampNS int64
	LeaseID           string
}

type engineEventWriter struct {
	client       pb.EngineServiceClient
	stream       pb.EngineService_EventStreamClient
	streamCancel context.CancelFunc
	streamCount  int64
	streamOps    chan streamWriterOp
}

type streamWriterOp struct {
	ctx    context.Context
	events []streamEvent
	flush  chan error
}

const (
	activationRPCAttempts = 6
	streamWriterQueueSize = 256
)

func waitActivationRPCRetry(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func newEngineEventWriter(client pb.EngineServiceClient) *engineEventWriter {
	w := &engineEventWriter{
		client:    client,
		streamOps: make(chan streamWriterOp, streamWriterQueueSize),
	}
	if client != nil {
		go w.runStreamWriter()
	}
	return w
}

func validateAppendBatchRecords(records []*pb.Record) error {
	if len(records) == 0 {
		return nil
	}
	runID := records[0].GetRunId()
	for _, record := range records[1:] {
		if record.GetRunId() != runID {
			return fmt.Errorf("agnt5: append batch records must share one run_id")
		}
	}
	return nil
}

func validateAppendBatchResponse(resp *pb.AppendBatchResponse, expected int) error {
	if resp == nil {
		return fmt.Errorf("agnt5: append batch returned a nil response")
	}
	if len(resp.GetOffsets()) != expected {
		return fmt.Errorf(
			"agnt5: append batch returned %d offsets, want %d",
			len(resp.GetOffsets()),
			expected,
		)
	}
	written := resp.GetWrittenCount()
	if written < 0 || int(written) > expected {
		return fmt.Errorf(
			"agnt5: append batch written_count %d is outside 0..%d",
			written,
			expected,
		)
	}
	return nil
}

func (w *engineEventWriter) WriteEvent(ctx context.Context, event journalEvent) error {
	if w == nil || w.client == nil {
		return nil
	}
	record, err := recordFromJournalEvent(event)
	if err != nil {
		return err
	}
	_, err = w.client.Append(ctx, &pb.AppendRequest{Record: record})
	if err != nil {
		return fmt.Errorf("agnt5: append %s checkpoint: %w", event.EventType, err)
	}
	return nil
}

func (w *engineEventWriter) WriteEvents(ctx context.Context, events []journalEvent) error {
	if w == nil || w.client == nil || len(events) == 0 {
		return nil
	}
	records := make([]*pb.Record, 0, len(events))
	for _, event := range events {
		record, err := recordFromJournalEvent(event)
		if err != nil {
			return err
		}
		records = append(records, record)
	}
	if err := validateAppendBatchRecords(records); err != nil {
		return err
	}
	resp, err := w.client.AppendBatch(ctx, &pb.AppendBatchRequest{Records: records})
	if err != nil {
		return fmt.Errorf("agnt5: append %d journal events: %w", len(events), err)
	}
	if err := validateAppendBatchResponse(resp, len(records)); err != nil {
		return err
	}
	return nil
}

func (w *engineEventWriter) StreamEvents(ctx context.Context, events []streamEvent) error {
	if w == nil || w.client == nil || len(events) == 0 {
		return nil
	}
	queued := make([]streamEvent, 0, len(events))
	for _, event := range events {
		if canonicalProjectID(event.Metadata) == "" {
			return fmt.Errorf("agnt5: cannot stream %s without project_id metadata", event.EventType)
		}
		event.Data = cloneBytes(event.Data)
		event.Metadata = cloneStringMap(event.Metadata)
		queued = append(queued, event)
	}
	select {
	case w.streamOps <- streamWriterOp{ctx: ctx, events: queued}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("agnt5: queue event stream: %w", ctx.Err())
	}
}

func (w *engineEventWriter) runStreamWriter() {
	var pendingErr error
	for op := range w.streamOps {
		if op.flush != nil {
			if pendingErr != nil {
				op.flush <- errors.Join(pendingErr, w.flushEvents(op.ctx))
				pendingErr = nil
				continue
			}
			op.flush <- w.flushEvents(op.ctx)
			continue
		}
		if err := w.writeStreamEvents(op.ctx, op.events); err != nil && pendingErr == nil {
			pendingErr = err
		}
	}
	if w.stream != nil {
		w.resetStream()
	}
}

func (w *engineEventWriter) writeStreamEvents(ctx context.Context, events []streamEvent) error {
	for attempt := 0; attempt < 2; attempt++ {
		if w.stream == nil {
			streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			stream, err := w.client.EventStream(streamCtx)
			if err != nil {
				cancel()
				return fmt.Errorf("agnt5: open event stream: %w", err)
			}
			w.stream = stream
			w.streamCancel = cancel
			w.streamCount = 0
		}
		if err := w.sendStreamEvents(events); err == nil {
			return nil
		} else if attempt == 1 {
			return err
		}
		w.resetStream()
	}
	return fmt.Errorf("agnt5: event stream retry exhausted")
}

func (w *engineEventWriter) sendStreamEvents(events []streamEvent) error {
	for _, event := range events {
		projectID := canonicalProjectID(event.Metadata)
		if err := w.stream.Send(&pb.EventStreamMessage{
			RunId:             event.RunID,
			EventType:         event.EventType,
			Data:              cloneBytes(event.Data),
			TraceId:           event.TraceID,
			SpanId:            event.SpanID,
			ProjectId:         projectID,
			SourceTimestampNs: sourceTimestampNS(event.SourceTimestampNS),
			WorkerId:          event.Metadata["worker_id"],
		}); err != nil {
			return fmt.Errorf("agnt5: stream %s event: %w", event.EventType, err)
		}
		w.streamCount++
	}
	return nil
}

// FlushEvents closes the current client stream and waits for the runtime to
// acknowledge every frame. Pull workers call this before CompleteJob so a
// terminal lifecycle event cannot overtake transient output deltas.
func (w *engineEventWriter) FlushEvents(ctx context.Context) error {
	if w == nil || w.client == nil {
		return nil
	}
	result := make(chan error, 1)
	select {
	case w.streamOps <- streamWriterOp{ctx: ctx, flush: result}:
	case <-ctx.Done():
		return fmt.Errorf("agnt5: queue event stream flush: %w", ctx.Err())
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("agnt5: flush event stream: %w", ctx.Err())
	}
}

func (w *engineEventWriter) flushEvents(ctx context.Context) error {
	if w.stream == nil {
		return nil
	}

	stream := w.stream
	cancel := w.streamCancel
	expected := w.streamCount
	w.stream = nil
	w.streamCancel = nil
	w.streamCount = 0

	type flushResult struct {
		ack *pb.EventStreamAck
		err error
	}
	resultCh := make(chan flushResult, 1)
	go func() {
		ack, err := stream.CloseAndRecv()
		resultCh <- flushResult{ack: ack, err: err}
	}()

	select {
	case result := <-resultCh:
		if cancel != nil {
			cancel()
		}
		if result.err != nil {
			return fmt.Errorf("agnt5: flush event stream: %w", result.err)
		}
		if !result.ack.GetSuccess() {
			return fmt.Errorf("agnt5: runtime rejected event stream flush")
		}
		if result.ack.GetEventsReceived() != expected {
			return fmt.Errorf("agnt5: runtime acknowledged %d streamed events, want %d", result.ack.GetEventsReceived(), expected)
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return fmt.Errorf("agnt5: flush event stream: %w", ctx.Err())
	}
}

func (w *engineEventWriter) resetStream() {
	if w.stream != nil {
		_ = w.stream.CloseSend()
	}
	if w.streamCancel != nil {
		w.streamCancel()
	}
	w.stream = nil
	w.streamCancel = nil
	w.streamCount = 0
}

func recordFromJournalEvent(event journalEvent) (*pb.Record, error) {
	projectID := canonicalProjectID(event.Metadata)
	if projectID == "" {
		return nil, fmt.Errorf("agnt5: cannot write %s without project_id metadata", event.EventType)
	}
	metadata := withCanonicalProjectMetadata(event.Metadata, projectID)
	return &pb.Record{
		ProjectId:      projectID,
		RunId:          event.RunID,
		EventType:      event.EventType,
		Data:           cloneBytes(event.Data),
		TimestampNs:    sourceTimestampNS(event.SourceTimestampNS),
		CorrelationId:  event.CorrelationID,
		ParentEventId:  event.ParentCorrelationID,
		Metadata:       metadata,
		DataType:       "json",
		DataCompressed: false,
	}, nil
}

func (w *engineEventWriter) Checkpoint(ctx context.Context, req *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	if w == nil || w.client == nil {
		return nil, fmt.Errorf("agnt5: checkpoint client is not configured")
	}
	resp, err := w.client.Checkpoint(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("agnt5: checkpoint step %q: %w", req.GetCheckpoint().GetStepKey(), err)
	}
	if !resp.GetSuccess() {
		return nil, fmt.Errorf("agnt5: checkpoint step %q rejected: %s", req.GetCheckpoint().GetStepKey(), resp.GetErrorMessage())
	}
	return resp, nil
}

func (w *engineEventWriter) BeginActivation(ctx context.Context, req *pb.BeginActivationRequest) (*pb.BeginActivationResponse, error) {
	if w == nil || w.client == nil {
		return nil, newActivationError(ActivationErrorDurabilityUnavailable, "activation client is not configured", "", 0, nil)
	}
	for attempt := 0; attempt < activationRPCAttempts; attempt++ {
		resp, err := w.client.BeginActivation(ctx, req)
		if err == nil {
			return resp, nil
		}
		if attempt+1 >= activationRPCAttempts || !isRetryableActivationRPCError(err) || !waitActivationRPCRetry(ctx, attempt) {
			return nil, activationRPCError("BeginActivation", err)
		}
	}
	panic("activation retry loop must return")
}

func (w *engineEventWriter) CompleteActivation(ctx context.Context, req *pb.CompleteActivationRequest) (*pb.CompleteActivationResponse, error) {
	if w == nil || w.client == nil {
		return nil, newActivationError(ActivationErrorDurabilityUnavailable, "activation client is not configured", req.GetActivationId(), req.GetAttempt(), nil)
	}
	for attempt := 0; attempt < activationRPCAttempts; attempt++ {
		resp, err := w.client.CompleteActivation(ctx, req)
		if err == nil {
			return resp, nil
		}
		if attempt+1 >= activationRPCAttempts || !isRetryableActivationRPCError(err) || !waitActivationRPCRetry(ctx, attempt) {
			return nil, activationRPCError("CompleteActivation", err)
		}
	}
	panic("activation retry loop must return")
}

func (w *engineEventWriter) FailActivation(ctx context.Context, req *pb.FailActivationRequest) (*pb.FailActivationResponse, error) {
	if w == nil || w.client == nil {
		return nil, newActivationError(ActivationErrorDurabilityUnavailable, "activation client is not configured", req.GetActivationId(), req.GetAttempt(), nil)
	}
	for attempt := 0; attempt < activationRPCAttempts; attempt++ {
		resp, err := w.client.FailActivation(ctx, req)
		if err == nil {
			return resp, nil
		}
		if attempt+1 >= activationRPCAttempts || !isRetryableActivationRPCError(err) || !waitActivationRPCRetry(ctx, attempt) {
			return nil, activationRPCError("FailActivation", err)
		}
	}
	panic("activation retry loop must return")
}

func (w *Worker) writeEvent(ctx context.Context, event journalEvent) error {
	if w.eventWriter == nil {
		return nil
	}
	return w.eventWriter.WriteEvent(ctx, event)
}

func (w *Worker) writeEvents(ctx context.Context, events []journalEvent) error {
	if w.eventWriter == nil || len(events) == 0 {
		return nil
	}
	if uint32(len(events)) > w.effectiveJournalQueueSize() {
		return fmt.Errorf("agnt5: journal event queue overflow: %d events exceeds %d", len(events), w.effectiveJournalQueueSize())
	}
	if batchWriter, ok := w.eventWriter.(batchEventWriter); ok && batchWriter != nil {
		batchSize := int(w.effectiveJournalBatchSize())
		for start := 0; start < len(events); start += batchSize {
			end := start + batchSize
			if end > len(events) {
				end = len(events)
			}
			if err := batchWriter.WriteEvents(ctx, events[start:end]); err != nil {
				return err
			}
		}
		return nil
	}
	for _, event := range events {
		if err := w.eventWriter.WriteEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) effectiveJournalQueueSize() uint32 {
	if w == nil || w.journalQueueSize == 0 {
		return defaultJournalQueueSize
	}
	return w.journalQueueSize
}

func (w *Worker) effectiveJournalBatchSize() uint32 {
	if w == nil || w.journalBatchSize == 0 {
		return defaultJournalBatchSize
	}
	return w.journalBatchSize
}

func lifecycleEventData(eventType, name string, componentType ComponentType, timestampNS int64, fields map[string]any) []byte {
	data := map[string]any{
		"event_type":            eventType,
		"event_id":              newEventID(),
		"name":                  name,
		"timestamp_ns":          timestampNS,
		"component_type":        string(componentType),
		"correlation_id":        fields["correlation_id"],
		"parent_correlation_id": fields["parent_correlation_id"],
	}
	for key, value := range fields {
		if value != nil {
			data[key] = value
		}
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

func eventDataBytes(data any) []byte {
	if data == nil {
		return []byte(`null`)
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return []byte(`null`)
	}
	return encoded
}

func jsonValueFromBytes(data []byte) any {
	if len(data) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(data, &value); err == nil {
		return value
	}
	return string(data)
}

func canonicalProjectID(metadata map[string]string) string {
	for _, key := range []string{"project_id", envProjectID} {
		if value := metadata[key]; value != "" {
			return value
		}
	}
	return ""
}

func withCanonicalProjectMetadata(metadata map[string]string, projectID string) map[string]string {
	out := cloneStringMap(metadata)
	if projectID != "" {
		out["project_id"] = projectID
		out["tenant_id"] = projectID
		out[envProjectID] = projectID
	}
	return out
}

func sourceTimestampNS(timestampNS int64) int64 {
	if timestampNS > 0 {
		return timestampNS
	}
	return nowUnixNS()
}

func nowUnixNS() int64 {
	return time.Now().UnixNano()
}

func newEventID() string {
	return "evt-" + newRandomHex(16)
}

func newCorrelationID(prefix string) string {
	return prefix + "-" + newRandomHex(8)
}

func newRandomHex(size int) string {
	if size <= 0 {
		size = 8
	}
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}
