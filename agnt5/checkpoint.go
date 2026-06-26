package agnt5

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
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

type stepCheckpointWriter interface {
	Checkpoint(context.Context, *pb.CheckpointRequest) (*pb.CheckpointResponse, error)
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
	client pb.EngineServiceClient
}

func newEngineEventWriter(client pb.EngineServiceClient) *engineEventWriter {
	return &engineEventWriter{client: client}
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
	resp, err := w.client.AppendBatch(ctx, &pb.AppendBatchRequest{Records: records})
	if err != nil {
		return fmt.Errorf("agnt5: append %d journal events: %w", len(events), err)
	}
	if resp.GetWrittenCount() != 0 && int(resp.GetWrittenCount()) != len(records) {
		return fmt.Errorf("agnt5: append batch wrote %d events, want %d", resp.GetWrittenCount(), len(records))
	}
	return nil
}

func (w *engineEventWriter) StreamEvents(ctx context.Context, events []streamEvent) error {
	if w == nil || w.client == nil || len(events) == 0 {
		return nil
	}
	stream, err := w.client.EventStream(ctx)
	if err != nil {
		return fmt.Errorf("agnt5: open event stream: %w", err)
	}
	for _, event := range events {
		projectID := canonicalProjectID(event.Metadata)
		if projectID == "" {
			return fmt.Errorf("agnt5: cannot stream %s without project_id metadata", event.EventType)
		}
		if err := stream.Send(&pb.EventStreamMessage{
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
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("agnt5: close event stream: %w", err)
	}
	if !ack.GetSuccess() {
		return fmt.Errorf("agnt5: event stream rejected %d events", len(events))
	}
	return nil
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
