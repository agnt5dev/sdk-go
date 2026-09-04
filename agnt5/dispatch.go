package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func (w *Worker) dispatchServiceMessages(ctx context.Context, req *pb.DispatchComponentRequest) []*pb.ServiceMessage {
	invocation := invocationFromDispatch(req)
	startedAt := time.Now()
	startedAtNS := startedAt.UnixNano()
	runCorrelationID := runCorrelationIDFromRunID(invocation.RunID)
	componentCorrelationID := newCorrelationID(string(invocation.ComponentType))
	metadata := w.invocationMetadata(invocation)
	resumedWorkflowCorrelationID, isHITLResume := "", false
	if invocation.ComponentType == ComponentTypeWorkflow {
		resumedWorkflowCorrelationID, isHITLResume = hitlResumeWorkflowCorrelationID(invocation.Metadata)
	}
	if isHITLResume {
		componentCorrelationID = resumedWorkflowCorrelationID
	}

	if !isHITLResume {
		// run.started and <component>.started always travel together: one
		// acknowledged batch for streaming or legacy runs, and one held pair
		// for a run whose lifecycle rides in CompleteJob.
		started := []journalEvent{
			runStartedEvent(invocation, metadata, runCorrelationID, startedAtNS),
			componentStartedEvent(invocation, metadata, componentCorrelationID, runCorrelationID, startedAtNS),
		}
		if err := w.writeEvents(ctx, started); err != nil {
			return []*pb.ServiceMessage{w.dispatchServiceMessageFromResponse(dispatchResponseFromResult(req, InvocationResult{}, err))}
		}
		w.logLifecycle(ctx, invocation, "INFO", "run started")
		w.logLifecycle(ctx, invocation, "INFO", "component started")
	}

	result, invokeErr := w.invoke(ctx, invocation, componentCorrelationID, runCorrelationID)
	durationMS := time.Since(startedAt).Milliseconds()
	eventsToFlush := result.Events
	if IsWaitingForUserInput(invokeErr) && metadata["dispatch_mode"] == "pull" {
		eventsToFlush = make([]Event, 0, len(result.Events))
		for _, event := range result.Events {
			// CompleteJob owns the fenced workflow.paused terminal record for a
			// pull lease. The generic append path must still durably flush every
			// preceding lifecycle event before the paused response is returned.
			if event.Type != "workflow.paused" {
				eventsToFlush = append(eventsToFlush, event)
			}
		}
	}
	messages, eventsErr := w.flushInvocationEvents(ctx, invocation, eventsToFlush, metadata, componentCorrelationID)
	if eventsErr != nil {
		if invokeErr != nil {
			invokeErr = fmt.Errorf("agnt5: flush invocation events: %w (handler outcome: %v)", eventsErr, invokeErr)
		} else {
			invokeErr = eventsErr
		}
	}
	if IsWaitingForUserInput(invokeErr) {
		w.logLifecycle(ctx, invocation, "INFO", "workflow paused")
		messages = append(messages, w.dispatchServiceMessageFromResponse(dispatchPausedResponse(req, result, invokeErr)))
		return messages
	}
	var sleepSuspension *durableSleepSuspensionError
	if errors.As(invokeErr, &sleepSuspension) && sleepSuspension.suspension != nil {
		w.logLifecycle(ctx, invocation, "INFO", "workflow paused")
		messages = append(messages, w.dispatchServiceMessageFromResponse(
			dispatchSuspendedResponse(req, sleepSuspension.suspension),
		))
		return messages
	}
	if invokeErr != nil {
		w.logLifecycle(ctx, invocation, "ERROR", "component failed", "error", invokeErr.Error())
		_ = w.writeComponentFailed(ctx, invocation, metadata, componentCorrelationID, runCorrelationID, durationMS, invokeErr)
		messages = append(messages, w.dispatchServiceMessageFromResponse(dispatchResponseFromResult(req, result, invokeErr)))
		return messages
	}
	if err := w.writeComponentCompleted(ctx, invocation, metadata, componentCorrelationID, runCorrelationID, durationMS, result); err != nil {
		messages = append(messages, w.dispatchServiceMessageFromResponse(dispatchResponseFromResult(req, result, err)))
		return messages
	}
	w.logLifecycle(ctx, invocation, "INFO", "component completed", "duration_ms", durationMS)
	messages = append(messages, w.dispatchServiceMessageFromResponse(dispatchResponseFromResult(req, result, nil)))
	return messages
}

func runCorrelationIDFromRunID(runID string) string {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return newCorrelationID("run")
	}
	if len(runID) > 8 {
		return runID[:8]
	}
	return runID
}

func hitlResumeWorkflowCorrelationID(metadata map[string]string) (string, bool) {
	if metadata["pause_reason"] != "user_input_required" {
		return "", false
	}
	if _, ok := metadata["user_response"]; !ok {
		return "", false
	}
	correlationID := strings.TrimSpace(metadata["workflow_correlation_id"])
	return correlationID, correlationID != ""
}

func (w *Worker) dispatchServiceMessageFromResponse(response *pb.DispatchComponentResponse) *pb.ServiceMessage {
	return &pb.ServiceMessage{
		WorkerId: w.workerID,
		Metadata: map[string]string{},
		MessageType: &pb.ServiceMessage_FunctionResponse{
			FunctionResponse: response,
		},
	}
}

func invocationFromDispatch(req *pb.DispatchComponentRequest) Invocation {
	invocationID := req.GetInvocationId()
	return Invocation{
		ID:             invocationID,
		RunID:          runIDFromInvocationID(invocationID),
		ComponentName:  req.GetComponentName(),
		ComponentType:  componentTypeFromProto(req.GetComponentType()),
		Input:          cloneBytes(req.GetInputData()),
		Attempt:        int(req.GetAttempt()),
		Metadata:       mergeDurableContinuation(req.GetMetadata()),
		LeaseID:        req.GetLeaseId(),
		IsStreaming:    req.GetIsStreaming(),
		StreamFallback: true,
	}
}

func dispatchSuspendedResponse(req *pb.DispatchComponentRequest, suspension *pb.WorkerSuspension) *pb.DispatchComponentResponse {
	return &pb.DispatchComponentResponse{
		InvocationId: req.GetInvocationId(),
		Success:      true,
		Result: &pb.DispatchComponentResponse_WorkerSuspension{
			WorkerSuspension: suspension,
		},
		Metadata:  cloneStringMap(req.GetMetadata()),
		EventType: "workflow.paused",
		Attempt:   req.GetAttempt(),
		LeaseId:   req.GetLeaseId(),
	}
}

func dispatchResponseFromResult(req *pb.DispatchComponentRequest, result InvocationResult, invokeErr error) *pb.DispatchComponentResponse {
	metadata := cloneStringMap(req.GetMetadata())
	for key, value := range result.Metadata {
		metadata[key] = value
	}
	leaseID := result.LeaseID
	if leaseID == "" {
		leaseID = req.GetLeaseId()
	}
	response := &pb.DispatchComponentResponse{
		InvocationId: req.GetInvocationId(),
		Metadata:     metadata,
		Attempt:      req.GetAttempt(),
		LeaseId:      leaseID,
	}
	if invokeErr != nil {
		response.Success = false
		response.ErrorMessage = invokeErr.Error()
		response.EventType = "run.failed"
		return response
	}
	response.Success = true
	response.Result = &pb.DispatchComponentResponse_OutputData{
		OutputData: cloneBytes(result.Output),
	}
	response.EventType = "run.completed"
	return response
}

func dispatchPausedResponse(req *pb.DispatchComponentRequest, result InvocationResult, pauseErr error) *pb.DispatchComponentResponse {
	metadata := cloneStringMap(req.GetMetadata())
	for key, value := range result.Metadata {
		metadata[key] = value
	}
	for _, event := range result.Events {
		if event.Type != "workflow.paused" {
			continue
		}
		for key, value := range event.Metadata {
			metadata[key] = value
		}
		break
	}
	leaseID := result.LeaseID
	if leaseID == "" {
		leaseID = req.GetLeaseId()
	}
	output := map[string]any{"_paused": true}
	var target *WaitingForUserInputError
	if errors.As(pauseErr, &target) {
		output["question"] = target.Request.Prompt
		if value := metadata["pause_index"]; value != "" {
			output["pause_index"] = value
		}
	}
	payload, _ := json.Marshal(output)
	return &pb.DispatchComponentResponse{
		InvocationId: req.GetInvocationId(),
		Success:      true,
		Result: &pb.DispatchComponentResponse_OutputData{
			OutputData: payload,
		},
		ErrorMessage: "",
		Metadata:     metadata,
		EventType:    "workflow.paused",
		Attempt:      req.GetAttempt(),
		LeaseId:      leaseID,
	}
}

func (w *Worker) invocationMetadata(inv Invocation) map[string]string {
	metadata := cloneStringMap(w.metadata)
	for key, value := range inv.Metadata {
		metadata[key] = value
	}
	if w.projectID != "" {
		metadata["project_id"] = w.projectID
		metadata[envProjectID] = w.projectID
	}
	if projectID := canonicalProjectID(metadata); projectID != "" {
		metadata = withCanonicalProjectMetadata(metadata, projectID)
	}
	if w.deploymentID != "" {
		metadata["deployment_id"] = w.deploymentID
		metadata[envDeploymentID] = w.deploymentID
	}
	metadata["worker_id"] = w.workerID
	metadata["service_name"] = w.serviceName
	metadata["component_name"] = inv.ComponentName
	if inv.ComponentType != "" {
		metadata["component_type"] = string(inv.ComponentType)
	}
	metadata["attempt"] = fmt.Sprintf("%d", inv.Attempt)
	if inv.LeaseID != "" {
		metadata["lease_id"] = inv.LeaseID
		metadata["lease_attempt"] = fmt.Sprintf("%d", inv.Attempt)
		if metadata["dispatch_mode"] == "" {
			metadata["dispatch_mode"] = "push"
		}
		if metadata["dispatch_mode"] == "push" {
			// Push leases use the durable worker identity as their session
			// fence. Pull dispatch stamps the authenticated session ID before
			// reaching this method and must retain it unchanged.
			metadata["worker_session_id"] = w.workerID
		}
	}
	return metadata
}

var executionAuthorityMetadataKeys = [...]string{
	"dispatch_mode",
	"worker_id",
	"worker_session_id",
	"lease_id",
	"lease_attempt",
	"assignment_commit_offset",
}

func mergeInvocationEventMetadata(baseMetadata, eventMetadata map[string]string) map[string]string {
	metadata := cloneStringMap(baseMetadata)
	for key, value := range eventMetadata {
		metadata[key] = value
	}
	// Per-event metadata is user-authored. Preserve custom fields, but pin the
	// execution fence to the transport-authored dispatch values.
	for _, key := range executionAuthorityMetadataKeys {
		if value, ok := baseMetadata[key]; ok {
			metadata[key] = value
		}
	}
	return metadata
}

func runStartedEvent(inv Invocation, metadata map[string]string, correlationID string, timestampNS int64) journalEvent {
	eventType := "run.started"
	return journalEvent{
		RunID:               inv.RunID,
		EventType:           eventType,
		CorrelationID:       correlationID,
		ParentCorrelationID: "",
		SourceTimestampNS:   timestampNS,
		Metadata:            metadata,
		Data: lifecycleEventData(eventType, inv.ComponentName, ComponentTypeRun, timestampNS, map[string]any{
			"correlation_id":        correlationID,
			"parent_correlation_id": "",
			"input_data":            jsonValueFromBytes(inv.Input),
			"input_type":            "json",
			"attempt":               inv.Attempt,
		}),
	}
}

func componentStartedEvent(inv Invocation, metadata map[string]string, correlationID, parentCorrelationID string, timestampNS int64) journalEvent {
	componentType := lifecycleComponentType(inv.ComponentType)
	eventType := lifecycleEventType(componentType, "started")
	return journalEvent{
		RunID:               inv.RunID,
		EventType:           eventType,
		CorrelationID:       correlationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   timestampNS,
		Metadata:            metadata,
		Data: lifecycleEventData(eventType, inv.ComponentName, componentType, timestampNS, map[string]any{
			"correlation_id":        correlationID,
			"parent_correlation_id": parentCorrelationID,
			"input_data":            jsonValueFromBytes(inv.Input),
			"input_type":            "json",
			"attempt":               inv.Attempt,
		}),
	}
}

func (w *Worker) writeComponentCompleted(ctx context.Context, inv Invocation, metadata map[string]string, correlationID, parentCorrelationID string, durationMS int64, result InvocationResult) error {
	componentType := lifecycleComponentType(inv.ComponentType)
	eventType := lifecycleEventType(componentType, "completed")
	timestampNS := nowUnixNS()
	eventMetadata := cloneStringMap(metadata)
	for key, value := range result.Metadata {
		eventMetadata[key] = value
	}
	return w.writeEvent(ctx, journalEvent{
		RunID:               inv.RunID,
		EventType:           eventType,
		CorrelationID:       correlationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   timestampNS,
		Metadata:            eventMetadata,
		Data: lifecycleEventData(eventType, inv.ComponentName, componentType, timestampNS, map[string]any{
			"correlation_id":        correlationID,
			"parent_correlation_id": parentCorrelationID,
			"output_data":           jsonValueFromBytes(result.Output),
			"output_type":           "json",
			"duration_ms":           durationMS,
		}),
	})
}

func (w *Worker) writeComponentFailed(ctx context.Context, inv Invocation, metadata map[string]string, correlationID, parentCorrelationID string, durationMS int64, invokeErr error) error {
	componentType := lifecycleComponentType(inv.ComponentType)
	eventType := lifecycleEventType(componentType, "failed")
	timestampNS := nowUnixNS()
	return w.writeEvent(ctx, journalEvent{
		RunID:               inv.RunID,
		EventType:           eventType,
		CorrelationID:       correlationID,
		ParentCorrelationID: parentCorrelationID,
		SourceTimestampNS:   timestampNS,
		Metadata:            metadata,
		Data: lifecycleEventData(eventType, inv.ComponentName, componentType, timestampNS, map[string]any{
			"correlation_id":        correlationID,
			"parent_correlation_id": parentCorrelationID,
			"error_code":            "WORKER_EXECUTION_FAILED",
			"error_message":         invokeErr.Error(),
			"duration_ms":           durationMS,
		}),
	})
}

func (w *Worker) flushInvocationEvents(ctx context.Context, inv Invocation, events []Event, baseMetadata map[string]string, parentCorrelationID string) ([]*pb.ServiceMessage, error) {
	messages := make([]*pb.ServiceMessage, 0, len(events))
	durableEvents := make([]journalEvent, 0, len(events))
	streamEvents := make([]streamEvent, 0, len(events))
	for _, event := range events {
		if event.RunID == "" {
			event.RunID = inv.RunID
		}
		if event.SourceTimestampNS == 0 {
			event.SourceTimestampNS = nowUnixNS()
		}
		metadata := mergeInvocationEventMetadata(baseMetadata, event.Metadata)
		correlationID := event.CorrelationID
		if correlationID == "" {
			correlationID = newCorrelationID("event")
		}
		eventParentCorrelationID := event.ParentCorrelationID
		if eventParentCorrelationID == "" {
			eventParentCorrelationID = parentCorrelationID
		}
		if IsSSEOnlyEventType(event.Type) {
			if inv.IsStreaming {
				streamEvents = append(streamEvents, streamEventFromEvent(
					inv, event, metadata, correlationID, eventParentCorrelationID,
				))
			}
			continue
		}
		durableEvents = append(durableEvents, journalEvent{
			RunID:               event.RunID,
			EventType:           event.Type,
			Data:                eventDataBytes(event.Data),
			Metadata:            metadata,
			CorrelationID:       correlationID,
			ParentCorrelationID: eventParentCorrelationID,
			SourceTimestampNS:   event.SourceTimestampNS,
		})
	}
	if err := w.streamInvocationEvents(ctx, streamEvents); err != nil {
		if !inv.StreamFallback {
			return messages, err
		}
		messages = append(messages, w.dispatchMessagesFromStreamEvents(inv, streamEvents)...)
	}
	if err := w.writeEvents(ctx, durableEvents); err != nil {
		return messages, err
	}
	return messages, nil
}

func (w *Worker) streamInvocationEvent(ctx context.Context, inv Invocation, event Event, baseMetadata map[string]string, parentCorrelationID string) error {
	if event.RunID == "" {
		event.RunID = inv.RunID
	}
	if event.SourceTimestampNS == 0 {
		event.SourceTimestampNS = nowUnixNS()
	}
	metadata := mergeInvocationEventMetadata(baseMetadata, event.Metadata)
	correlationID := event.CorrelationID
	if correlationID == "" {
		correlationID = newCorrelationID("event")
	}
	eventParentCorrelationID := event.ParentCorrelationID
	if eventParentCorrelationID == "" {
		eventParentCorrelationID = parentCorrelationID
	}
	return w.streamInvocationEvents(ctx, []streamEvent{
		streamEventFromEvent(inv, event, metadata, correlationID, eventParentCorrelationID),
	})
}

func (w *Worker) writeInvocationEvent(ctx context.Context, inv Invocation, event Event, baseMetadata map[string]string, parentCorrelationID string) error {
	if event.RunID == "" {
		event.RunID = inv.RunID
	}
	if event.SourceTimestampNS == 0 {
		event.SourceTimestampNS = nowUnixNS()
	}
	metadata := mergeInvocationEventMetadata(baseMetadata, event.Metadata)
	correlationID := event.CorrelationID
	if correlationID == "" {
		correlationID = newCorrelationID("event")
	}
	eventParentCorrelationID := event.ParentCorrelationID
	if eventParentCorrelationID == "" {
		eventParentCorrelationID = parentCorrelationID
	}
	return w.writeEvent(ctx, journalEvent{
		RunID:               event.RunID,
		EventType:           event.Type,
		Data:                eventDataBytes(event.Data),
		Metadata:            metadata,
		CorrelationID:       correlationID,
		ParentCorrelationID: eventParentCorrelationID,
		SourceTimestampNS:   event.SourceTimestampNS,
	})
}

func streamEventFromEvent(inv Invocation, event Event, metadata map[string]string, correlationID, parentCorrelationID string) streamEvent {
	return streamEvent{
		RunID:             event.RunID,
		EventType:         event.Type,
		Data:              eventDataBytes(event.Data),
		Metadata:          metadata,
		TraceID:           correlationID,
		SpanID:            parentCorrelationID,
		ContentIndex:      event.ContentIndex,
		Sequence:          event.Sequence,
		Attempt:           int32(inv.Attempt),
		SourceTimestampNS: event.SourceTimestampNS,
		LeaseID:           inv.LeaseID,
	}
}

func (w *Worker) streamInvocationEvents(ctx context.Context, events []streamEvent) error {
	if len(events) == 0 {
		return nil
	}
	streamer, ok := w.eventWriter.(eventStreamer)
	if !ok || streamer == nil {
		return ErrTransportNotImplemented
	}
	return streamer.StreamEvents(ctx, events)
}

func (w *Worker) flushStreamEvents(ctx context.Context) error {
	flusher, ok := w.eventWriter.(eventStreamFlusher)
	if !ok || flusher == nil {
		return nil
	}
	return flusher.FlushEvents(ctx)
}

func (w *Worker) dispatchMessagesFromStreamEvents(inv Invocation, events []streamEvent) []*pb.ServiceMessage {
	messages := make([]*pb.ServiceMessage, 0, len(events))
	for _, event := range events {
		messages = append(messages, w.dispatchServiceMessageFromResponse(&pb.DispatchComponentResponse{
			InvocationId:      inv.ID,
			Success:           true,
			Result:            &pb.DispatchComponentResponse_OutputData{OutputData: cloneBytes(event.Data)},
			Metadata:          cloneStringMap(event.Metadata),
			EventType:         event.EventType,
			ContentIndex:      int32(event.ContentIndex),
			Sequence:          event.Sequence,
			Attempt:           event.Attempt,
			SourceTimestampNs: event.SourceTimestampNS,
			LeaseId:           event.LeaseID,
		}))
	}
	return messages
}

func lifecycleComponentType(componentType ComponentType) ComponentType {
	switch componentType {
	case ComponentTypeAgent, ComponentTypeMCP, ComponentTypeScorer, ComponentTypeTool:
		return componentType
	case ComponentTypeWorkflow:
		return ComponentTypeWorkflow
	default:
		return ComponentTypeFunction
	}
}

func lifecycleEventType(componentType ComponentType, stage string) string {
	if componentType == "" {
		componentType = ComponentTypeFunction
	}
	return string(componentType) + "." + stage
}

func componentTypeFromProto(componentType pb.ComponentType) ComponentType {
	switch componentType {
	case pb.ComponentType_COMPONENT_TYPE_FUNCTION:
		return ComponentTypeFunction
	case pb.ComponentType_COMPONENT_TYPE_WORKFLOW:
		return ComponentTypeWorkflow
	case pb.ComponentType_COMPONENT_TYPE_AGENT:
		return ComponentTypeAgent
	case pb.ComponentType_COMPONENT_TYPE_TOOL:
		return ComponentTypeTool
	case pb.ComponentType_COMPONENT_TYPE_MCP:
		return ComponentTypeMCP
	case pb.ComponentType_COMPONENT_TYPE_ENTITY:
		return ComponentTypeEntity
	case pb.ComponentType_COMPONENT_TYPE_SCORER:
		return ComponentTypeScorer
	default:
		return ""
	}
}

func runIDFromInvocationID(invocationID string) string {
	if idx := strings.IndexByte(invocationID, ':'); idx > 0 {
		return invocationID[:idx]
	}
	return invocationID
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
