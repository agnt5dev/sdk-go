package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func TestHITLResumeContinuesOriginalTraceAndSkipsCachedStepEvents(t *testing.T) {
	writer := &recordingEventWriter{}
	worker := NewWorker("svc",
		WithProjectID("proj-1"),
		withEventWriter(writer),
	)
	planExecutions := 0
	conductExecutions := 0
	if err := RegisterWorkflow(worker, "deep_research_workflow", func(ctx *Context, _ map[string]any) (map[string]any, error) {
		plan, err := Step(ctx, "plan_research", func(context.Context) (string, error) {
			planExecutions++
			return "plan", nil
		})
		if err != nil {
			return nil, err
		}
		approval, err := ctx.AskUser(UserInputRequest{
			Prompt: "Approve the plan?",
			Type:   HITLApproval,
		})
		if err != nil {
			return nil, err
		}
		research, err := Step(ctx, "conduct_research", func(context.Context) (string, error) {
			conductExecutions++
			return "research", nil
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"plan":     plan,
			"approval": approval,
			"research": research,
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	initial := hitlDispatchRequest("12345678-aaaa:invoke", "deep_research_workflow", nil)
	paused := requireDispatchResponse(t, worker.dispatchServiceMessages(context.Background(), initial))
	if !paused.GetSuccess() || paused.GetEventType() != "workflow.paused" {
		t.Fatalf("pause response = %#v", paused)
	}

	initialEvents := writer.Events()
	requireEventTypes(t, initialEvents, []string{
		"run.started",
		"workflow.started",
		"workflow.step.started",
		"workflow.step.completed",
		"workflow.step.started",
		"approval.requested",
		"workflow.step.paused",
		"workflow.paused",
	})
	runCorrelationID := initialEvents[0].CorrelationID
	workflowCorrelationID := initialEvents[1].CorrelationID
	waitCorrelationID := initialEvents[4].CorrelationID
	if runCorrelationID != "12345678" {
		t.Fatalf("run correlation id = %q", runCorrelationID)
	}
	if initialEvents[1].ParentCorrelationID != runCorrelationID ||
		initialEvents[4].ParentCorrelationID != workflowCorrelationID ||
		initialEvents[6].CorrelationID != waitCorrelationID ||
		initialEvents[7].CorrelationID != workflowCorrelationID ||
		initialEvents[7].ParentCorrelationID != runCorrelationID {
		t.Fatalf("initial lifecycle correlation = %#v", initialEvents)
	}
	if paused.GetMetadata()["workflow_correlation_id"] != workflowCorrelationID ||
		paused.GetMetadata()["step_correlation_id"] != waitCorrelationID {
		t.Fatalf("pause response metadata = %#v", paused.GetMetadata())
	}
	for _, eventLocalKey := range []string{"cid", "pcid", "correlation_id", "parent_correlation_id", "name"} {
		if value := paused.GetMetadata()[eventLocalKey]; value != "" {
			t.Fatalf("pause response leaked event-local metadata %s=%q", eventLocalKey, value)
		}
	}

	resumeMetadata := cloneStringMap(paused.GetMetadata())
	resumeMetadata["user_response"] = "approve"
	resumed := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest(initial.GetInvocationId(), "deep_research_workflow", resumeMetadata),
	))
	if !resumed.GetSuccess() || resumed.GetEventType() != "run.completed" {
		t.Fatalf("resume response = %#v", resumed)
	}
	if planExecutions != 1 || conductExecutions != 1 {
		t.Fatalf("executions: plan=%d conduct=%d", planExecutions, conductExecutions)
	}

	allEvents := writer.Events()
	requireEventTypes(t, allEvents, []string{
		"run.started",
		"workflow.started",
		"workflow.step.started",
		"workflow.step.completed",
		"workflow.step.started",
		"approval.requested",
		"workflow.step.paused",
		"workflow.paused",
		"workflow.resumed",
		"workflow.step.completed",
		"workflow.step.started",
		"workflow.step.completed",
		"workflow.completed",
	})
	if countJournalEvents(allEvents, "run.started", "") != 1 ||
		countJournalEvents(allEvents, "workflow.started", "") != 1 ||
		countJournalEvents(allEvents, "workflow.step.started", "plan_research") != 1 ||
		countJournalEvents(allEvents, "workflow.step.completed", "plan_research") != 1 ||
		countJournalEvents(allEvents, "workflow.step.started", "conduct_research") != 1 ||
		countJournalEvents(allEvents, "workflow.step.completed", "conduct_research") != 1 {
		t.Fatalf("duplicate root or replayed plan events = %#v", journalEventSummary(allEvents))
	}
	resumedEvent := allEvents[8]
	waitCompleted := allEvents[9]
	conductStarted := allEvents[10]
	workflowCompleted := allEvents[12]
	if resumedEvent.CorrelationID != workflowCorrelationID ||
		resumedEvent.ParentCorrelationID != runCorrelationID ||
		waitCompleted.CorrelationID != waitCorrelationID ||
		waitCompleted.ParentCorrelationID != workflowCorrelationID ||
		conductStarted.ParentCorrelationID != workflowCorrelationID ||
		workflowCompleted.CorrelationID != workflowCorrelationID ||
		workflowCompleted.ParentCorrelationID != runCorrelationID {
		t.Fatalf("resumed lifecycle correlation = %#v", allEvents[8:])
	}
}

func TestHITLPauseFailsClosedWhenDurableEventFlushFails(t *testing.T) {
	flushErr := errors.New("append batch unavailable")
	writer := &failingInvocationBatchWriter{err: flushErr}
	worker := NewWorker("svc", withEventWriter(writer))
	if err := RegisterWorkflow(worker, "approval", func(ctx *Context, _ map[string]any) (string, error) {
		return ctx.AskUser(UserInputRequest{Prompt: "Approve?", Type: HITLApproval})
	}); err != nil {
		t.Fatal(err)
	}

	response := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest("12345678-aaaa:invoke", "approval", nil),
	))
	if response.GetSuccess() || response.GetEventType() != "run.failed" {
		t.Fatalf("pause response must fail closed when events are not durable: %#v", response)
	}
	if !strings.Contains(response.GetErrorMessage(), flushErr.Error()) {
		t.Fatalf("pause response error = %q, want %q", response.GetErrorMessage(), flushErr)
	}
}

func TestPullHITLPauseLeavesTerminalRecordToCompleteJob(t *testing.T) {
	writer := &recordingEventWriter{}
	worker := NewWorker("svc", withEventWriter(writer))
	if err := RegisterWorkflow(worker, "approval", func(ctx *Context, _ map[string]any) (string, error) {
		return ctx.AskUser(UserInputRequest{Prompt: "Approve?", Type: HITLApproval})
	}); err != nil {
		t.Fatal(err)
	}

	response := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest("12345678-aaaa:invoke", "approval", map[string]string{
			"dispatch_mode": "pull",
		}),
	))
	if !response.GetSuccess() || response.GetEventType() != "workflow.paused" {
		t.Fatalf("pause response = %#v", response)
	}
	requireEventTypes(t, writer.Events(), []string{
		"run.started",
		"workflow.started",
		"workflow.step.started",
		"approval.requested",
		"workflow.step.paused",
	})
}

func TestHITLResumeFailureUsesOriginalWorkflowCorrelationID(t *testing.T) {
	writer := &recordingEventWriter{}
	worker := NewWorker("svc", withEventWriter(writer))
	resumeErr := errors.New("research failed")
	if err := RegisterWorkflow(worker, "failing_workflow", func(ctx *Context, _ map[string]any) (map[string]any, error) {
		if _, err := ctx.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLApproval}); err != nil {
			return nil, err
		}
		return nil, resumeErr
	}); err != nil {
		t.Fatal(err)
	}

	request := hitlDispatchRequest("failure-run:invoke", "failing_workflow", nil)
	paused := requireDispatchResponse(t, worker.dispatchServiceMessages(context.Background(), request))
	events := writer.Events()
	workflowCorrelationID := events[1].CorrelationID
	runCorrelationID := events[0].CorrelationID

	resumeMetadata := cloneStringMap(paused.GetMetadata())
	resumeMetadata["user_response"] = "approve"
	failed := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest(request.GetInvocationId(), "failing_workflow", resumeMetadata),
	))
	if failed.GetSuccess() || failed.GetEventType() != "run.failed" ||
		failed.GetErrorMessage() != resumeErr.Error() {
		t.Fatalf("failed resume response = %#v", failed)
	}
	events = writer.Events()
	if countJournalEvents(events, "run.started", "") != 1 ||
		countJournalEvents(events, "workflow.started", "") != 1 {
		t.Fatalf("duplicate roots on failed resume = %#v", events)
	}
	workflowFailed := requireJournalEvent(t, events, "workflow.failed", "failing_workflow")
	if workflowFailed.CorrelationID != workflowCorrelationID ||
		workflowFailed.ParentCorrelationID != runCorrelationID {
		t.Fatalf("workflow.failed correlation = %#v", workflowFailed)
	}
}

type failingInvocationBatchWriter struct {
	recordingEventWriter
	err error
}

func (w *failingInvocationBatchWriter) WriteEvents(context.Context, []journalEvent) error {
	return w.err
}

func TestHITLMultiPauseCompletesOnlyCurrentWaitStep(t *testing.T) {
	writer := &recordingEventWriter{}
	worker := NewWorker("svc", withEventWriter(writer))
	if err := RegisterWorkflow(worker, "multi_pause", func(ctx *Context, _ map[string]any) (map[string]any, error) {
		first, err := ctx.AskUser(UserInputRequest{Prompt: "First?", Type: HITLText})
		if err != nil {
			return nil, err
		}
		second, err := ctx.AskUser(UserInputRequest{Prompt: "Second?", Type: HITLText})
		if err != nil {
			return nil, err
		}
		return map[string]any{"first": first, "second": second}, nil
	}); err != nil {
		t.Fatal(err)
	}

	request := hitlDispatchRequest("multi-pause-run:invoke", "multi_pause", nil)
	pauseZero := requireDispatchResponse(t, worker.dispatchServiceMessages(context.Background(), request))
	waitZeroCorrelationID := pauseZero.GetMetadata()["step_correlation_id"]

	resumeZeroMetadata := cloneStringMap(pauseZero.GetMetadata())
	resumeZeroMetadata["user_response"] = "one"
	pauseOne := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest(request.GetInvocationId(), "multi_pause", resumeZeroMetadata),
	))
	if pauseOne.GetEventType() != "workflow.paused" ||
		pauseOne.GetMetadata()["pause_index"] != "1" {
		t.Fatalf("second pause response = %#v", pauseOne)
	}
	waitOneCorrelationID := pauseOne.GetMetadata()["step_correlation_id"]
	if waitOneCorrelationID == "" || waitOneCorrelationID == waitZeroCorrelationID {
		t.Fatalf("wait correlation IDs: zero=%q one=%q", waitZeroCorrelationID, waitOneCorrelationID)
	}

	resumeOneMetadata := cloneStringMap(pauseOne.GetMetadata())
	resumeOneMetadata["user_response"] = "two"
	completed := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest(request.GetInvocationId(), "multi_pause", resumeOneMetadata),
	))
	if !completed.GetSuccess() || completed.GetEventType() != "run.completed" {
		t.Fatalf("multi-pause completion = %#v", completed)
	}

	events := writer.Events()
	if countJournalEvents(events, "run.started", "") != 1 ||
		countJournalEvents(events, "workflow.started", "") != 1 ||
		countJournalEvents(events, "workflow.resumed", "") != 2 ||
		countJournalEvents(events, "workflow.step.started", "wait_for_user_0") != 1 ||
		countJournalEvents(events, "workflow.step.completed", "wait_for_user_0") != 1 ||
		countJournalEvents(events, "workflow.step.started", "wait_for_user_1") != 1 ||
		countJournalEvents(events, "workflow.step.completed", "wait_for_user_1") != 1 {
		t.Fatalf("multi-pause lifecycle counts = %#v", journalEventSummary(events))
	}
	waitZeroCompleted := requireJournalEvent(t, events, "workflow.step.completed", "wait_for_user_0")
	waitOneCompleted := requireJournalEvent(t, events, "workflow.step.completed", "wait_for_user_1")
	if waitZeroCompleted.CorrelationID != waitZeroCorrelationID ||
		waitOneCompleted.CorrelationID != waitOneCorrelationID {
		t.Fatalf(
			"multi-pause completion correlation: zero=%q/%q one=%q/%q",
			waitZeroCompleted.CorrelationID,
			waitZeroCorrelationID,
			waitOneCompleted.CorrelationID,
			waitOneCorrelationID,
		)
	}
}

func TestHITLResumeWithoutStoredWorkflowCorrelationUsesFreshRoot(t *testing.T) {
	writer := &recordingEventWriter{}
	worker := NewWorker("svc", withEventWriter(writer))
	if err := RegisterWorkflow(worker, "legacy_resume", func(ctx *Context, _ map[string]any) (map[string]any, error) {
		answer, err := ctx.AskUser(UserInputRequest{Prompt: "Continue?", Type: HITLText})
		if err != nil {
			return nil, err
		}
		return map[string]any{"answer": answer}, nil
	}); err != nil {
		t.Fatal(err)
	}

	response := requireDispatchResponse(t, worker.dispatchServiceMessages(
		context.Background(),
		hitlDispatchRequest("short:invoke", "legacy_resume", map[string]string{
			"pause_reason":  "user_input_required",
			"pause_index":   "0",
			"user_response": "legacy",
		}),
	))
	if !response.GetSuccess() {
		t.Fatalf("legacy resume response = %#v", response)
	}
	events := writer.Events()
	if countJournalEvents(events, "run.started", "") != 1 ||
		countJournalEvents(events, "workflow.started", "") != 1 {
		t.Fatalf("legacy resume should create a complete fresh root = %#v", events)
	}
	if events[0].CorrelationID != "short" {
		t.Fatalf("short run correlation ID = %q", events[0].CorrelationID)
	}
}

func hitlDispatchRequest(invocationID, componentName string, metadata map[string]string) *pb.DispatchComponentRequest {
	return &pb.DispatchComponentRequest{
		InvocationId:  invocationID,
		ComponentName: componentName,
		ComponentType: pb.ComponentType_COMPONENT_TYPE_WORKFLOW,
		InputData:     []byte(`{}`),
		Metadata:      cloneStringMap(metadata),
		LeaseId:       "lease-hitl",
	}
}

func requireDispatchResponse(t *testing.T, messages []*pb.ServiceMessage) *pb.DispatchComponentResponse {
	t.Helper()
	for index := len(messages) - 1; index >= 0; index-- {
		if response := messages[index].GetFunctionResponse(); response != nil {
			return response
		}
	}
	t.Fatalf("missing dispatch response: %#v", messages)
	return nil
}

func requireEventTypes(t *testing.T, events []journalEvent, want []string) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(want), journalEventSummary(events))
	}
	for index, eventType := range want {
		if events[index].EventType != eventType {
			t.Fatalf(
				"event[%d] = %q, want %q: %#v",
				index,
				events[index].EventType,
				eventType,
				journalEventSummary(events),
			)
		}
	}
}

func countJournalEvents(events []journalEvent, eventType, name string) int {
	count := 0
	for _, event := range events {
		if event.EventType != eventType {
			continue
		}
		if name != "" && journalEventName(event) != name {
			continue
		}
		count++
	}
	return count
}

func requireJournalEvent(t *testing.T, events []journalEvent, eventType, name string) journalEvent {
	t.Helper()
	for _, event := range events {
		if event.EventType == eventType && (name == "" || journalEventName(event) == name) {
			return event
		}
	}
	t.Fatalf("missing %s/%s event: %#v", eventType, name, journalEventSummary(events))
	return journalEvent{}
}

func journalEventSummary(events []journalEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, fmt.Sprintf(
			"%s:%s:%s:%s",
			event.EventType,
			journalEventName(event),
			event.CorrelationID,
			event.ParentCorrelationID,
		))
	}
	return out
}

func journalEventName(event journalEvent) string {
	if name := event.Metadata["name"]; name != "" {
		return name
	}
	var data map[string]any
	if err := json.Unmarshal(event.Data, &data); err == nil {
		if name, ok := data["name"].(string); ok {
			return name
		}
	}
	return ""
}
