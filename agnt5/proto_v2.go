package agnt5

import (
	"fmt"
	"strings"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

const portableSchemaDialect = "https://json-schema.org/draft/2020-12/schema"

func (w *Worker) v2RegisterWorkerRequest() (*protocolv2.RegisterWorkerRequest, error) {
	components, err := v2ComponentDescriptors(w.serviceVersion, w.Components())
	if err != nil {
		return nil, err
	}
	maxConcurrency := w.maxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 1
	}
	return &protocolv2.RegisterWorkerRequest{
		WorkerId:        w.workerID,
		ServiceName:     w.serviceName,
		ServiceVersion:  w.serviceVersion,
		SdkLanguage:     "go",
		SdkVersion:      defaultServiceVersion,
		MinimumProtocol: newV2ProtocolVersion(),
		MaximumProtocol: newV2ProtocolVersion(),
		Components:      components,
		MaxConcurrency:  maxConcurrency,
		Metadata:        v2PublicMetadata(w.Metadata()),
	}, nil
}

func v2ComponentDescriptors(version string, infos []ComponentInfo) ([]*protocolv2.ComponentDescriptor, error) {
	if strings.TrimSpace(version) == "" {
		return nil, fmt.Errorf("agnt5: v2 component version is required")
	}
	out := make([]*protocolv2.ComponentDescriptor, 0, len(infos))
	for _, info := range infos {
		componentType, err := v2ComponentType(info.Type)
		if err != nil {
			return nil, fmt.Errorf("agnt5: component %q: %w", info.Name, err)
		}
		triggers, err := v2TriggerDescriptors(info)
		if err != nil {
			return nil, fmt.Errorf("agnt5: component %q: %w", info.Name, err)
		}
		inputSchema := []byte(info.Config["input_schema_json"])
		if len(inputSchema) == 0 {
			inputSchema = []byte(`{}`)
		}
		outputSchema := []byte(info.Config["output_schema_json"])
		if len(outputSchema) == 0 {
			outputSchema = []byte(`{}`)
		}
		out = append(out, &protocolv2.ComponentDescriptor{
			Type:              componentType,
			Name:              info.Name,
			Version:           version,
			InputSchemaJson:   inputSchema,
			OutputSchemaJson:  outputSchema,
			Metadata:          v2PublicMetadata(info.Metadata),
			ExecutionDefaults: v2ExecutionDefaults(info.Config),
			Triggers:          triggers,
			SchemaDialect:     portableSchemaDialect,
		})
	}
	return out, nil
}

func v2ComponentType(componentType ComponentType) (protocolv2.ComponentType, error) {
	switch componentType {
	case ComponentTypeFunction:
		return protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION, nil
	case ComponentTypeWorkflow:
		return protocolv2.ComponentType_COMPONENT_TYPE_WORKFLOW, nil
	case ComponentTypeEntity:
		return protocolv2.ComponentType_COMPONENT_TYPE_ENTITY, nil
	case ComponentTypeAgent:
		return protocolv2.ComponentType_COMPONENT_TYPE_AGENT, nil
	case ComponentTypeTool:
		return protocolv2.ComponentType_COMPONENT_TYPE_TOOL, nil
	case ComponentTypeScorer:
		return protocolv2.ComponentType_COMPONENT_TYPE_SCORER, nil
	default:
		return protocolv2.ComponentType_COMPONENT_TYPE_UNSPECIFIED, fmt.Errorf("component type %q is not supported by protocol v2", componentType)
	}
}

func componentTypeFromV2(componentType protocolv2.ComponentType) ComponentType {
	switch componentType {
	case protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION:
		return ComponentTypeFunction
	case protocolv2.ComponentType_COMPONENT_TYPE_WORKFLOW:
		return ComponentTypeWorkflow
	case protocolv2.ComponentType_COMPONENT_TYPE_ENTITY:
		return ComponentTypeEntity
	case protocolv2.ComponentType_COMPONENT_TYPE_AGENT:
		return ComponentTypeAgent
	case protocolv2.ComponentType_COMPONENT_TYPE_TOOL:
		return ComponentTypeTool
	case protocolv2.ComponentType_COMPONENT_TYPE_SCORER:
		return ComponentTypeScorer
	default:
		return ""
	}
}

func v2ExecutionDefaults(config map[string]string) *protocolv2.ExecutionDefaults {
	maximumAttempts, hasAttempts := int32Config(config, "max_attempts")
	initialMS, hasInitial := int32Config(config, "initial_interval_ms")
	maximumMS, hasMaximum := int32Config(config, "max_interval_ms")
	multiplier, hasMultiplier := float64Config(config, "backoff_multiplier")
	backoffType := config["backoff_type"]
	if !hasAttempts && !hasInitial && !hasMaximum && !hasMultiplier && backoffType == "" {
		return nil
	}
	retry := &protocolv2.RetryPolicy{}
	if maximumAttempts > 0 {
		retry.MaximumAttempts = uint32(maximumAttempts)
	}
	if initialMS > 0 {
		retry.InitialBackoff = durationpb.New(time.Duration(initialMS) * time.Millisecond)
	}
	if maximumMS > 0 {
		retry.MaximumBackoff = durationpb.New(time.Duration(maximumMS) * time.Millisecond)
	}
	if multiplier > 0 {
		retry.BackoffMultiplier = multiplier
	}
	switch strings.ToLower(backoffType) {
	case "constant":
		retry.BackoffStrategy = protocolv2.RetryBackoffStrategy_RETRY_BACKOFF_STRATEGY_CONSTANT
	case "linear":
		retry.BackoffStrategy = protocolv2.RetryBackoffStrategy_RETRY_BACKOFF_STRATEGY_LINEAR
	case "exponential":
		retry.BackoffStrategy = protocolv2.RetryBackoffStrategy_RETRY_BACKOFF_STRATEGY_EXPONENTIAL
	}
	return &protocolv2.ExecutionDefaults{RetryPolicy: retry}
}

func v2TriggerDescriptors(info ComponentInfo) ([]*protocolv2.TriggerDescriptor, error) {
	out := make([]*protocolv2.TriggerDescriptor, 0, len(info.Triggers)+1)
	for index, trigger := range info.Triggers {
		if trigger.TriggerType != "event" {
			return nil, fmt.Errorf("trigger type %q is not supported by the v2 Go adapter", trigger.TriggerType)
		}
		triggerID := trigger.TriggerID
		if triggerID == "" {
			triggerID = fmt.Sprintf("%s-event-%d", info.Name, index+1)
		}
		event := &protocolv2.EventTrigger{EventName: trigger.EventName}
		if trigger.FilterExpression != "" {
			event.Filter = &protocolv2.TriggerExpression{Language: "cel.v1", Source: trigger.FilterExpression}
		}
		if trigger.InputMapping != "" {
			event.InputMapping = &protocolv2.TriggerExpression{Language: "cel.v1", Source: trigger.InputMapping}
		}
		if trigger.BatchWindowMS > 0 {
			event.BatchWindow = durationpb.New(time.Duration(trigger.BatchWindowMS) * time.Millisecond)
			event.MaximumBatchSize = 100
		}
		if trigger.DelayExpression != "" {
			event.Delay = &protocolv2.TriggerExpression{Language: "cel.v1", Source: trigger.DelayExpression}
		}
		out = append(out, &protocolv2.TriggerDescriptor{
			TriggerId: triggerID,
			Kind:      &protocolv2.TriggerDescriptor_Event{Event: event},
		})
	}
	if expression := strings.TrimSpace(info.Metadata["cron"]); expression != "" {
		out = append(out, &protocolv2.TriggerDescriptor{
			TriggerId: info.Name + "-cron",
			Kind: &protocolv2.TriggerDescriptor_Schedule{Schedule: &protocolv2.ScheduleTrigger{
				Schedule: &protocolv2.ScheduleTrigger_Cron{Cron: &protocolv2.CronSchedule{
					Expression: expression,
					TimeZone:   "UTC",
					Format:     "unix-cron.v1",
				}},
			}},
		})
	}
	return out, nil
}

func invocationFromV2Execute(request *protocolv2.ExecuteRunRequest) (Invocation, error) {
	if request == nil {
		return Invocation{}, fmt.Errorf("agnt5: v2 execution request is required")
	}
	if len(request.GetExecutionToken()) == 0 {
		return Invocation{}, fmt.Errorf("agnt5: v2 execution token is required")
	}
	target := request.GetTarget()
	if target == nil || target.GetName() == "" || target.GetVersion() == "" {
		return Invocation{}, fmt.Errorf("agnt5: v2 execution target name and version are required")
	}
	componentType := componentTypeFromV2(target.GetType())
	if componentType == "" {
		return Invocation{}, fmt.Errorf("agnt5: unsupported v2 component type %s", target.GetType())
	}
	input, err := inlineV2Payload(request.GetInput())
	if err != nil {
		return Invocation{}, err
	}
	if checkpoint := request.GetCheckpoint(); checkpoint != nil && checkpoint.GetBody() != nil {
		return Invocation{}, fmt.Errorf("%w: v2 checkpoint replay is not available in the alpha.3 runtime bridge", ErrTransportNotImplemented)
	}
	if request.GetCheckpointThroughOperationSequence() != 0 {
		return Invocation{}, fmt.Errorf("%w: v2 durable operation replay is not available in the alpha.3 runtime bridge", ErrTransportNotImplemented)
	}
	metadata := cloneStringMap(request.GetMetadata())
	if trace := request.GetTraceContext(); trace != nil {
		metadata["traceparent"] = trace.GetTraceparent()
		metadata["tracestate"] = trace.GetTracestate()
		metadata["baggage"] = trace.GetBaggage()
	}
	if application := request.GetApplicationContext(); application != nil {
		metadata["session_id"] = application.GetSessionId()
		metadata["user_id"] = application.GetUserId()
	}
	invocationID := request.GetExecutionId()
	if invocationID == "" {
		invocationID = request.GetRunId()
	}
	return Invocation{
		ID:            invocationID,
		RunID:         request.GetRunId(),
		ComponentName: target.GetName(),
		ComponentType: componentType,
		Input:         input,
		Attempt:       int(request.GetAttempt()),
		Metadata:      metadata,
	}, nil
}

func inlineV2Payload(payload *protocolv2.Payload) ([]byte, error) {
	if payload == nil || payload.GetBody() == nil {
		return nil, nil
	}
	switch body := payload.GetBody().(type) {
	case *protocolv2.Payload_InlineData:
		return cloneBytes(body.InlineData), nil
	case *protocolv2.Payload_Reference:
		return nil, fmt.Errorf("%w: referenced v2 payloads require payload.transfer", ErrTransportNotImplemented)
	default:
		return nil, fmt.Errorf("agnt5: unsupported v2 payload body")
	}
}

func v2OutcomeFromResult(result InvocationResult, invokeErr error) (*protocolv2.RunOutcome, error) {
	if IsWaitingForUserInput(invokeErr) {
		return nil, fmt.Errorf("%w: suspended v2 outcomes are not available in the alpha.3 runtime", ErrTransportNotImplemented)
	}
	if invokeErr != nil {
		return &protocolv2.RunOutcome{Kind: &protocolv2.RunOutcome_Failed{Failed: &protocolv2.ExecutionFailed{
			Failure: &protocolv2.Failure{
				Code:           "WORKER_EXECUTION_FAILED",
				Message:        invokeErr.Error(),
				Kind:           protocolv2.FailureKind_FAILURE_KIND_APPLICATION,
				RetryDirective: protocolv2.RetryDirective_RETRY_DIRECTIVE_DEFAULT_POLICY,
			},
			Metadata: cloneStringMap(result.Metadata),
		}}}, nil
	}
	return &protocolv2.RunOutcome{Kind: &protocolv2.RunOutcome_Completed{Completed: &protocolv2.RunCompleted{
		Output: &protocolv2.Payload{
			Body:        &protocolv2.Payload_InlineData{InlineData: cloneBytes(result.Output)},
			ContentType: "application/json",
			SizeBytes:   uint64(len(result.Output)),
		},
		Metadata: cloneStringMap(result.Metadata),
	}}}, nil
}

func v2PublicMetadata(metadata map[string]string) map[string]string {
	out := make(map[string]string)
	for key, value := range metadata {
		normalized := strings.ToLower(key)
		if normalized == "project_id" || normalized == "deployment_id" || normalized == "tenant_id" ||
			normalized == strings.ToLower(envProjectID) || normalized == strings.ToLower(envDeploymentID) ||
			strings.Contains(normalized, "token") || strings.Contains(normalized, "secret") ||
			strings.Contains(normalized, "password") || strings.Contains(normalized, "authorization") {
			continue
		}
		out[key] = value
	}
	return out
}

func v2CommitID(request *protocolv2.ExecuteRunRequest) string {
	if request.GetExecutionId() != "" {
		return request.GetExecutionId() + ":outcome"
	}
	return request.GetRunId() + ":attempt:" + fmt.Sprint(request.GetAttempt()) + ":outcome"
}

func annotateUnsupportedV2Events(result *InvocationResult) {
	if result == nil || len(result.Events) == 0 {
		return
	}
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}
	result.Metadata["agnt5.protocol.omitted_event_count"] = fmt.Sprint(len(result.Events))
	result.Metadata["agnt5.protocol.event_transport"] = "unavailable"
}
