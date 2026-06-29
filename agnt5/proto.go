package agnt5

import (
	"strconv"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
)

func (w *Worker) registrationServiceMessage() *pb.ServiceMessage {
	return &pb.ServiceMessage{
		WorkerId: w.workerID,
		Metadata: w.Metadata(),
		MessageType: &pb.ServiceMessage_RegisterService{
			RegisterService: w.registerService(),
		},
	}
}

func (w *Worker) registerService() *pb.RegisterService {
	components := w.Components()
	return &pb.RegisterService{
		ServiceName:    w.serviceName,
		ServiceVersion: w.serviceVersion,
		ServiceType:    w.serviceType,
		Components:     protoComponentInfos(components),
		Metadata:       w.Metadata(),
		Mode:           protoWorkerMode(w.workerMode),
		DeploymentId:   w.deploymentID,
		MaxConcurrency: w.maxConcurrency,
		Capabilities:   protoCapabilities(components),
	}
}

func protoComponentInfos(infos []ComponentInfo) []*pb.ComponentInfo {
	out := make([]*pb.ComponentInfo, len(infos))
	for i, info := range infos {
		out[i] = protoComponentInfo(info)
	}
	return out
}

func protoComponentInfo(info ComponentInfo) *pb.ComponentInfo {
	out := &pb.ComponentInfo{
		Name:          info.Name,
		ComponentType: protoComponentType(info.Type),
		Config:        cloneStringMap(info.Config),
		Metadata:      cloneStringMap(info.Metadata),
		Triggers:      protoTriggerSpecs(info.Triggers),
	}
	if value, ok := int32Config(info.Config, "max_attempts"); ok {
		out.MaxAttempts = &value
	}
	if value, ok := int32Config(info.Config, "initial_interval_ms"); ok {
		out.InitialIntervalMs = &value
	}
	if value, ok := int32Config(info.Config, "max_interval_ms"); ok {
		out.MaxIntervalMs = &value
	}
	if value := info.Config["backoff_type"]; value != "" {
		out.BackoffType = &value
	}
	if value, ok := float64Config(info.Config, "backoff_multiplier"); ok {
		out.BackoffMultiplier = &value
	}
	return out
}

func protoTriggerSpecs(in []TriggerSpec) []*pb.TriggerSpec {
	out := make([]*pb.TriggerSpec, 0, len(in))
	for _, trigger := range in {
		if trigger.TriggerType == "" {
			continue
		}
		out = append(out, &pb.TriggerSpec{
			TriggerId:        trigger.TriggerID,
			TriggerType:      trigger.TriggerType,
			EventName:        trigger.EventName,
			FilterExpression: trigger.FilterExpression,
			InputMapping:     trigger.InputMapping,
			BatchWindowMs:    trigger.BatchWindowMS,
			DelayExpression:  trigger.DelayExpression,
		})
	}
	return out
}

func protoCapabilities(infos []ComponentInfo) []*pb.WorkerCapability {
	out := make([]*pb.WorkerCapability, len(infos))
	for i, info := range infos {
		out[i] = &pb.WorkerCapability{
			ComponentType: protoComponentType(info.Type),
			ComponentName: info.Name,
		}
	}
	return out
}

func protoComponentType(componentType ComponentType) pb.ComponentType {
	switch componentType {
	case ComponentTypeFunction:
		return pb.ComponentType_COMPONENT_TYPE_FUNCTION
	case ComponentTypeWorkflow:
		return pb.ComponentType_COMPONENT_TYPE_WORKFLOW
	case ComponentTypeAgent:
		return pb.ComponentType_COMPONENT_TYPE_AGENT
	case ComponentTypeTool:
		return pb.ComponentType_COMPONENT_TYPE_TOOL
	case ComponentTypeMCP:
		return pb.ComponentType_COMPONENT_TYPE_MCP
	case ComponentTypeEntity:
		return pb.ComponentType_COMPONENT_TYPE_ENTITY
	case ComponentTypeScorer:
		return pb.ComponentType_COMPONENT_TYPE_SCORER
	default:
		return pb.ComponentType_COMPONENT_TYPE_UNSPECIFIED
	}
}

func protoWorkerMode(mode WorkerMode) pb.WorkerMode {
	switch mode {
	case WorkerModePull:
		return pb.WorkerMode_WORKER_MODE_PULL
	case WorkerModePush:
		return pb.WorkerMode_WORKER_MODE_PUSH
	default:
		return pb.WorkerMode_WORKER_MODE_UNSPECIFIED
	}
}

func int32Config(config map[string]string, key string) (int32, bool) {
	value, ok := config[key]
	if !ok || value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(parsed), true
}

func float64Config(config map[string]string, key string) (float64, bool) {
	value, ok := config[key]
	if !ok || value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
