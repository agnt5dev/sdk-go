package agnt5

import (
	"errors"
	"testing"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
)

func TestV2ComponentDescriptorMapping(t *testing.T) {
	descriptors, err := v2ComponentDescriptors("1.2.3", []ComponentInfo{{
		Name: "greet",
		Type: ComponentTypeWorkflow,
		Config: map[string]string{
			"max_attempts":        "3",
			"initial_interval_ms": "100",
			"max_interval_ms":     "5000",
			"backoff_type":        "exponential",
			"backoff_multiplier":  "2",
		},
		Metadata: map[string]string{"owner": "sdk", "cron": "*/5 * * * *"},
		Triggers: []TriggerSpec{{
			TriggerType:      "event",
			EventName:        "user.created",
			FilterExpression: "event.enabled",
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("descriptor count = %d", len(descriptors))
	}
	descriptor := descriptors[0]
	if descriptor.GetType() != protocolv2.ComponentType_COMPONENT_TYPE_WORKFLOW || descriptor.GetVersion() != "1.2.3" {
		t.Fatalf("descriptor identity: %#v", descriptor)
	}
	if string(descriptor.GetInputSchemaJson()) != "{}" || descriptor.GetSchemaDialect() != portableSchemaDialect {
		t.Fatalf("descriptor schema: %#v", descriptor)
	}
	if retry := descriptor.GetExecutionDefaults().GetRetryPolicy(); retry.GetMaximumAttempts() != 3 || retry.GetBackoffMultiplier() != 2 {
		t.Fatalf("retry mapping: %#v", retry)
	}
	if len(descriptor.GetTriggers()) != 2 {
		t.Fatalf("triggers: %#v", descriptor.GetTriggers())
	}
}

func TestV2PublicMetadataExcludesRoutingAndSecrets(t *testing.T) {
	metadata := v2PublicMetadata(map[string]string{
		"owner":                   "sdk",
		"project_id":              "project",
		"AGNT5_DEPLOYMENT_ID":     "deployment",
		"worker_session_token":    "secret",
		"authorization":           "secret",
		"agnt5.protocol.selected": "v2.0",
	})
	if metadata["owner"] != "sdk" || metadata["agnt5.protocol.selected"] != "v2.0" {
		t.Fatalf("public metadata: %#v", metadata)
	}
	if len(metadata) != 2 {
		t.Fatalf("private metadata leaked: %#v", metadata)
	}
}

func TestInvocationFromV2ExecuteKeepsTokenOutOfInvocation(t *testing.T) {
	invocation, err := invocationFromV2Execute(&protocolv2.ExecuteRunRequest{
		RunId:          "run-1",
		ExecutionId:    "execution-1",
		ExecutionToken: []byte("opaque-secret"),
		Target: &protocolv2.ComponentTarget{
			Type:    protocolv2.ComponentType_COMPONENT_TYPE_FUNCTION,
			Name:    "greet",
			Version: "1.2.3",
		},
		Input:   &protocolv2.Payload{Body: &protocolv2.Payload_InlineData{InlineData: []byte(`{"name":"Ada"}`)}},
		Attempt: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if invocation.ID != "execution-1" || invocation.RunID != "run-1" || invocation.ComponentName != "greet" {
		t.Fatalf("invocation: %#v", invocation)
	}
	if invocation.LeaseID != "" {
		t.Fatalf("execution authority leaked into invocation: %#v", invocation)
	}
}

func TestV2OutcomeMapping(t *testing.T) {
	completed, err := v2OutcomeFromResult(InvocationResult{Output: []byte(`{"ok":true}`)}, nil)
	if err != nil || completed.GetCompleted() == nil {
		t.Fatalf("completed outcome=%#v err=%v", completed, err)
	}
	failure := errors.New("boom")
	failed, err := v2OutcomeFromResult(InvocationResult{}, failure)
	if err != nil || failed.GetFailed().GetFailure().GetMessage() != "boom" {
		t.Fatalf("failed outcome=%#v err=%v", failed, err)
	}
}

func TestV2ExecutionDefaultsRejectInvalidRetryConfiguration(t *testing.T) {
	for name, config := range map[string]map[string]string{
		"missing attempts": {"initial_interval_ms": "100"},
		"zero attempts":    {"max_attempts": "0"},
		"invalid attempts": {"max_attempts": "many"},
		"inverted backoff": {"max_attempts": "3", "initial_interval_ms": "500", "max_interval_ms": "100"},
		"unknown strategy": {"max_attempts": "3", "backoff_type": "random"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := v2ExecutionDefaults(config); err == nil {
				t.Fatalf("expected config %#v to fail", config)
			}
		})
	}
}

func TestV2RunPolicyConfigIsRejectedInsteadOfDropped(t *testing.T) {
	if _, err := v2ComponentDescriptors("1.2.3", []ComponentInfo{{
		Name:   "greet",
		Type:   ComponentTypeFunction,
		Config: map[string]string{"run_policy_json": `{}`},
	}}); !errors.Is(err, ErrTransportNotImplemented) {
		t.Fatalf("run policy error = %v, want ErrTransportNotImplemented", err)
	}
}

func TestV2CapabilityRequirementsFollowTriggerDeclarations(t *testing.T) {
	requirements, err := v2CapabilityRequirements([]ComponentInfo{{
		Name:     "triggered",
		Type:     ComponentTypeWorkflow,
		Metadata: map[string]string{"cron": "*/5 * * * *"},
		Triggers: []TriggerSpec{{TriggerType: "event", EventName: "user.created", FilterExpression: "event.enabled"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]uint32, len(requirements))
	for _, requirement := range requirements {
		if !requirement.GetRequired() {
			t.Fatalf("capability %q is not required", requirement.GetName())
		}
		got[requirement.GetName()] = requirement.GetMinimumVersion()
	}
	for _, name := range []string{v2CapabilityDurableOperations, v2CapabilityTriggersEvent, v2CapabilityTriggerExpression, v2CapabilityTriggersSchedule} {
		if got[name] != 1 {
			t.Fatalf("capability %q = %d, want 1", name, got[name])
		}
	}
}

func TestV2CapabilityRequirementsKeepPlainFunctionsOnBasePull(t *testing.T) {
	requirements, err := v2CapabilityRequirements([]ComponentInfo{{
		Name: "greet",
		Type: ComponentTypeFunction,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(requirements) != 0 {
		t.Fatalf("plain function requirements = %#v, want base pull with no optional capabilities", requirements)
	}
}
