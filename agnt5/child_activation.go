package agnt5

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func runDelegatedChild(
	ctx *Context,
	target *Agent,
	input AgentInput,
	joinPolicy ChildJoinPolicy,
) (AgentResult, error) {
	if ctx.Metadata(durableActivationV1Capability) != "true" {
		return target.Run(ctx, input)
	}
	if ctx.activationWriter == nil {
		return AgentResult{}, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"runtime negotiated durable_activation_v1 but no activation writer is configured",
			"",
			0,
			nil,
		)
	}
	protoJoinPolicy, err := childJoinPolicyProto(joinPolicy)
	if err != nil {
		return AgentResult{}, err
	}
	parentDefinition, err := activationDefinitionDigestFromContext(ctx)
	if err != nil {
		return AgentResult{}, err
	}
	childDefinition := childActivationDefinitionDigest(parentDefinition, target.Name)
	canonicalInput, err := canonicalActivationValue(map[string]any{
		"agent":   target.Name,
		"message": input.Message,
		"history": input.Messages,
	})
	if err != nil {
		return AgentResult{}, err
	}
	inputDigest := sha256.Sum256(canonicalInput)
	stableKey := ctx.nextActivationKey("child", target.Name)
	parentID := parentActivationID(ctx)
	expectedID := activationID(
		ctx.projectID,
		ctx.RunID(),
		parentID,
		pb.ActivationKind_ACTIVATION_KIND_CHILD,
		stableKey,
	)
	suffix := strings.TrimPrefix(expectedID, "actv1_")
	childSessionID := ctx.Metadata("session_id")
	if childSessionID == "" {
		childSessionID = "session_" + suffix
	}
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
		return AgentResult{}, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"durable child activation requires project, run, worker-session, run, and lease authority",
			"",
			0,
			nil,
		)
	}
	begin, err := ctx.activationWriter.BeginActivation(ctx, &pb.BeginActivationRequest{
		ProjectId:          ctx.projectID,
		RunId:              ctx.RunID(),
		ParentActivationId: parentID,
		Kind:               pb.ActivationKind_ACTIVATION_KIND_CHILD,
		StableKey:          stableKey,
		InputDigest:        inputDigest[:],
		DefinitionDigest:   cloneBytes(childDefinition),
		RecoveryPolicy:     pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_DURABLE_STEPS,
		WorkerSessionId:    workerSessionID,
		RunAuthority:       []byte(runAuthority),
		LeaseAuthority:     []byte(leaseAuthority),
		Child: &pb.ChildActivationLinkage{
			ChildKey:              stableKey,
			ChildRunId:            "child_" + suffix,
			ChildSessionId:        childSessionID,
			ChildDefinitionDigest: cloneBytes(childDefinition),
			JoinPolicy:            protoJoinPolicy,
		},
	})
	if err != nil {
		return AgentResult{}, err
	}
	if begin.GetActivationId() != expectedID {
		return AgentResult{}, newActivationError(
			ActivationErrorUnknownOutcome,
			fmt.Sprintf("runtime returned child activation ID %q, want %q", begin.GetActivationId(), expectedID),
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	if begin.GetOutcome() == pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY {
		payload, payloadErr := inlineActivationBytes(begin.GetReplayResult())
		if payloadErr != nil {
			return AgentResult{}, payloadErr
		}
		var replayed AgentResult
		if err := json.Unmarshal(payload, &replayed); err != nil {
			return AgentResult{}, fmt.Errorf("agnt5: decode replayed child %q: %w", target.Name, err)
		}
		return replayed, nil
	}
	if begin.GetOutcome() != pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE {
		return AgentResult{}, activationBeginDecisionError(begin, "child")
	}
	if begin.GetAttempt() == 0 || len(begin.GetFenceToken()) == 0 {
		return AgentResult{}, newActivationError(
			ActivationErrorUnknownOutcome,
			"child EXECUTE receipt is missing fenced authority",
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
	result, childErr := target.Run(ctx.withActivationExecution(execution), input)
	if childErr != nil {
		errorData, _ := json.Marshal(map[string]string{
			"message": childErr.Error(),
			"type":    fmt.Sprintf("%T", childErr),
		})
		failed, failErr := ctx.activationWriter.FailActivation(ctx, &pb.FailActivationRequest{
			ProjectId:                ctx.projectID,
			RunId:                    ctx.RunID(),
			ActivationId:             begin.GetActivationId(),
			Attempt:                  begin.GetAttempt(),
			FenceToken:               cloneBytes(begin.GetFenceToken()),
			ErrorCode:                "CHILD_FAILED",
			ErrorData:                inlineActivationPayload(errorData),
			Retryable:                true,
			ExternalOutcomeCertainty: pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN,
		})
		if failErr != nil {
			return AgentResult{}, fmt.Errorf("agnt5: record child %q failure: %w (child error: %v)", target.Name, failErr, childErr)
		}
		if !failed.GetAccepted() || failed.GetActivationId() != begin.GetActivationId() || failed.GetAttempt() != begin.GetAttempt() {
			return AgentResult{}, newActivationError(
				ActivationErrorUnknownOutcome,
				"runtime returned an invalid child failure receipt",
				begin.GetActivationId(),
				begin.GetAttempt(),
				childErr,
			)
		}
		return AgentResult{}, childErr
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return AgentResult{}, fmt.Errorf("agnt5: encode child %q output: %w", target.Name, err)
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
		return AgentResult{}, err
	}
	if !completed.GetAccepted() || completed.GetActivationId() != begin.GetActivationId() || completed.GetAttempt() != begin.GetAttempt() {
		return AgentResult{}, newActivationError(
			ActivationErrorUnknownOutcome,
			"runtime returned an invalid child completion receipt",
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	return result, nil
}
