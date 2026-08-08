package agnt5

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

type modelActivationInput struct {
	Provider string          `json:"provider,omitempty"`
	Request  GenerateRequest `json:"request"`
}

type modelActivationFailureEvidence interface {
	activationFailureEvidence(attempt uint32) []*pb.ActivationEvidence
	activationFailureErrorCode() string
}

type activationStreamingModel struct {
	model            StreamingLanguageModel
	emit             func(ModelStreamChunk) error
	partialHash      hash.Hash
	partialChunks    int
	partialUTF8Bytes int
}

func (m *activationStreamingModel) Generate(
	ctx context.Context,
	request GenerateRequest,
) (GenerateResponse, error) {
	return m.model.Stream(ctx, request, func(chunk ModelStreamChunk) error {
		partial := chunk.Content
		if chunk.ArgumentsDelta != "" {
			partial = chunk.ArgumentsDelta
		}
		if partial != "" {
			_, _ = m.partialHash.Write([]byte(partial))
			m.partialChunks++
			m.partialUTF8Bytes += len([]byte(partial))
		}
		return m.emit(chunk)
	})
}

func (m *activationStreamingModel) activationFailureEvidence(attempt uint32) []*pb.ActivationEvidence {
	payload, _ := json.Marshal(struct {
		Schema           string `json:"schema"`
		Attempt          uint32 `json:"attempt"`
		PartialChunks    int    `json:"partial_chunks"`
		PartialUTF8Bytes int    `json:"partial_utf8_bytes"`
		PartialSHA256    string `json:"partial_sha256"`
		Classification   string `json:"classification"`
	}{
		Schema:           "agnt5.model_stream_interruption.v1",
		Attempt:          attempt,
		PartialChunks:    m.partialChunks,
		PartialUTF8Bytes: m.partialUTF8Bytes,
		PartialSHA256:    fmt.Sprintf("%x", m.partialHash.Sum(nil)),
		Classification:   "provider_interrupted",
	})
	return []*pb.ActivationEvidence{inlineModelEvidence("model_stream_interruption_v1", payload)}
}

func (m *activationStreamingModel) activationFailureErrorCode() string {
	return "MODEL_STREAM_INTERRUPTED"
}

func (c *Context) streamDurableModel(
	model StreamingLanguageModel,
	request GenerateRequest,
	modelName string,
	provider string,
	emit func(ModelStreamChunk) error,
) (GenerateResponse, error) {
	wrapper := &activationStreamingModel{
		model:       model,
		emit:        emit,
		partialHash: sha256.New(),
	}
	return c.generateDurableModel(wrapper, request, modelName, provider)
}

func inlineModelEvidence(evidenceType string, payload []byte) *pb.ActivationEvidence {
	digest := sha256.Sum256(payload)
	return &pb.ActivationEvidence{
		EvidenceType: evidenceType,
		Payload:      inlineActivationPayload(payload),
		Sha256:       digest[:],
	}
}

func modelTerminalEvidence(response GenerateResponse) []*pb.ActivationEvidence {
	payload, _ := json.Marshal(struct {
		Schema         string `json:"schema"`
		Classification string `json:"classification"`
		FinishReason   string `json:"finish_reason,omitempty"`
		ResponseID     string `json:"response_id,omitempty"`
	}{
		Schema:         "agnt5.model_provider_terminal.v1",
		Classification: "accepted_final",
		FinishReason:   response.FinishReason,
		ResponseID:     response.ID,
	})
	return []*pb.ActivationEvidence{inlineModelEvidence("model_provider_terminal_v1", payload)}
}

func (c *Context) generateDurableModel(
	model LanguageModel,
	request GenerateRequest,
	modelName string,
	provider string,
) (GenerateResponse, error) {
	if c.activationWriter == nil {
		return GenerateResponse{}, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"runtime negotiated durable_activation_v1 but no activation writer is configured",
			"",
			0,
			nil,
		)
	}
	policy := request.RecoveryPolicy
	if policy == "" {
		policy = RecoveryPolicyUnknownOutcome
	}
	protoPolicy, err := recoveryPolicyProto(policy)
	if err != nil {
		return GenerateResponse{}, err
	}
	canonicalRequest := request
	canonicalRequest.RecoveryPolicy = ""
	canonicalInput, err := canonicalActivationValue(modelActivationInput{
		Provider: provider,
		Request:  canonicalRequest,
	})
	if err != nil {
		return GenerateResponse{}, newActivationError(
			ActivationErrorInvalidArgument,
			err.Error(),
			"",
			0,
			err,
		)
	}
	definitionDigest, err := activationDefinitionDigestFromContext(c)
	if err != nil {
		return GenerateResponse{}, err
	}
	workerSessionID := c.Metadata("worker_session_id")
	if workerSessionID == "" {
		workerSessionID = c.Metadata("worker_id")
	}
	runAuthority := c.Metadata("run_authority")
	if runAuthority == "" {
		runAuthority = c.InvocationID()
	}
	leaseAuthority := c.Metadata("lease_authority")
	if leaseAuthority == "" {
		leaseAuthority = c.LeaseID()
	}
	if c.projectID == "" || c.RunID() == "" || workerSessionID == "" || runAuthority == "" || leaseAuthority == "" {
		return GenerateResponse{}, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"durable model activation requires project, run, worker-session, run, and lease authority",
			"",
			0,
			nil,
		)
	}
	stableKey := c.nextActivationKey("model", modelName)
	parentActivationID := parentActivationID(c)
	expectedActivationID := activationID(
		c.projectID,
		c.RunID(),
		parentActivationID,
		pb.ActivationKind_ACTIVATION_KIND_MODEL,
		stableKey,
	)
	inputDigest := sha256.Sum256(canonicalInput)
	begin, err := c.activationWriter.BeginActivation(c, &pb.BeginActivationRequest{
		ProjectId:          c.projectID,
		RunId:              c.RunID(),
		ParentActivationId: parentActivationID,
		Kind:               pb.ActivationKind_ACTIVATION_KIND_MODEL,
		StableKey:          stableKey,
		InputDigest:        inputDigest[:],
		DefinitionDigest:   cloneBytes(definitionDigest),
		RecoveryPolicy:     protoPolicy,
		WorkerSessionId:    workerSessionID,
		RunAuthority:       []byte(runAuthority),
		LeaseAuthority:     []byte(leaseAuthority),
	})
	if err != nil {
		return GenerateResponse{}, err
	}
	if begin.GetActivationId() != expectedActivationID {
		return GenerateResponse{}, newActivationError(
			ActivationErrorUnknownOutcome,
			fmt.Sprintf("runtime returned activation ID %q, want %q", begin.GetActivationId(), expectedActivationID),
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	if begin.GetOutcome() == pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_REPLAY {
		payload, payloadErr := inlineActivationBytes(begin.GetReplayResult())
		if payloadErr != nil {
			return GenerateResponse{}, payloadErr
		}
		var replayed GenerateResponse
		if err := json.Unmarshal(payload, &replayed); err != nil {
			return GenerateResponse{}, fmt.Errorf("agnt5: decode replayed model %q: %w", modelName, err)
		}
		return replayed, nil
	}
	if begin.GetOutcome() != pb.BeginActivationOutcome_BEGIN_ACTIVATION_OUTCOME_EXECUTE {
		return GenerateResponse{}, activationBeginDecisionError(begin, "model")
	}
	if begin.GetAttempt() == 0 || len(begin.GetFenceToken()) == 0 {
		return GenerateResponse{}, newActivationError(
			ActivationErrorUnknownOutcome,
			"EXECUTE receipt is missing fenced authority",
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
	response, modelErr := model.Generate(c.withActivationExecution(execution), request)
	if modelErr != nil {
		errorData, _ := json.Marshal(map[string]string{
			"message": modelErr.Error(),
			"type":    fmt.Sprintf("%T", modelErr),
		})
		retryable := policy == RecoveryPolicyIdempotentRetry || policy == RecoveryPolicyDurableSteps
		failed, failErr := c.activationWriter.FailActivation(c, &pb.FailActivationRequest{
			ProjectId:                c.projectID,
			RunId:                    c.RunID(),
			ActivationId:             begin.GetActivationId(),
			Attempt:                  begin.GetAttempt(),
			FenceToken:               cloneBytes(begin.GetFenceToken()),
			ErrorCode:                modelFailureErrorCode(model),
			ErrorData:                inlineActivationPayload(errorData),
			Retryable:                retryable,
			ExternalOutcomeCertainty: pb.ActivationExternalOutcomeCertainty_ACTIVATION_EXTERNAL_OUTCOME_CERTAINTY_UNKNOWN,
			Evidence:                 modelFailureEvidence(model, begin.GetAttempt()),
		})
		if failErr != nil {
			return GenerateResponse{}, fmt.Errorf("agnt5: record failure for model %q: %w (model error: %v)", modelName, failErr, modelErr)
		}
		if !failed.GetAccepted() || failed.GetActivationId() != begin.GetActivationId() || failed.GetAttempt() != begin.GetAttempt() {
			return GenerateResponse{}, newActivationError(
				ActivationErrorUnknownOutcome,
				"runtime returned an invalid model failure receipt",
				begin.GetActivationId(),
				begin.GetAttempt(),
				modelErr,
			)
		}
		return GenerateResponse{}, modelErr
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return GenerateResponse{}, fmt.Errorf("agnt5: encode model %q output: %w", modelName, err)
	}
	outputDigest := sha256.Sum256(payload)
	completed, err := c.activationWriter.CompleteActivation(c, &pb.CompleteActivationRequest{
		ProjectId:    c.projectID,
		RunId:        c.RunID(),
		ActivationId: begin.GetActivationId(),
		Attempt:      begin.GetAttempt(),
		FenceToken:   cloneBytes(begin.GetFenceToken()),
		Output:       inlineActivationPayload(payload),
		OutputDigest: outputDigest[:],
		Usage: &pb.ActivationUsage{
			TokensIn:  int64(response.Usage.InputTokens),
			TokensOut: int64(response.Usage.OutputTokens),
			LatencyMs: time.Since(startedAt).Milliseconds(),
			Provider:  provider,
			Model:     modelName,
		},
		Evidence: modelTerminalEvidence(response),
	})
	if err != nil {
		return GenerateResponse{}, err
	}
	if !completed.GetAccepted() || completed.GetActivationId() != begin.GetActivationId() || completed.GetAttempt() != begin.GetAttempt() {
		return GenerateResponse{}, newActivationError(
			ActivationErrorUnknownOutcome,
			"runtime returned an invalid model completion receipt",
			begin.GetActivationId(),
			begin.GetAttempt(),
			nil,
		)
	}
	return response, nil
}

func modelFailureEvidence(model LanguageModel, attempt uint32) []*pb.ActivationEvidence {
	provider, ok := model.(modelActivationFailureEvidence)
	if !ok {
		return nil
	}
	return provider.activationFailureEvidence(attempt)
}

func modelFailureErrorCode(model LanguageModel) string {
	provider, ok := model.(modelActivationFailureEvidence)
	if !ok {
		return "MODEL_FAILED"
	}
	return provider.activationFailureErrorCode()
}
