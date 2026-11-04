package agnt5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// StepConfig configures runtime-owned attempts and the timeout of each handler.
// Zero MaxAttempts means one attempt. A zero InitialInterval retries immediately.
type StepConfig struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Backoff         string
	Multiplier      float64
	Timeout         time.Duration
}

// DeclaredStep is a workflow-local operation, not a registered component.
// Execute awaits durable admission and completion without function checkpoints.
type DeclaredStep[In any, Out any] struct {
	name    string
	handler func(*Context, In) (Out, error)
	config  StepConfig
}

// NewStep declares a typed durable operation. Invoke it with Execute inside a
// workflow, using a stable explicit key. Existing Step and Task APIs are unchanged.
func NewStep[In any, Out any](name string, handler func(*Context, In) (Out, error), configs ...StepConfig) (*DeclaredStep[In, Out], error) {
	if strings.TrimSpace(name) == "" {
		return nil, ErrInvalidStepName
	}
	if handler == nil {
		return nil, ErrNilHandler
	}
	if len(configs) > 1 {
		return nil, errors.New("agnt5: NewStep accepts one StepConfig")
	}
	config := StepConfig{}
	if len(configs) == 1 {
		config = configs[0]
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 1
	}
	if config.MaxInterval == 0 {
		config.MaxInterval = time.Minute
	}
	if config.Backoff == "" {
		config.Backoff = "exponential"
	}
	if config.Multiplier == 0 {
		config.Multiplier = 2
	}
	if config.MaxAttempts < 1 || config.InitialInterval < 0 || config.MaxInterval < config.InitialInterval || config.Timeout < 0 || config.Multiplier <= 0 || math.IsNaN(config.Multiplier) || math.IsInf(config.Multiplier, 0) {
		return nil, errors.New("agnt5: invalid step retry or timeout configuration")
	}
	switch config.Backoff {
	case "constant", "linear", "exponential":
	default:
		return nil, errors.New("agnt5: invalid step backoff")
	}
	return &DeclaredStep[In, Out]{name: name, handler: handler, config: config}, nil
}

type stepRetryReady struct {
	cause   error
	attempt uint32
}

func (e *stepRetryReady) Error() string { return e.cause.Error() }
func (e *stepRetryReady) Unwrap() error { return e.cause }

func (config StepConfig) delay(attempt uint32) time.Duration {
	if config.InitialInterval == 0 {
		return 0
	}
	factor := float64(1)
	switch config.Backoff {
	case "linear":
		factor = float64(attempt)
	case "exponential":
		factor = math.Pow(config.Multiplier, float64(attempt-1))
	}
	delay := float64(config.InitialInterval) * factor
	if delay >= float64(config.MaxInterval) {
		return config.MaxInterval
	}
	return time.Duration(delay)
}

// Execute runs or replays one declared step. The key must be stable across
// retries/recovery and unique for parallel or repeated work within its parent.
// Arguments are JSON snapshots; every attempt receives a fresh typed copy.
// Handler timeouts are cooperative: propagate the provided Context to I/O.
func (s *DeclaredStep[In, Out]) Execute(ctx *Context, input In, key string) (out Out, err error) {
	var zero Out
	if s == nil || s.handler == nil {
		return zero, ErrNilHandler
	}
	if ctx == nil {
		return zero, context.Canceled
	}
	if ctx.Metadata(durableActivationV1Capability) != "true" || ctx.activationWriter == nil {
		return zero, newActivationError(ActivationErrorDurabilityUnavailable, "declared steps require runtime durable activation support", "", 0, nil)
	}
	if strings.TrimSpace(key) == "" {
		return zero, newActivationError(ActivationErrorInvalidArgument, "explicit step key is required", "", 0, nil)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return zero, fmt.Errorf("agnt5: encode step input: %w", err)
	}
	var snapshot In
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return zero, fmt.Errorf("agnt5: snapshot step input: %w", err)
	}
	stepKey := "step:" + s.name + ":" + key
	plan, _, err := activationPlanForStep(ctx, stepKey, snapshot)
	if err != nil {
		return zero, err
	}
	// Keep the invocation in flight across retry backoff. Otherwise a sibling
	// durable wait could release its lease after a failed attempt settles.
	reservation := newCorrelationID("step-inflight")
	ctx.shared.activity.beginStarted()
	ctx.shared.activity.beginFinished(reservation, true)
	defer ctx.shared.activity.settled(reservation)
	// A step's own waits must exclude this local reservation as well as its
	// admitted activation, while sibling waits still see unfinished work.
	ctx = ctx.withParentCorrelationID(ctx.parentCID)
	ctx.enclosingActivations = append(slices.Clip(ctx.enclosingActivations), reservation)
	ctx, finish := ctx.startTelemetrySpan("workflow.step." + s.name)
	defer finishTelemetryScope(finish, &err)
	for {
		result, callErr := runActivatedStep(ctx, s.name, stepKey, plan, func(parent context.Context, cid string) (Out, error) {
			scope := stepScope(ctx, parent).withParentCorrelationID(cid)
			if err := scope.Err(); err != nil {
				return zero, err
			}
			var value In
			if err := json.Unmarshal(encoded, &value); err != nil {
				return zero, err
			}
			cancel := func() {}
			if s.config.Timeout > 0 {
				scope.Context, cancel = context.WithTimeout(scope.Context, s.config.Timeout)
			}
			defer cancel()
			result, handlerErr := s.handler(scope, value)
			if err := scope.Err(); err != nil {
				return zero, err
			}
			return result, handlerErr
		}, &s.config)
		var retry *stepRetryReady
		if !errors.As(callErr, &retry) {
			return result, callErr
		}
		timer := time.NewTimer(s.config.delay(retry.attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		}
	}
}
