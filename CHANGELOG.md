# Changelog

All notable changes to the AGNT5 Go SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-09-02

### Changed

- Durable activations are now the step boundary records. When the runtime
  negotiates `durable_activation_v1`, the SDK no longer emits its own
  `workflow.step.*`, `lm.*`, `tool_call.*`, or child `agent.*` lifecycle
  events for STEP, TIMER, MODEL, TOOL, and CHILD activations; the runtime
  journals one kind-named record per side from the activation RPCs. The SDK
  now supplies `display_name` and a JSON `input_data` (capped at 64 KiB) on
  `BeginActivation`, `latency_ms` on `FailActivation`, and `cached_tokens` in
  model usage. A replayed activation emits nothing.
- Events emitted inside a durable activation (nested activations, `Task`
  `function.*` events, model stream deltas, child-agent iteration events)
  now use the activation ID as their parent correlation ID, so they attach to
  the journal record instead of the enclosing component.
- Regenerated `internal/pb/api/v1/engine.pb.go` from the updated SDK proto;
  the `Activation*Record` / `ActivationJournalRecord` messages were removed.
- Trace assertions (`LMCalls`, `MaxTokens`, `MaxLMCalls`, `NoErrors`) now
  match the runtime's `lm.completed` / `lm.failed` event names, and
  `StepMemoized` also accepts a record whose `decision` is `replay`.

Legacy (non-durable) contexts, HITL, top-level `agent.*` / `function.*`
dispatch lifecycle, and stream deltas are unchanged.

### Fixed

- Pin the runtime-authored assignment commit offset on lifecycle records so
  append-time lease fencing can bridge projection lag immediately after a
  pull claim.

## [0.4.1] - 2026-08-24

### Added

- Add the remote worker bootstrap identity lifecycle and negotiate managed
  worker authentication and transport security from the bootstrap profile.

### Fixed

- Print the registered Go component tree, project dashboard URL, and
  coordinator connection lifecycle during `agnt5 dev` startup.

## [0.4.0] - 2026-08-08

### Added

- Add durable activation V1 bindings, capability negotiation, deployment
  artifact fencing, and replay-safe tool, model, timer, and delegated-child
  execution.
- Preserve execution lease authority and renew both push and pull worker
  leases.
- Expose invocation idempotency keys and wait for durably detached runs to be
  accepted by the runtime.

### Changed

- Grow pull polling capacity as jobs become active, retire surplus idle
  pollers, and report the live desired slot count while preserving the
  runtime-provided idle floor.
- Serialize streaming sends through the writer actor.

### Fixed

- Retry transient exact activation writes and preserve required child errors.
- Skip completed durable sleeps during replay and preserve activation events
  across pull pauses.

## [0.3.1] - 2026-07-31

### Fixed

- Return distinct workerless authentication errors for missing signature
  headers, unsupported signature versions, malformed or expired timestamps,
  and invalid HMAC values.
- Require the workerless signature version header for signed invocations.

## [0.3.0] - 2026-07-31

### Added

- Add concurrent batch evaluation, response helpers, typed scorer specs, and
  versioned evaluator presets matching the Python and TypeScript SDKs.
- Add the complete AGNT5 built-in scorer catalog: 25 deterministic scorers and
  five LLM-as-judge scorers.
- Add trace assertions, tool-trajectory helpers, typed trace/session artifacts,
  scorer field bindings, and typed scorer errors.
- Advertise built-in scorers from pull workers and execute them before custom
  component lookup, while reserving built-in names from custom registration.
- Validate deterministic scorer behavior against the shared cross-language
  golden fixture.

## [0.2.3] - 2026-07-30

### Fixed

- Continue HITL workflow and `wait_for_user_*` lifecycle spans across resume
  dispatches, while keeping replayed durable steps out of the logical trace
  tree.

## [0.2.2] - 2026-07-29

### Fixed

- Emit agent, iteration, model, and tool lifecycle events with stable
  correlation IDs, explicit parent relationships, and canonical component
  metadata so durable traces retain their execution hierarchy.
- Isolate derived execution correlation while preserving shared event, state,
  checkpoint, and human-in-the-loop context across nested and parallel work.

## [0.2.1] - 2026-07-26

### Added

- Add schema-driven agent state support and parallel execution coverage.

### Fixed

- Preserve fenced pull completion and deterministic trace ordering across
  concurrent agent and tool execution.

## [0.2.0] - 2026-07-25

### Added

- Standalone GitHub-hosted release validation for the Go SDK module.
- Native release automation based on semantic GitHub Release tags.
- Pull workers now use typed lease, session, and attempt authority for
  completion, renewal, and session replacement.

### Fixed

- Reject multi-run append batches before mutation and require exact runtime
  outcome cardinality.
- Cancel and join every old pull-session task before reconnecting, preventing
  session overlap and event-writer races.

[Unreleased]: https://github.com/agnt5dev/sdk-go/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/agnt5dev/sdk-go/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/agnt5dev/sdk-go/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/agnt5dev/sdk-go/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/agnt5dev/sdk-go/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/agnt5dev/sdk-go/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/agnt5dev/sdk-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/agnt5dev/sdk-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/agnt5dev/sdk-go/releases/tag/v0.2.0
