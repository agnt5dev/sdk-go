# Changelog

All notable changes to the AGNT5 Go SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.3] - 2026-09-25

### Added

- Built-in structured assertions with SDK-core conformance, bounded execution, and named result metadata.

### Fixed

- Durable model, tool and delegated-agent activations begun inside an agent
  iteration now carry that iteration as a reader-only display parent, so Studio
  nests them under the iteration instead of beside the owning workflow step
  (AGNT5-1243). Ownership, activation identity, digests and replay are
  unchanged. `internal/pb` is regenerated from sdk-core 0.3.1 for the
  `display_parent_correlation_id` field; runtimes that predate it ignore it.
- OpenAI reasoning models (the gpt-5 and gpt-6 families and the o-series) are sent `max_completion_tokens` instead of `max_tokens` and no `temperature`, which those models reject with a 400; gpt-4o and gpt-4.1 keep the classic parameters (AGNT5-1303).

## [0.10.2] - 2026-09-22

### Fixed

- One slow model provider response no longer fails the whole run. OpenAI, Anthropic and Google model calls default to a ten-minute request timeout, bounded by the run's own deadline, instead of a fixed 60 seconds, and retry up to twice with jittered exponential backoff when the provider times out or answers 408, 429, 500, 502, 503, 504 or 529, honouring `Retry-After`. This matches the Python and TypeScript SDKs, including the `AGNT5_LM_MAX_RETRIES`, `AGNT5_LM_INITIAL_DELAY_MS` and `AGNT5_LM_MAX_DELAY_MS` overrides. A request that still fails returns a `ModelRequestError` that says whether the provider timed out (AGNT5-1251).
- The worker reports its service version as 0.10.2; 0.10.1 still reported 0.10.0.

## [0.10.1] - 2026-09-22

### Added

- Opt-in external-worker mTLS with persistent authentication selection, certificate-bound tokens and restart-safe certificate renewal after a lost response.
- Independent server certificate trust through system roots or `AGNT5_WORKER_SERVER_CA_FILE`.

### Upgrade

- Existing bearer workers retain their default behavior. Enable `AGNT5_WORKER_MTLS_ENABLED=true` only after commissioning the compatible control plane and dedicated runtime mTLS endpoint, with a private persistent `AGNT5_WORKER_SESSION_DIR`. A worker already pinned to mTLS cannot silently fall back to bearer authentication.

## [0.10.0] - 2026-09-13

### Changed

- Run and stream calls share a five-minute response wait by default. Use
  `WithWaitTimeout` to choose a whole-millisecond wait from zero to 24 hours;
  zero returns immediately after acceptance. This requires gateway support for
  `X-AGNT5-Wait-Timeout-Ms` and does not change the workflow execution deadline.
- `Run` returns accepted pending receipts without additional status polling.
  `StreamEvents` exposes `stream.wait_expired` and `stream.detached`, while
  chunk-only `Stream` returns a `RunError` containing the run ID when waiting
  ends. Accepted work continues; use the run ID to retrieve status and results.
- Response wait and HTTP deadlines are separate. The default HTTP deadline
  allows the requested wait plus ten seconds, or the configured client timeout
  if longer. `WithRunTimeout` and the caller's context can set an earlier limit.

### Removed

- Remove `Client.BatchStream` and `BatchStreamEvent` with the retired
  `/batch/stream` gateway endpoint. Migrate callers to `Batch` and
  `GetBatchStatus`; existing callers using the removed symbols must update.

## [0.9.0] - 2026-09-12

### Added

- Context-aware `NewSlogHandler` forwarding, preserving the application's local
  handler, groups, and level filtering (AGNT5-1079).
- OTLP invocation, step, and model spans with run and log correlation; preserve
  W3C parents and runtime pull-job trace IDs, record handler errors/panics, and
  drain logs and traces together during worker shutdown (AGNT5-1079).
- Enable trace export when a shared or trace-specific OTLP endpoint is set;
  include canonical workspace/project/deployment tags on every span. Built-in
  judge models and nested panics retain their trace attribution (AGNT5-1079).

## [0.8.0] - 2026-09-11

### Fixed

- Include canonical `agnt5.app_name` alongside the existing application-name resource attribute in OTLP logs (AGNT5-1079).

### Changed

- Workers now default to `pull` when `AGNT5_WORKER_MODE` is unset or empty.
  Explicit `push` remains supported; set it before upgrading if your worker
  relies on coordinator-push dispatch (AGNT5-1100).

## [0.7.1] - 2026-09-08

### Added

- Observe pull-slot lifecycle, activation RPCs, and durable business execution
  with opt-in core metrics logs.
- Negotiate server-driven pull-slot scaling while retaining the existing
  local ramp and idle-retirement policy with older runtimes.

### Fixed

- Preserve causal state snapshots and exact retries after uncertain writes.
- Retire server-hinted surplus slots concurrently.
- Keep a healthy pull session polling after the runtime definitively rejects a
  stale or already-terminal completion instead of retrying and reconnecting.

## [0.7.0] - 2026-09-04

### Added

- Export application and component lifecycle logs through OTLP with AGNT5
  resource and run attributes.

## [0.6.0] - 2026-09-03

### Added

- Add deterministic `TaskWithKey` identities for parallel, reordered, and
  repeated same-name durable work.
- Add progressive `SKILL.md` discovery and loading, ordered `AGENTS.md`
  guidance, automatic agent sandbox tools, and bundled skill resource
  materialization.
- Add an additive sandbox workspace deletion capability, including HTTP and
  concurrency-safe in-memory implementations.

### Fixed

- Preserve ordered component and activation lifecycle records when pull
  workflows flush concurrent durable boundaries.
- Send Gemini function declarations, parse returned function calls, and replay
  tool results with their call identifiers and thought signatures intact.

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

[Unreleased]: https://github.com/agnt5dev/sdk-go/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/agnt5dev/sdk-go/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/agnt5dev/sdk-go/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/agnt5dev/sdk-go/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/agnt5dev/sdk-go/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/agnt5dev/sdk-go/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/agnt5dev/sdk-go/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/agnt5dev/sdk-go/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/agnt5dev/sdk-go/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/agnt5dev/sdk-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/agnt5dev/sdk-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/agnt5dev/sdk-go/releases/tag/v0.2.0
