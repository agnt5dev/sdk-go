# Changelog

All notable changes to the AGNT5 Go SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/agnt5dev/sdk-go/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/agnt5dev/sdk-go/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/agnt5dev/sdk-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/agnt5dev/sdk-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/agnt5dev/sdk-go/releases/tag/v0.2.0
