# Changelog

All notable changes to the AGNT5 Go SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/agnt5dev/sdk-go/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/agnt5dev/sdk-go/releases/tag/v0.2.0
