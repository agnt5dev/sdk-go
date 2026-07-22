# Changelog

All notable changes to the AGNT5 Go SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Standalone GitHub-hosted release validation for the Go SDK module.
- Native release automation based on semantic GitHub Release tags.
- Dual-stack worker protocol selection with `auto`, forced v1, and forced v2
  modes.
- Protocol v2.0 negotiation and a session-pinned pull worker with replay-stable
  polling, lease renewal, and fenced completed/failed outcome commits.
- An immutable alpha.3 protocol dependency lock, conformance fixtures, and
  generated descriptor digest verification.

### Changed

- Protocol v2 descriptor registration now validates schemas and retry defaults,
  declares required workflow and trigger capabilities, and rejects unsupported
  run-policy configuration instead of silently degrading behavior.
- Protocol v2 workers now distinguish natural session expiry from replacement,
  drain concurrent poll slots before re-registration, keep cancellation and
  stale execution failures scoped to one execution, and enforce every
  negotiated message and payload size limit.

[Unreleased]: https://github.com/agnt5dev/sdk-go/commits/main
