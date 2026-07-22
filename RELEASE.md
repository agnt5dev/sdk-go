# AGNT5 Go SDK Release Notes

## Module Path

The public module path is `github.com/agnt5dev/sdk-go`.

Release the Go SDK from this module-root repository. Its root `go.mod` is:

```text
module github.com/agnt5dev/sdk-go
```

Because the module path is the GitHub repository URL, no vanity endpoint is required. Release tags in that repository are plain module tags:

```text
v0.x.y
```

The current module version is `v0.2.0`.

## Compatibility Policy

- The v1 worker targets the frozen SDK-local runtime contract generated under
  `internal/pb`.
- The v2 worker pins the immutable public projection
  `github.com/agnt5dev/runtime/gen/go` and verifies it against
  `protocol/agnt5-protocol.lock.json` and the release fixtures.
- `internal/pb` remains SDK-local and v1-only. Generated v2 types stay behind
  internal adapter functions and do not appear in exported handler signatures.
- Runtime protobuf changes that affect worker registration, dispatch,
  checkpointing, `EventStream`, `AppendBatch`, pull polling, lease renewal, or
  completion fencing must update Go SDK tests in the same PR.
- Minor `v0.x` releases may add APIs. Breaking user-facing APIs require release
  notes and a migration note.

## Release Checklist

- `go test ./...`
- `go vet ./...`
- `go mod verify`
- Verify `protocol/agnt5-protocol.lock.json`, its fixtures, and the generated
  descriptor digest with `go test ./protocol`.
- Verify the Go quickstart template compiles against the release candidate.
- Tag the module-root repository with `v0.x.y`.
- Publish template bundles that reference the released SDK version.
- Confirm `ghcr.io/agnt5dev/go-worker:latest` is published or update
  `AGNT5_CONTROL_PLANE_BASE_IMAGE_GO`.
