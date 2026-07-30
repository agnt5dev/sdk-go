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

The current module version is `v0.2.3`.

## Compatibility Policy

- The Go SDK targets the runtime protobuf/event contract generated under
  `internal/pb`.
- `internal/pb` stays SDK-local for `v0.x` releases. Split it into a standalone
  public proto module only if another public Go package needs those generated
  types.
- Runtime protobuf changes that affect worker registration, dispatch,
  checkpointing, `EventStream`, `AppendBatch`, pull polling, lease renewal, or
  completion fencing must update Go SDK tests in the same PR.
- Minor `v0.x` releases may add APIs. Breaking user-facing APIs require release
  notes and a migration note.

## Release Checklist

- `go test ./...`
- Verify the Go quickstart template compiles against the release candidate.
- Tag the module-root repository with `v0.x.y`.
- Publish template bundles that reference the released SDK version.
- Confirm `ghcr.io/agnt5dev/go-worker:latest` is published or update
  `AGNT5_CONTROL_PLANE_BASE_IMAGE_GO`.
