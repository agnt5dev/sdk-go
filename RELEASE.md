# AGNT5 Go SDK Release Notes

## Module Path

The public module path is `agnt5.dev/sdk-go`.

Release the Go SDK as a module-root repository, not as a monorepo subdirectory
module. The source of truth lives in this monorepo under `sdk/sdk-go`, but the
public Go module should be published by syncing that directory to a dedicated
repository whose root `go.mod` is:

```text
module agnt5.dev/sdk-go
```

Configure the `agnt5.dev/sdk-go?go-get=1` vanity endpoint to point at that
module-root repository. Release tags in that repository are plain module tags:

```text
v0.x.y
```

The current module version is `v0.2.0`. Do not tag the monorepo with
`sdk/sdk-go/v0.x.y` for this module path; that convention only applies if the
public module path includes the monorepo subdirectory.

## Compatibility Policy

- The Go SDK targets the runtime protobuf/event contract generated under
  `sdk/sdk-go/internal/pb`.
- `internal/pb` stays SDK-local for `v0.x` releases. Split it into a standalone
  public proto module only if another public Go package needs those generated
  types.
- Runtime protobuf changes that affect worker registration, dispatch,
  checkpointing, `EventStream`, `AppendBatch`, pull polling, lease renewal, or
  completion fencing must update Go SDK tests in the same PR.
- Minor `v0.x` releases may add APIs. Breaking user-facing APIs require release
  notes and a migration note.

## Release Checklist

- `cd sdk/sdk-go && go test ./...`
- Verify the Go quickstart template compiles against the release candidate.
- Confirm `https://agnt5.dev/sdk-go?go-get=1` serves valid Go vanity metadata.
- Sync `sdk/sdk-go` to the dedicated `agnt5.dev/sdk-go` module-root repository.
- Tag the module-root repository with `v0.x.y`.
- Publish template bundles that reference the released SDK version.
- Confirm `ghcr.io/agnt5dev/go-worker:latest` is published or update
  `AGNT5_CONTROL_PLANE_BASE_IMAGE_GO`.
