# Generated protobuf code

These files are generated from the SDK-facing protos vendored in
`sdk-core/proto` (which trail the platform protos by design). Regenerate with
the plugin versions recorded in the file headers:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
PKG='github.com/agnt5dev/sdk-go/internal/pb/api/v1;v1'
cd ../sdk-core/proto && protoc -I . \
  --go_out=../../sdk-go/internal/pb --go_opt=paths=source_relative \
  --go-grpc_out=../../sdk-go/internal/pb --go-grpc_opt=paths=source_relative \
  --go_opt=Mapi/v1/engine.proto="$PKG" --go_opt=Mapi/v1/common.proto="$PKG" \
  --go_opt=Mapi/v1/worker_coordinator.proto="$PKG" \
  --go-grpc_opt=Mapi/v1/engine.proto="$PKG" --go-grpc_opt=Mapi/v1/common.proto="$PKG" \
  --go-grpc_opt=Mapi/v1/worker_coordinator.proto="$PKG" \
  api/v1/engine.proto api/v1/common.proto api/v1/worker_coordinator.proto
```

Copy only the files whose proto actually changed; the others differ by the
`protoc` version line alone.
