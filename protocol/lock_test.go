package protocol_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	_ "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	wantProtocolVersion = "0.1.0-alpha.3"
	wantGoModule        = "github.com/agnt5dev/runtime/gen/go"
	wantLockDigest      = "3924d62a5f53ec6f2a401bb35b24d4af643b379a7bdd0ea5979f6885a901b0c1"
)

type protocolLock struct {
	SchemaVersion   int    `json:"schema_version"`
	ProtocolPackage string `json:"protocol_package"`
	ReleaseTag      string `json:"release_tag"`
	ArtifactVersion string `json:"artifact_version"`
	SourceCommit    string `json:"source_commit"`
	WireVersion     struct {
		Major uint32 `json:"major"`
		Minor uint32 `json:"minor"`
	} `json:"wire_version"`
	Descriptor  lockedFile `json:"descriptor"`
	Projections struct {
		Rust struct {
			Package string `json:"package"`
			Version string `json:"version"`
		} `json:"rust"`
		Go struct {
			Module  string `json:"module"`
			Version string `json:"version"`
		} `json:"go"`
	} `json:"projections"`
	Files []lockedFile `json:"files"`
}

type lockedFile struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

func TestProtocolLockMatchesPublishedProjection(t *testing.T) {
	lock := readProtocolLock(t)
	if lock.SchemaVersion != 1 || lock.ProtocolPackage != "agnt5.protocol.v2" {
		t.Fatalf("protocol lock identity: schema=%d package=%q", lock.SchemaVersion, lock.ProtocolPackage)
	}
	if lock.WireVersion.Major != 2 || lock.WireVersion.Minor != 0 {
		t.Fatalf("wire version: %d.%d", lock.WireVersion.Major, lock.WireVersion.Minor)
	}
	if lock.ReleaseTag != "protocol/v"+wantProtocolVersion || lock.ArtifactVersion != wantProtocolVersion {
		t.Fatalf("release identity: tag=%q artifact=%q", lock.ReleaseTag, lock.ArtifactVersion)
	}
	if len(lock.SourceCommit) != 40 {
		t.Fatalf("source commit is not a full SHA: %q", lock.SourceCommit)
	}
	if _, err := hex.DecodeString(lock.SourceCommit); err != nil {
		t.Fatalf("source commit is not hexadecimal: %v", err)
	}
	if lock.Projections.Rust.Package != "agnt5-proto" || lock.Projections.Rust.Version != wantProtocolVersion {
		t.Fatalf("Rust projection: %#v", lock.Projections.Rust)
	}
	if lock.Projections.Go.Module != wantGoModule || lock.Projections.Go.Version != "v"+wantProtocolVersion {
		t.Fatalf("Go projection: %#v", lock.Projections.Go)
	}
	assertGoModDependency(t, wantGoModule, "v"+wantProtocolVersion)
	assertLockedFiles(t, lock)
	assertDescriptorDigest(t, lock.Descriptor.SHA256)
}

func TestProtocolFixturesAreValidJSON(t *testing.T) {
	for _, path := range []string{
		"fixtures/component-descriptor-v1.json",
		"fixtures/endpoint-signature-v1.json",
		"fixtures/payload-transfer-v1.json",
		"protocol-lock.schema.json",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatalf("%s: invalid JSON: %v", path, err)
		}
	}
}

func readProtocolLock(t *testing.T) protocolLock {
	t.Helper()
	data, err := os.ReadFile("agnt5-protocol.lock.json")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != wantLockDigest {
		t.Fatalf("protocol lock digest mismatch: got %x want %s", digest, wantLockDigest)
	}
	var lock protocolLock
	if err := json.Unmarshal(data, &lock); err != nil {
		t.Fatalf("decode protocol lock: %v", err)
	}
	return lock
}

func assertGoModDependency(t *testing.T, path, version string) {
	t.Helper()
	data, err := os.ReadFile("../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == path {
			if fields[1] != version {
				t.Fatalf("%s version = %q, want %q", path, fields[1], version)
			}
			return
		}
	}
	t.Fatalf("%s is missing from the test build", path)
}

func assertLockedFiles(t *testing.T, lock protocolLock) {
	t.Helper()
	paths := map[string]string{
		"component-descriptor-v1.json": "fixtures/component-descriptor-v1.json",
		"endpoint-signature-v1.json":   "fixtures/endpoint-signature-v1.json",
		"payload-transfer-v1.json":     "fixtures/payload-transfer-v1.json",
		"protocol-lock.schema.json":    "protocol-lock.schema.json",
	}
	locked := make(map[string]string, len(lock.Files))
	for _, file := range lock.Files {
		locked[file.Name] = file.SHA256
	}
	for name, path := range paths {
		want := locked[name]
		if want == "" {
			t.Fatalf("%s is missing from protocol lock", name)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		got := sha256.Sum256(data)
		if hex.EncodeToString(got[:]) != want {
			t.Fatalf("%s digest mismatch: got %x want %s", path, got, want)
		}
	}
}

func assertDescriptorDigest(t *testing.T, want string) {
	t.Helper()
	paths := []string{
		"agnt5/protocol/v2/capabilities.proto",
		"google/protobuf/duration.proto",
		"agnt5/protocol/v2/common.proto",
		"agnt5/protocol/v2/execution_options.proto",
		"agnt5/protocol/v2/run_policy.proto",
		"google/protobuf/timestamp.proto",
		"agnt5/protocol/v2/trigger.proto",
		"agnt5/protocol/v2/component.proto",
		"agnt5/protocol/v2/dispatch.proto",
		"agnt5/protocol/v2/state.proto",
		"agnt5/protocol/v2/durable.proto",
		"agnt5/protocol/v2/endpoint.proto",
		"agnt5/protocol/v2/errors.proto",
		"agnt5/protocol/v2/event.proto",
		"agnt5/protocol/v2/execution.proto",
		"agnt5/protocol/v2/payload.proto",
		"agnt5/protocol/v2/worker.proto",
	}
	set := &descriptorpb.FileDescriptorSet{}
	for _, path := range paths {
		file, err := protoregistry.GlobalFiles.FindFileByPath(path)
		if err != nil {
			t.Fatalf("find generated descriptor %s: %v", path, err)
		}
		if strings.HasPrefix(path, "agnt5/protocol/v2/") && string(file.Package()) != "agnt5.protocol.v2" {
			t.Fatalf("public descriptor %s has package %q", path, file.Package())
		}
		protoFile := protodesc.ToFileDescriptorProto(file)
		protoFile.SourceCodeInfo = nil
		set.File = append(set.File, protoFile)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(set)
	if err != nil {
		t.Fatalf("marshal generated descriptor set: %v", err)
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != want {
		t.Fatalf("generated descriptor digest mismatch: got %x want %s", digest, want)
	}
}
