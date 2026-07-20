package serverless

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSignedWorkflowAndCheckpointReplay(t *testing.T) {
	h := New(Options{ServiceName: "go-test", SigningSecret: func(*http.Request) string { return "secret" }})
	calls := 0
	if err := RegisterWorkflow(h, "hello", func(ctx *Context, input struct {
		Name string `json:"name"`
	}) (map[string]string, error) {
		name, err := Step(ctx, "normalize", func(context.Context) (string, error) { calls++; return strings.ToUpper(input.Name), nil })
		return map[string]string{"message": "hello " + name}, err
	}); err != nil {
		t.Fatal(err)
	}

	body := `{"protocol_version":"workerless.v1","run_id":"run-1","component_type":"workflow","component_name":"hello","input":{"name":"Ada"},"checkpoint":{"steps":{"step:normalize":"ADA"}}}`
	timestamp := time.Now().UnixMilli()
	req := httptest.NewRequest(http.MethodPost, InvokePath, strings.NewReader(body))
	req.Header.Set("X-AGNT5-Timestamp", formatInt(timestamp))
	req.Header.Set("X-AGNT5-Attempt-ID", "attempt-1")
	req.Header.Set("X-AGNT5-Signature-Version", SignatureVersion)
	req.Header.Set("X-AGNT5-Signature", sign("secret", formatInt(timestamp), "attempt-1", body))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"message":"hello ADA"`) {
		t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
	}
	if calls != 0 {
		t.Fatalf("checkpointed step executed %d times", calls)
	}
}

func TestManifest(t *testing.T) {
	h := New(Options{ServiceName: "go-test", ServiceVersion: "v1"})
	_ = RegisterWorkflow(h, "hello", func(*Context, struct{}) (struct{}, error) { return struct{}{}, nil })
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, ManifestPath, nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"protocol_version":"workerless.v1"`) {
		t.Fatalf("unexpected manifest: %s", response.Body.String())
	}
}

func sign(secret, timestamp, attemptID, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + attemptID + "." + body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func formatInt(value int64) string { return fmt.Sprintf("%d", value) }
