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

func TestSignatureFailuresUseStableProtocolCodes(t *testing.T) {
	h := New(Options{SigningSecret: func(*http.Request) string { return "secret" }})
	body := `{}`
	now := formatInt(time.Now().UnixMilli())
	expired := formatInt(time.Now().Add(-10 * time.Minute).UnixMilli())

	tests := []struct {
		name    string
		headers map[string]string
		code    string
	}{
		{name: "missing", headers: map[string]string{}, code: "WORKERLESS_SIGNATURE_MISSING"},
		{
			name: "missing version",
			headers: map[string]string{
				"X-AGNT5-Timestamp":  now,
				"X-AGNT5-Attempt-ID": "attempt-1",
				"X-AGNT5-Signature":  sign("secret", now, "attempt-1", body),
			},
			code: "WORKERLESS_SIGNATURE_VERSION_UNSUPPORTED",
		},
		{
			name: "invalid timestamp",
			headers: map[string]string{
				"X-AGNT5-Signature-Version": SignatureVersion,
				"X-AGNT5-Timestamp":         "invalid",
				"X-AGNT5-Attempt-ID":        "attempt-1",
				"X-AGNT5-Signature":         "sha256=invalid",
			},
			code: "WORKERLESS_SIGNATURE_TIMESTAMP_INVALID",
		},
		{
			name: "expired",
			headers: map[string]string{
				"X-AGNT5-Signature-Version": SignatureVersion,
				"X-AGNT5-Timestamp":         expired,
				"X-AGNT5-Attempt-ID":        "attempt-1",
				"X-AGNT5-Signature":         sign("secret", expired, "attempt-1", body),
			},
			code: "WORKERLESS_SIGNATURE_EXPIRED",
		},
		{
			name: "invalid signature",
			headers: map[string]string{
				"X-AGNT5-Signature-Version": SignatureVersion,
				"X-AGNT5-Timestamp":         now,
				"X-AGNT5-Attempt-ID":        "attempt-1",
				"X-AGNT5-Signature":         "sha256=invalid",
			},
			code: "WORKERLESS_SIGNATURE_INVALID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, InvokePath, strings.NewReader(body))
			for key, value := range test.headers {
				req.Header.Set(key, value)
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func sign(secret, timestamp, attemptID, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + attemptID + "." + body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func formatInt(value int64) string { return fmt.Sprintf("%d", value) }
