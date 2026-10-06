package serverless

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMissingSigningSecretRejectsInvokes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resolver func(*http.Request) string
	}{
		{"omitted", nil},
		{"empty", func(*http.Request) string { return "" }},
		{"blank", func(*http.Request) string { return " \t\n" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			h := New(Options{SigningSecret: tc.resolver})
			if err := RegisterWorkflow(h, "probe", func(*Context, struct{}) (string, error) {
				calls++
				return "executed", nil
			}); err != nil {
				t.Fatal(err)
			}
			manifest := httptest.NewRecorder()
			h.ServeHTTP(manifest, httptest.NewRequest(http.MethodGet, ManifestPath, nil))
			if manifest.Code != http.StatusOK {
				t.Fatalf("manifest status = %d", manifest.Code)
			}
			response := invoke(t, h, `{"component_type":"workflow","component_name":"probe","run_id":"probe","input":{}}`)
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "WORKERLESS_SIGNING_SECRET_REQUIRED") || calls != 0 {
				t.Fatalf("unsigned invoke: status=%d workflow_calls=%d body=%s", response.Code, calls, response.Body.String())
			}
		})
	}
}

func TestAllowUnsignedWarnsAndPreservesSignatureVerification(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	h := New(Options{AllowUnsigned: true})
	if !strings.Contains(logs.String(), "AllowUnsigned=true permits unsigned invokes") {
		t.Fatal("missing startup warning")
	}
	_ = RegisterWorkflow(h, "probe", func(*Context, struct{}) (string, error) { return "executed", nil })
	body := `{"component_type":"workflow","component_name":"probe","run_id":"probe","input":{}}`
	if response := invoke(t, h, body); response.Code != http.StatusOK {
		t.Fatalf("unsigned local invoke = %d", response.Code)
	}
	signed := New(Options{AllowUnsigned: true, SigningSecret: func(*http.Request) string { return "fixture-secret" }})
	calls := 0
	_ = RegisterWorkflow(signed, "probe", func(*Context, struct{}) (string, error) { calls++; return "executed", nil })
	if response := invoke(t, signed, body); response.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("configured secret bypassed: status=%d calls=%d", response.Code, calls)
	}
}

func TestMissingSecretWarnings(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	New(Options{})
	if !strings.Contains(logs.String(), "signing secret is missing; invokes are rejected") {
		t.Fatal("missing startup warning")
	}
	logs.Reset()
	h := New(Options{SigningSecret: func(*http.Request) string { return "" }})
	if logs.Len() != 0 {
		t.Fatal("request-time resolver should not be evaluated at startup")
	}
	invoke(t, h, `{}`)
	invoke(t, h, `{}`)
	if count := strings.Count(logs.String(), "signing secret is missing"); count != 1 {
		t.Fatalf("warnings = %d, want 1", count)
	}
}
