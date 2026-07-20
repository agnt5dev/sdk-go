package serverless

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const payloadRefKind = "agnt5.object_store.signed_url.v1"
const outputRefKind = "agnt5.object_store.ref.v1"

type PayloadRef struct {
	Kind        string `json:"kind"`
	URL         string `json:"url"`
	Method      string `json:"method"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	ExpiresAtMS int64  `json:"expires_at_ms"`
}
type OutputUpload struct {
	Kind           string `json:"kind"`
	URL            string `json:"url"`
	Method         string `json:"method"`
	Ref            string `json:"ref"`
	ThresholdBytes int64  `json:"threshold_bytes"`
	MaxBytes       int64  `json:"max_bytes"`
	ContentType    string `json:"content_type"`
	ExpiresAtMS    int64  `json:"expires_at_ms"`
}
type OutputRef struct {
	Kind        string `json:"kind"`
	Ref         string `json:"ref"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
}
type protocolError struct {
	Status        int
	Code, Message string
}

func (h *Handler) httpClient() *http.Client {
	if h.opts.HTTPClient != nil {
		return h.opts.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("too many redirects")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("unsupported redirect scheme")
		}
		return nil
	}}
}

func (h *Handler) resolveInput(ctx context.Context, inline json.RawMessage, ref *PayloadRef) (json.RawMessage, *protocolError) {
	if ref != nil && len(inline) > 0 && string(inline) != "null" {
		return nil, perr(400, "WORKERLESS_INPUT_REF_INVALID", "invoke payload must not include both input and input_ref")
	}
	if ref == nil {
		return inline, nil
	}
	if ref.Kind != payloadRefKind || strings.ToUpper(defaultString(ref.Method, "GET")) != "GET" {
		return nil, perr(400, "WORKERLESS_INPUT_REF_UNSUPPORTED", "input_ref kind or method is unsupported")
	}
	if !validHTTPURL(ref.URL) || ref.SizeBytes < 0 || ref.SizeBytes > maxPayloadBytes || len(ref.SHA256) != 64 {
		return nil, perr(400, "WORKERLESS_INPUT_REF_INVALID", "input_ref URL, size_bytes, or sha256 is invalid")
	}
	if ref.ExpiresAtMS > 0 && time.Now().UnixMilli() >= ref.ExpiresAtMS {
		return nil, perr(410, "WORKERLESS_INPUT_REF_EXPIRED", "input_ref has expired")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return nil, perr(400, "WORKERLESS_INPUT_REF_INVALID", err.Error())
	}
	resp, err := h.httpClient().Do(req)
	if err != nil {
		return nil, perr(502, "WORKERLESS_INPUT_REF_FETCH_FAILED", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, perr(502, "WORKERLESS_INPUT_REF_FETCH_FAILED", fmt.Sprintf("input_ref returned HTTP %d", resp.StatusCode))
	}
	data, err := readLimited(resp.Body, maxPayloadBytes)
	if err != nil {
		return nil, perr(413, "WORKERLESS_INPUT_REF_TOO_LARGE", err.Error())
	}
	if int64(len(data)) != ref.SizeBytes {
		return nil, perr(400, "WORKERLESS_INPUT_REF_INVALID", "input_ref size_bytes did not match fetched payload")
	}
	digest := sha256.Sum256(data)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), ref.SHA256) {
		return nil, perr(400, "WORKERLESS_INPUT_REF_CHECKSUM_MISMATCH", "input_ref sha256 did not match fetched payload")
	}
	if !json.Valid(data) {
		return nil, perr(400, "WORKERLESS_INPUT_REF_INVALID", "input_ref payload must be JSON")
	}
	return data, nil
}

func (h *Handler) completeOutput(ctx context.Context, output any, upload *OutputUpload, checkpoint checkpointEnvelope, events []Event) (map[string]any, *protocolError) {
	body := map[string]any{"status": "completed", "output": output, "checkpoint": checkpoint, "events": events}
	if upload == nil {
		return body, nil
	}
	data, err := json.Marshal(output)
	if err != nil {
		return nil, perr(500, "WORKERLESS_OUTPUT_SERIALIZATION_FAILED", err.Error())
	}
	if upload.ThresholdBytes < 0 || int64(len(data)) <= upload.ThresholdBytes {
		return body, nil
	}
	if upload.Kind != payloadRefKind || strings.ToUpper(defaultString(upload.Method, "PUT")) != "PUT" {
		return nil, perr(400, "WORKERLESS_OUTPUT_UPLOAD_UNSUPPORTED", "output_upload kind or method is unsupported")
	}
	if !validHTTPURL(upload.URL) || upload.Ref == "" || upload.MaxBytes <= 0 || upload.MaxBytes > maxPayloadBytes || int64(len(data)) > upload.MaxBytes {
		return nil, perr(400, "WORKERLESS_OUTPUT_UPLOAD_INVALID", "output_upload URL, ref, or size limit is invalid")
	}
	if upload.ExpiresAtMS > 0 && time.Now().UnixMilli() >= upload.ExpiresAtMS {
		return nil, perr(410, "WORKERLESS_OUTPUT_UPLOAD_EXPIRED", "output_upload has expired")
	}
	contentType := defaultString(upload.ContentType, "application/json")
	if contentType != "application/json" {
		return nil, perr(400, "WORKERLESS_OUTPUT_UPLOAD_UNSUPPORTED", "output_upload content_type is unsupported")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, upload.URL, bytes.NewReader(data))
	if err != nil {
		return nil, perr(400, "WORKERLESS_OUTPUT_UPLOAD_INVALID", err.Error())
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := h.httpClient().Do(req)
	if err != nil {
		return nil, perr(502, "WORKERLESS_OUTPUT_UPLOAD_FAILED", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, perr(502, "WORKERLESS_OUTPUT_UPLOAD_FAILED", fmt.Sprintf("output_upload returned HTTP %d", resp.StatusCode))
	}
	digest := sha256.Sum256(data)
	return map[string]any{"status": "completed", "output_ref": OutputRef{Kind: outputRefKind, Ref: upload.Ref, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), ContentType: contentType}, "checkpoint": checkpoint, "events": events}, nil
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("payload exceeds %d bytes", limit)
	}
	return data, nil
}
func validHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
}
func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
func perr(status int, code, message string) *protocolError {
	return &protocolError{Status: status, Code: code, Message: message}
}
