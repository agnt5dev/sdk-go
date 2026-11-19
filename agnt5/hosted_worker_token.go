package agnt5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	envWorkloadBrokerURL     = "AGNT5_WORKLOAD_BROKER_URL"
	envWorkloadTokenFile     = "AGNT5_WORKLOAD_TOKEN_FILE"
	runtimeWorkloadAudience  = "agnt5-runtime"
	workloadBrokerTokenPath  = "/internal/workload/token"
	workloadTokenStartupWait = 10 * time.Second
	workloadTokenRetryMin    = time.Second
	workloadTokenRetryMax    = 30 * time.Second
)

// hostedWorkloadConfig describes how a worker running on AGNT5-hosted
// infrastructure trades its pod's Kubernetes identity for a runtime token.
type hostedWorkloadConfig struct {
	brokerURL  *url.URL
	tokenFile  string
	httpClient *http.Client
}

// hostedWorkloadConfigFromEnv reports whether the pod was given both a
// workload broker and a projected identity token. Either one alone is treated
// as not configured, so local runs that happen to export one variable keep
// working without a token.
func hostedWorkloadConfigFromEnv() (hostedWorkloadConfig, bool, error) {
	broker := strings.TrimSpace(os.Getenv(envWorkloadBrokerURL))
	tokenFile := strings.TrimSpace(os.Getenv(envWorkloadTokenFile))
	if broker == "" || tokenFile == "" {
		return hostedWorkloadConfig{}, false, nil
	}
	parsed, err := url.Parse(broker)
	if err != nil {
		return hostedWorkloadConfig{}, false, fmt.Errorf("agnt5: invalid %s: %w", envWorkloadBrokerURL, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return hostedWorkloadConfig{}, false, fmt.Errorf("agnt5: %s must be an http(s) URL with a host", envWorkloadBrokerURL)
	}
	return hostedWorkloadConfig{
		brokerURL: parsed,
		tokenFile: tokenFile,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("workload broker redirects are not allowed")
			},
		},
	}, true, nil
}

// hostedWorkloadTokenSource attaches a broker-minted runtime token to every
// gRPC call, the same way externalWorkerSession does for self-hosted workers.
// Unlike the self-hosted path it never fails a call: while the runtime only
// observes tokens, a broker outage must not take hosted workers down, so calls
// go out without a token and a background loop keeps retrying.
type hostedWorkloadTokenSource struct {
	config hostedWorkloadConfig
	warn   io.Writer
	now    func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func newHostedWorkloadTokenSource(config hostedWorkloadConfig, warn io.Writer) *hostedWorkloadTokenSource {
	return &hostedWorkloadTokenSource{config: config, warn: warn, now: time.Now}
}

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (s *hostedWorkloadTokenSource) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" || !s.now().Before(s.expiresAt) {
		return map[string]string{}, nil
	}
	return map[string]string{"authorization": "Bearer " + s.token}, nil
}

// RequireTransportSecurity returns false because hosted workers reach the
// runtime over the cluster network, which may be plaintext.
func (s *hostedWorkloadTokenSource) RequireTransportSecurity() bool { return false }

// refresh re-reads the projected identity token, which the kubelet rotates in
// place, and exchanges it for a fresh runtime token.
func (s *hostedWorkloadTokenSource) refresh(ctx context.Context) error {
	contents, err := os.ReadFile(s.config.tokenFile)
	if err != nil {
		return fmt.Errorf("read workload token file %s: %w", filepath.Clean(s.config.tokenFile), err)
	}
	identity := strings.TrimSpace(string(contents))
	if identity == "" {
		return fmt.Errorf("workload token file %s is empty", filepath.Clean(s.config.tokenFile))
	}
	payload, err := json.Marshal(map[string]any{
		"token":    identity,
		"audience": []string{runtimeWorkloadAudience},
	})
	if err != nil {
		return fmt.Errorf("encode workload token request: %w", err)
	}
	endpoint := *s.config.brokerURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + workloadBrokerTokenPath
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build workload token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.config.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("workload broker request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		var problem struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(limited, &problem)
		if problem.Error == "" {
			problem.Error = "request_failed"
		}
		return fmt.Errorf("workload broker returned %s: %s", response.Status, problem.Error)
	}
	var minted struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&minted); err != nil {
		return fmt.Errorf("decode workload token response: %w", err)
	}
	if strings.TrimSpace(minted.Token) == "" || !minted.ExpiresAt.After(s.now()) {
		return errors.New("workload broker returned an invalid runtime token")
	}
	s.mu.Lock()
	s.token = strings.TrimSpace(minted.Token)
	s.expiresAt = minted.ExpiresAt
	s.mu.Unlock()
	return nil
}

// refreshDelay schedules the next exchange a minute before expiry, or halfway
// through the token's life when it is shorter than two minutes.
func (s *hostedWorkloadTokenSource) refreshDelay() time.Duration {
	s.mu.Lock()
	remaining := s.expiresAt.Sub(s.now())
	s.mu.Unlock()
	delay := remaining - externalTokenRefreshSkew
	if half := remaining / 2; delay < half {
		delay = half
	}
	if delay < workloadTokenRetryMin {
		delay = workloadTokenRetryMin
	}
	return delay
}

// run keeps the token fresh until ctx ends. ready is the outcome of the
// startup exchange; a failure there only delays the first token.
func (s *hostedWorkloadTokenSource) run(ctx context.Context, ready error) {
	failing := ready != nil
	backoff := workloadTokenRetryMin
	delay := backoff
	if !failing {
		delay = s.refreshDelay()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := s.refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Warn once per outage rather than on every retry.
			if !failing {
				s.warnf("runtime token refresh failed, retrying: %v", err)
			}
			failing = true
			delay = backoff
			backoff = min(backoff*2, workloadTokenRetryMax)
		} else {
			if failing {
				fmt.Fprintln(s.warn, "[INFO] agnt5 runtime token obtained from workload broker")
			}
			failing = false
			backoff = workloadTokenRetryMin
			delay = s.refreshDelay()
		}
		timer.Reset(delay)
	}
}

func (s *hostedWorkloadTokenSource) warnf(format string, args ...any) {
	fmt.Fprintf(s.warn, "[WARN] agnt5 "+format+"\n", args...)
}

// startHostedWorkloadToken installs a runtime token on hosted workers that
// were not configured for the self-hosted key exchange. It returns a function
// that stops background refresh; it is a no-op when the hosted path does not
// apply. A bad broker setting is reported but never stops the worker, for the
// same reason a broker outage does not.
func (w *Worker) startHostedWorkloadToken(ctx context.Context) func() {
	w.externalMu.Lock()
	defer w.externalMu.Unlock()
	if w.externalSession != nil {
		return func() {}
	}
	if w.hostedToken == nil {
		config, enabled, err := hostedWorkloadConfigFromEnv()
		if err != nil {
			fmt.Fprintf(w.warnOutput(), "[WARN] agnt5 continuing without a runtime token: %v\n", err)
			return func() {}
		}
		if !enabled {
			return func() {}
		}
		w.hostedToken = newHostedWorkloadTokenSource(config, w.warnOutput())
		w.grpcDialOptions = append(w.grpcDialOptions, grpc.WithPerRPCCredentials(w.hostedToken))
	}
	source := w.hostedToken
	startupCtx, cancelStartup := context.WithTimeout(ctx, workloadTokenStartupWait)
	ready := source.refresh(startupCtx)
	cancelStartup()
	if ready != nil {
		source.warnf("could not obtain a runtime token from the workload broker, continuing without one and retrying in the background: %v", ready)
	}
	refreshCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		source.run(refreshCtx, ready)
	}()
	return func() {
		stop()
		<-done
	}
}

func (w *Worker) warnOutput() io.Writer {
	if w.warnWriter != nil {
		return w.warnWriter
	}
	return os.Stderr
}
