package agnt5

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
)

const externalWorkerSessionFile = "worker-session.json"

var errExternalWorkerIdentityRotated = errors.New("agnt5: worker identity rotated; reconnect required")

type externalWorkerIdentity struct {
	SessionID                 string    `json:"session_id"`
	ProjectID                 string    `json:"project_id"`
	EnvironmentID             string    `json:"environment_id"`
	DeploymentID              string    `json:"deployment_id"`
	WorkerPoolID              string    `json:"worker_pool_id"`
	WorkerID                  string    `json:"worker_id"`
	SPIFFEID                  string    `json:"spiffe_id"`
	RuntimeEndpoint           string    `json:"runtime_endpoint"`
	CertificateDERBase64      string    `json:"certificate_der_base64"`
	CertificateChainDERBase64 []string  `json:"certificate_chain_der_base64"`
	TrustBundleDERBase64      []string  `json:"trust_bundle_der_base64"`
	TrustBundleVersion        string    `json:"trust_bundle_version"`
	CertificateExpiresAt      time.Time `json:"certificate_expires_at"`
	RenewAfter                time.Time `json:"renew_after"`
	WorkloadToken             string    `json:"workload_token"`
	TokenType                 string    `json:"token_type"`
	TokenExpiresAt            time.Time `json:"token_expires_at"`
	PrivateKeyPEM             string    `json:"private_key_pem"`
}

type externalWorkerTokenRefresh struct {
	WorkloadToken  string    `json:"workload_token"`
	TokenType      string    `json:"token_type"`
	TokenExpiresAt time.Time `json:"token_expires_at"`
}

func loadOrOpenExternalWorkerIdentity(ctx context.Context, config externalWorkerBootstrapConfig, credential string, authority externalWorkerConnection) (*externalWorkerIdentity, error) {
	if identity, err := readExternalWorkerIdentity(config.sessionPath, authority); err == nil {
		return identity, nil
	}
	privateKey, csr, err := newExternalWorkerCSR()
	if err != nil {
		return nil, err
	}
	body := map[string]string{
		"project_id": authority.ProjectID, "environment_id": authority.EnvironmentID,
		"deployment_id": authority.DeploymentID, "worker_pool_id": authority.WorkerPoolID,
		"csr_der_base64": csr,
	}
	var identity externalWorkerIdentity
	if err := externalWorkerIdentityRequest(ctx, config.httpClient, config.controlPlaneURL.String(), credential, "api/v1/external-worker-sessions", body, &identity); err != nil {
		return nil, err
	}
	identity.PrivateKeyPEM = privateKey
	if identity.RuntimeEndpoint == "" {
		identity.RuntimeEndpoint = authority.RuntimeEndpoint
	}
	if err := validateExternalWorkerIdentity(&identity, authority); err != nil {
		return nil, err
	}
	if err := writeExternalWorkerIdentity(config.sessionPath, &identity); err != nil {
		return nil, err
	}
	return &identity, nil
}

func (s *externalWorkerSession) refreshIdentityTokenLocked(ctx context.Context) error {
	if s.identity == nil {
		return errors.New("agnt5: worker identity is unavailable")
	}
	client, err := s.identityHTTPClient()
	if err != nil {
		return err
	}
	var token externalWorkerTokenRefresh
	if err := externalWorkerIdentityRequest(ctx, client, s.config.identityURL.String(), "", "api/v1/external-worker-sessions/token", map[string]string{}, &token); err != nil {
		return err
	}
	if token.WorkloadToken == "" || !strings.EqualFold(token.TokenType, "Bearer") || !token.TokenExpiresAt.After(time.Now()) {
		return errors.New("agnt5: worker token refresh returned an invalid credential")
	}
	s.identity.WorkloadToken = token.WorkloadToken
	s.identity.TokenType = token.TokenType
	s.identity.TokenExpiresAt = token.TokenExpiresAt
	s.token = token.WorkloadToken
	s.expiresAt = token.TokenExpiresAt
	return writeExternalWorkerIdentity(s.config.sessionPath, s.identity)
}

func (s *externalWorkerSession) renewIdentityLocked(ctx context.Context) error {
	privateKey, csr, err := newExternalWorkerCSR()
	if err != nil {
		return err
	}
	client, err := s.identityHTTPClient()
	if err != nil {
		return err
	}
	var next externalWorkerIdentity
	if err := externalWorkerIdentityRequest(ctx, client, s.config.identityURL.String(), s.identity.WorkloadToken, "api/v1/external-worker-sessions/renew", map[string]string{"csr_der_base64": csr}, &next); err != nil {
		return err
	}
	next.PrivateKeyPEM = privateKey
	if next.RuntimeEndpoint == "" {
		next.RuntimeEndpoint = s.connection.RuntimeEndpoint
	}
	if err := validateExternalWorkerIdentity(&next, s.connection); err != nil {
		return err
	}
	if err := writeExternalWorkerIdentity(s.config.sessionPath, &next); err != nil {
		return err
	}
	s.identity = &next
	s.token = next.WorkloadToken
	s.expiresAt = next.TokenExpiresAt
	return nil
}

func (s *externalWorkerSession) identityHTTPClient() (*http.Client, error) {
	tlsConfig, err := s.identityTLSConfig(s.config.identityURL.Hostname())
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("worker identity redirects are not allowed")
		},
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}, nil
}

func (s *externalWorkerSession) transportCredentials() (credentials.TransportCredentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	endpoint, err := validateExternalEndpoint(s.connection.RuntimeEndpoint, "runtime")
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "https" {
		return nil, errors.New("agnt5: bootstrap identity runtime endpoint must use https")
	}
	tlsConfig, err := s.identityTLSConfig(endpoint.Hostname())
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsConfig), nil
}

func (s *externalWorkerSession) identityTLSConfig(serverName string) (*tls.Config, error) {
	if s.identity == nil {
		return nil, errors.New("agnt5: worker identity is unavailable")
	}
	certificate, roots, err := parseExternalWorkerTLSIdentity(s.identity)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: serverName, RootCAs: roots,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

func parseExternalWorkerTLSIdentity(identity *externalWorkerIdentity) (tls.Certificate, *x509.CertPool, error) {
	chainPEM, err := certificatePEM(append([]string{identity.CertificateDERBase64}, identity.CertificateChainDERBase64...))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certificate, err := tls.X509KeyPair(chainPEM, []byte(identity.PrivateKeyPEM))
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("agnt5: load worker TLS identity: %w", err)
	}
	roots := x509.NewCertPool()
	for _, value := range identity.TrustBundleDERBase64 {
		der, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return tls.Certificate{}, nil, errors.New("agnt5: worker trust bundle is invalid")
		}
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			return tls.Certificate{}, nil, errors.New("agnt5: worker trust bundle is invalid")
		}
		roots.AddCert(certificate)
	}
	if len(identity.TrustBundleDERBase64) == 0 {
		return tls.Certificate{}, nil, errors.New("agnt5: worker trust bundle is empty")
	}
	return certificate, roots, nil
}

func certificatePEM(values []string) ([]byte, error) {
	var result []byte
	for _, value := range values {
		der, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, errors.New("agnt5: worker certificate chain is invalid")
		}
		result = append(result, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	return result, nil
}

func newExternalWorkerCSR() (string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("agnt5: generate worker identity key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{}}, key)
	if err != nil {
		return "", "", fmt.Errorf("agnt5: generate worker identity CSR: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", fmt.Errorf("agnt5: encode worker identity key: %w", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})), base64.StdEncoding.EncodeToString(csr), nil
}

func validateExternalWorkerIdentity(identity *externalWorkerIdentity, authority externalWorkerConnection) error {
	now := time.Now()
	if identity == nil || identity.ProjectID != authority.ProjectID || identity.EnvironmentID != authority.EnvironmentID || identity.DeploymentID != authority.DeploymentID || identity.WorkerPoolID != authority.WorkerPoolID || identity.SessionID == "" || identity.WorkerID == "" || identity.PrivateKeyPEM == "" || identity.WorkloadToken == "" || identity.CertificateDERBase64 == "" || len(identity.TrustBundleDERBase64) == 0 || !identity.CertificateExpiresAt.After(now.Add(30*time.Second)) || !identity.TokenExpiresAt.After(now.Add(30*time.Second)) {
		return errors.New("agnt5: worker identity is expired, incomplete, or outside discovery authority")
	}
	_, _, err := parseExternalWorkerTLSIdentity(identity)
	return err
}

func readExternalWorkerIdentity(path string, authority externalWorkerConnection) (*externalWorkerIdentity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("agnt5: worker identity file permissions are too broad")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var identity externalWorkerIdentity
	if err := json.Unmarshal(contents, &identity); err != nil {
		return nil, err
	}
	if err := validateExternalWorkerIdentity(&identity, authority); err != nil {
		return nil, err
	}
	return &identity, nil
}

func writeExternalWorkerIdentity(path string, identity *externalWorkerIdentity) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("agnt5: create worker session directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("agnt5: secure worker session directory: %w", err)
	}
	contents, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("agnt5: encode worker identity: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+externalWorkerSessionFile+".*.tmp")
	if err != nil {
		return fmt.Errorf("agnt5: write worker identity: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("agnt5: secure worker identity: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("agnt5: write worker identity: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("agnt5: flush worker identity: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("agnt5: close worker identity: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("agnt5: replace worker identity atomically: %w", err)
	}
	return nil
}

func externalWorkerIdentityRequest(ctx context.Context, client *http.Client, baseURL, bearer, path string, body, target any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("agnt5: encode worker identity request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/"+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("agnt5: build worker identity request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("agnt5: worker identity request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		var problem struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(limited, &problem)
		if problem.Error == "" {
			problem.Error = "request_failed"
		}
		return fmt.Errorf("agnt5: worker identity request returned %s: %s", response.Status, problem.Error)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("agnt5: decode worker identity response: %w", err)
	}
	return nil
}
