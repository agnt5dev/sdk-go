package agnt5

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExternalWorkerCSRUsesECDSAP256AndProofOfPossession(t *testing.T) {
	privateKey, encodedCSR, err := newExternalWorkerCSR()
	if err != nil {
		t.Fatalf("newExternalWorkerCSR: %v", err)
	}
	if privateKey == "" {
		t.Fatal("private key is empty")
	}
	csrDER, err := base64.StdEncoding.DecodeString(encodedCSR)
	if err != nil {
		t.Fatalf("decode CSR: %v", err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR signature: %v", err)
	}
	publicKey, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || publicKey.Curve.Params().Name != "P-256" {
		t.Fatalf("CSR public key = %T/%v", csr.PublicKey, publicKey)
	}
}

func TestExternalWorkerIdentityFileIsPrivateAtomicAndAuthorityBound(t *testing.T) {
	authority := externalWorkerConnection{
		ProjectID: "project", EnvironmentID: "environment", DeploymentID: "deployment",
		WorkerPoolID: "pool", RuntimeEndpoint: "https://runtime.example.com",
	}
	identity := testExternalWorkerIdentity(t, authority)
	path := filepath.Join(t.TempDir(), "session", externalWorkerSessionFile)
	if err := writeExternalWorkerIdentity(path, identity); err != nil {
		t.Fatalf("writeExternalWorkerIdentity: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat identity: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o, want 600", info.Mode().Perm())
	}
	loaded, err := readExternalWorkerIdentity(path, authority)
	if err != nil || loaded.SessionID != identity.SessionID {
		t.Fatalf("read identity = %#v, %v", loaded, err)
	}
	wrong := authority
	wrong.ProjectID = "other"
	if _, err := readExternalWorkerIdentity(path, wrong); err == nil {
		t.Fatal("cross-project session was accepted")
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), "."+externalWorkerSessionFile+".*.tmp")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary identity files remain: %v, %v", matches, err)
	}
}

func TestExternalWorkerConfigurationDefersAuthenticationToDiscovery(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "bootstrap-key")
	if err := os.WriteFile(keyPath, []byte("credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAPIKey, "")
	t.Setenv(envAPIKeyFile, keyPath)
	t.Setenv(envControlPlaneURL, "https://api.example.com")
	t.Setenv(envWorkerSessionDir, t.TempDir())
	config, enabled, err := externalWorkerConfigFromEnv(false)
	if err != nil || !enabled || config.identityMode || config.identityURL != nil || config.sessionPath != "" {
		t.Fatalf("identity config = %#v, enabled=%t, err=%v", config, enabled, err)
	}
}

func testExternalWorkerIdentity(t *testing.T, authority externalWorkerConnection) *externalWorkerIdentity {
	t.Helper()
	privateKeyPEM, _, err := newExternalWorkerCSR()
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(privateKeyPEM))
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key := parsed.(*ecdsa.PrivateKey)
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "worker"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	identity := &externalWorkerIdentity{
		SessionID: "session", ProjectID: authority.ProjectID, EnvironmentID: authority.EnvironmentID,
		DeploymentID: authority.DeploymentID, WorkerPoolID: authority.WorkerPoolID, WorkerID: "worker",
		SPIFFEID:        "spiffe://example/workload/project/environment/deployment/worker",
		RuntimeEndpoint: authority.RuntimeEndpoint, CertificateDERBase64: base64.StdEncoding.EncodeToString(der),
		TrustBundleDERBase64: []string{base64.StdEncoding.EncodeToString(der)}, TrustBundleVersion: "v1",
		CertificateExpiresAt: now.Add(time.Hour), RenewAfter: now.Add(40 * time.Minute),
		WorkloadToken: "token", TokenType: "Bearer", TokenExpiresAt: now.Add(10 * time.Minute), PrivateKeyPEM: privateKeyPEM,
	}
	if _, err := json.Marshal(identity); err != nil {
		t.Fatal(err)
	}
	return identity
}
