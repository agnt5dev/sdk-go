package agnt5

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestCoordinatorDialConfigFromEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   string
		wantTarget string
		wantTLS    bool
		wantServer string
	}{
		{
			name:       "default",
			endpoint:   "",
			wantTarget: "localhost:34186",
			wantServer: "localhost",
		},
		{
			name:       "host port",
			endpoint:   "runtime.example:34186",
			wantTarget: "runtime.example:34186",
			wantServer: "runtime.example",
		},
		{
			name:       "localhost port",
			endpoint:   "localhost:34186",
			wantTarget: "localhost:34186",
			wantServer: "localhost",
		},
		{
			name:       "http",
			endpoint:   "http://runtime.example:34186",
			wantTarget: "runtime.example:34186",
			wantServer: "runtime.example",
		},
		{
			name:       "https",
			endpoint:   "https://grpc.agnt5.com:3418",
			wantTarget: "grpc.agnt5.com:3418",
			wantTLS:    true,
			wantServer: "grpc.agnt5.com",
		},
		{
			name:       "https without port defaults to 443",
			endpoint:   "https://grpc.agnt5.com",
			wantTarget: "grpc.agnt5.com:443",
			wantTLS:    true,
			wantServer: "grpc.agnt5.com",
		},
		{
			name:       "https without port and trailing slash",
			endpoint:   "https://grpc.agnt5.com/",
			wantTarget: "grpc.agnt5.com:443",
			wantTLS:    true,
			wantServer: "grpc.agnt5.com",
		},
		{
			name:       "http without port defaults to 80",
			endpoint:   "http://runtime.example",
			wantTarget: "runtime.example:80",
			wantServer: "runtime.example",
		},
		{
			name:       "https explicit 443",
			endpoint:   "https://grpc.agnt5.com:443",
			wantTarget: "grpc.agnt5.com:443",
			wantTLS:    true,
			wantServer: "grpc.agnt5.com",
		},
		{
			name:       "ipv6 with port",
			endpoint:   "https://[::1]:8443",
			wantTarget: "[::1]:8443",
			wantTLS:    true,
			wantServer: "::1",
		},
		{
			name:       "ipv6 without port",
			endpoint:   "https://[::1]",
			wantTarget: "[::1]:443",
			wantTLS:    true,
			wantServer: "::1",
		},
		{
			name:       "bare ipv6 host port",
			endpoint:   "[::1]:34186",
			wantTarget: "[::1]:34186",
			wantServer: "::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coordinatorDialConfigFromEndpoint(tt.endpoint)
			if err != nil {
				t.Fatalf("config: %v", err)
			}
			if got.target != tt.wantTarget || got.tls != tt.wantTLS || got.serverName != tt.wantServer {
				t.Fatalf("config = %#v", got)
			}
			if got.rootCAs != nil {
				t.Fatal("endpoint config must verify against system roots")
			}
		})
	}
}

func TestCoordinatorDialConfigTransportCredentials(t *testing.T) {
	secure, err := coordinatorDialConfigFromEndpoint("https://grpc.agnt5.com")
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	info := secure.transportCredentials().Info()
	if info.SecurityProtocol != "tls" || info.ServerName != "grpc.agnt5.com" {
		t.Fatalf("https credentials = %+v", info)
	}

	plain, err := coordinatorDialConfigFromEndpoint("http://runtime.example")
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if protocol := plain.transportCredentials().Info().SecurityProtocol; protocol != "insecure" {
		t.Fatalf("http security protocol = %q", protocol)
	}
}

func TestCoordinatorDialConfigRejectsUnsupportedScheme(t *testing.T) {
	_, err := coordinatorDialConfigFromEndpoint("ftp://runtime.example:34186")
	if err == nil {
		t.Fatal("expected unsupported scheme error")
	}
}

// TestDialRuntimeOverTLSWithoutPort dials a real TLS gRPC listener from an
// https URL that has no port, the shape worker discovery returns. A context
// dialer stands in for DNS and port 443 so the test can see the address gRPC
// asked for and still complete a verified TLS handshake.
func TestDialRuntimeOverTLSWithoutPort(t *testing.T) {
	serverCert, roots := newLocalhostCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	dial := func(t *testing.T, config coordinatorDialConfig) (string, error) {
		t.Helper()
		var mu sync.Mutex
		var dialed string
		dialer := grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			mu.Lock()
			dialed = address
			mu.Unlock()
			return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		})
		conn, err := dialRuntimeConfig(config, dialer)
		if err != nil {
			t.Fatalf("dialRuntimeConfig: %v", err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		mu.Lock()
		defer mu.Unlock()
		return dialed, err
	}

	t.Run("trusted certificate", func(t *testing.T) {
		config, err := coordinatorDialConfigFromEndpoint("https://localhost")
		if err != nil {
			t.Fatalf("config: %v", err)
		}
		config.rootCAs = roots
		dialed, err := dial(t, config)
		if err != nil {
			t.Fatalf("health check over TLS: %v", err)
		}
		if dialed != "localhost:443" {
			t.Fatalf("dialed %q, want localhost:443", dialed)
		}
	})

	t.Run("untrusted certificate is rejected", func(t *testing.T) {
		config, err := coordinatorDialConfigFromEndpoint("https://localhost")
		if err != nil {
			t.Fatalf("config: %v", err)
		}
		_, err = dial(t, config)
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("expected certificate verification error, got %v", err)
		}
	})

	t.Run("server name mismatch is rejected", func(t *testing.T) {
		config, err := coordinatorDialConfigFromEndpoint("https://runtime.example")
		if err != nil {
			t.Fatalf("config: %v", err)
		}
		config.rootCAs = roots
		dialed, err := dial(t, config)
		if dialed != "runtime.example:443" {
			t.Fatalf("dialed %q, want runtime.example:443", dialed)
		}
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("expected certificate name error, got %v", err)
		}
	})
}

func newLocalhostCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots
}
