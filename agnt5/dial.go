package agnt5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type coordinatorDialConfig struct {
	target     string
	tls        bool
	serverName string
	// rootCAs is nil in production so TLS verifies against the system roots;
	// tests set it to trust a local certificate authority.
	rootCAs *x509.CertPool
}

func dialCoordinator(ctx context.Context, endpoint string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return dialRuntime(ctx, endpoint, extraOpts...)
}

func dialEngine(ctx context.Context, endpoint string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return dialRuntime(ctx, endpoint, extraOpts...)
}

func dialRuntime(ctx context.Context, endpoint string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := coordinatorDialConfigFromEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	return dialRuntimeConfig(config, extraOpts...)
}

func dialRuntimeConfig(config coordinatorDialConfig, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(config.transportCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	opts = append(opts, extraOpts...)
	return grpc.NewClient("passthrough:///"+config.target, opts...)
}

func coordinatorDialConfigFromEndpoint(endpoint string) (coordinatorDialConfig, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = defaultCoordinatorEndpoint
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return coordinatorDialConfig{}, fmt.Errorf("agnt5: invalid coordinator endpoint %q: %w", endpoint, err)
	}
	switch parsed.Scheme {
	case "http", "https":
		if parsed.Host == "" {
			return coordinatorDialConfig{}, fmt.Errorf("agnt5: invalid coordinator endpoint %q: missing host", endpoint)
		}
		// gRPC dials host:port, so a URL without a port (as discovery returns,
		// e.g. https://grpc.agnt5.com) gets the scheme's standard port.
		target := parsed.Host
		if parsed.Port() == "" {
			port := "80"
			if parsed.Scheme == "https" {
				port = "443"
			}
			target = net.JoinHostPort(parsed.Hostname(), port)
		}
		return coordinatorDialConfig{
			target:     target,
			tls:        parsed.Scheme == "https",
			serverName: parsed.Hostname(),
		}, nil
	default:
		return coordinatorDialConfig{}, fmt.Errorf("agnt5: unsupported coordinator endpoint scheme %q", parsed.Scheme)
	}
}

func (c coordinatorDialConfig) transportCredentials() credentials.TransportCredentials {
	if !c.tls {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(&tls.Config{ServerName: c.serverName, RootCAs: c.rootCAs, MinVersion: tls.VersionTLS12})
}

func newWorkerCoordinatorClient(conn grpc.ClientConnInterface) pb.WorkerCoordinatorServiceClient {
	return pb.NewWorkerCoordinatorServiceClient(conn)
}

func newEngineServiceClient(conn grpc.ClientConnInterface) pb.EngineServiceClient {
	return pb.NewEngineServiceClient(conn)
}

func withGRPCDialOptions(opts ...grpc.DialOption) WorkerOption {
	return func(w *Worker) {
		w.grpcDialOptions = append(w.grpcDialOptions, opts...)
	}
}
