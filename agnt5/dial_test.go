package agnt5

import "testing"

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
		})
	}
}

func TestCoordinatorDialConfigRejectsUnsupportedScheme(t *testing.T) {
	_, err := coordinatorDialConfigFromEndpoint("ftp://runtime.example:34186")
	if err == nil {
		t.Fatal("expected unsupported scheme error")
	}
}
