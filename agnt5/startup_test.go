package agnt5

import (
	"bytes"
	"context"
	"strings"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
)

func TestStartupBannerListsRegisteredComponentsAndDashboard(t *testing.T) {
	dashboardURL := "https://app.agnt5.com/projects/project-1/components"
	t.Setenv(envDashboardURL, dashboardURL)

	worker := NewWorker("customer-service", WithServiceVersion("2.0.0"))
	var output bytes.Buffer
	worker.startupWriter = &output

	handler := func(*Context, []byte) ([]byte, error) { return nil, nil }
	for _, registration := range []struct {
		name          string
		componentType ComponentType
	}{
		{name: "write_report", componentType: ComponentTypeFunction},
		{name: "book_trip", componentType: ComponentTypeWorkflow},
		{name: "search_web", componentType: ComponentTypeTool},
		{name: "TravelAgent", componentType: ComponentTypeAgent},
		{name: "conduct_research", componentType: ComponentTypeFunction},
	} {
		if err := RegisterRaw(worker, registration.name, registration.componentType, handler); err != nil {
			t.Fatalf("register %s: %v", registration.name, err)
		}
	}

	worker.printStartupBanner()

	want := strings.Join([]string{
		"",
		"  customer-service v2.0.0",
		"  ────────────────────────────────────────",
		"  ◆ workflows (1)",
		"    └── book_trip",
		"  ƒ functions (2)",
		"    ├── conduct_research",
		"    └── write_report",
		"  ● agents (1)",
		"    └── TravelAgent",
		"  ◇ tools (1)",
		"    └── search_web",
		"  ────────────────────────────────────────",
		"  Dashboard: " + dashboardURL,
		"",
		"",
	}, "\n")
	if got := output.String(); got != want {
		t.Fatalf("startup banner mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
	if strings.Contains(output.String(), "scorers (0)") {
		t.Fatal("empty component sections must be omitted")
	}
}

func TestRunPrintsStartupBeforeCoordinatorConnection(t *testing.T) {
	t.Setenv(envDashboardURL, "https://app.agnt5.com/projects/project-1/components")
	server := &testCoordinator{
		ack:      true,
		received: make(chan *pb.ServiceMessage, 1),
	}
	listener := newTestCoordinatorListener(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-1"),
		WithCoordinatorEndpoint("http://bufnet"),
		withGRPCDialOptions(grpc.WithContextDialer(testBufconnDialer(listener))),
	)
	var output bytes.Buffer
	worker.startupWriter = &output
	if err := RegisterFunction(worker, "greet", func(_ *Context, input string) (string, error) {
		return input, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		t.Fatalf("run worker: %v", err)
	}

	text := output.String()
	dashboardIndex := strings.Index(text, "Dashboard: https://app.agnt5.com/projects/project-1/components")
	connectingIndex := strings.Index(text, "Connecting to coordinator (http://bufnet)...")
	connectedIndex := strings.Index(text, "Connected to coordinator (http://bufnet)")
	if dashboardIndex < 0 || connectingIndex <= dashboardIndex || connectedIndex <= connectingIndex {
		t.Fatalf("startup output is out of order:\n%s", text)
	}
}

func TestStartupBannerOmitsDashboardWhenCLIProvidesNone(t *testing.T) {
	t.Setenv(envDashboardURL, "")
	worker := NewWorker("svc")
	var output bytes.Buffer
	worker.startupWriter = &output
	worker.printStartupBanner()
	if strings.Contains(output.String(), "Dashboard:") {
		t.Fatalf("unexpected dashboard link:\n%s", output.String())
	}
}
