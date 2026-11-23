package agnt5

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

func writeCgroupFile(t *testing.T, root, file, contents string) {
	t.Helper()
	path := filepath.Join(root, file)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestResourceSamplerMemoryIsTheWorkingSetOverTheLimit(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "memory.max", "1073741824\n")
	writeCgroupFile(t, root, "memory.current", "805306368\n")
	writeCgroupFile(t, root, "memory.stat", "anon 536870912\nfile 268435456\ninactive_file 268435456\n")
	// 768 MiB charged, 256 MiB of it reclaimable cache, under 1 GiB.
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 0.5 {
		t.Fatalf("memory usage = %v, want 0.5", got)
	}

	writeCgroupFile(t, root, "memory.current", "2147483648\n")
	writeCgroupFile(t, root, "memory.stat", "inactive_file 0\n")
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 1 {
		t.Fatalf("memory usage over the limit = %v, want 1", got)
	}
}

func TestResourceSamplerMemoryReadsCgroupV1AndIsUnknownWithoutALimit(t *testing.T) {
	root := t.TempDir()
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 0 {
		t.Fatalf("memory usage without a cgroup = %v, want 0", got)
	}

	writeCgroupFile(t, root, "memory/memory.limit_in_bytes", "536870912\n")
	writeCgroupFile(t, root, "memory/memory.usage_in_bytes", "268435456\n")
	writeCgroupFile(t, root, "memory/memory.stat", "total_inactive_file 134217728\n")
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 0.25 {
		t.Fatalf("cgroup v1 memory usage = %v, want 0.25", got)
	}

	writeCgroupFile(t, root, "memory/memory.limit_in_bytes", "9223372036854771712\n")
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 0 {
		t.Fatalf("unlimited cgroup v1 memory usage = %v, want 0", got)
	}

	writeCgroupFile(t, root, "memory.max", "max\n")
	writeCgroupFile(t, root, "memory.current", "268435456\n")
	if got := (&resourceSampler{root: root}).memoryUsage(); got != 0 {
		t.Fatalf("unlimited cgroup v2 memory usage = %v, want 0", got)
	}
}

func TestResourceSamplerCPUIsTheTimeUsedOverTheQuotaSinceTheLastSample(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "cpu.max", "200000 100000\n")
	writeCgroupFile(t, root, "cpu.stat", "usage_usec 1000000\nuser_usec 900000\n")
	sampler := &resourceSampler{root: root}
	start := time.Now()
	if got := sampler.cpuUsage(start); got != 0 {
		t.Fatalf("first CPU sample = %v, want 0", got)
	}

	// One CPU-second in one second, on a two-core quota.
	writeCgroupFile(t, root, "cpu.stat", "usage_usec 2000000\nuser_usec 1800000\n")
	if got := sampler.cpuUsage(start.Add(time.Second)); got != 0.5 {
		t.Fatalf("CPU usage = %v, want 0.5", got)
	}

	// Throttled at the quota: two CPU-seconds in the next second.
	writeCgroupFile(t, root, "cpu.stat", "usage_usec 4000000\n")
	if got := sampler.cpuUsage(start.Add(2 * time.Second)); got != 1 {
		t.Fatalf("CPU usage at the quota = %v, want 1", got)
	}
}

func TestResourceSamplerCPUWithoutAQuotaUsesTheCoresAvailable(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "cpu.max", "max 100000\n")
	writeCgroupFile(t, root, "cpu.stat", "usage_usec 0\n")
	sampler := &resourceSampler{root: root}
	start := time.Now()
	sampler.cpuUsage(start)
	writeCgroupFile(t, root, "cpu.stat", "usage_usec 1000000\n")
	want := math.Min(1, 1/float64(runtime.NumCPU()))
	if got := sampler.cpuUsage(start.Add(time.Second)); math.Abs(got-want) > 1e-9 {
		t.Fatalf("CPU usage = %v, want %v", got, want)
	}
}

func TestResourceSamplerCPUReadsCgroupV1AndIsUnknownWithoutACgroup(t *testing.T) {
	root := t.TempDir()
	sampler := &resourceSampler{root: root}
	start := time.Now()
	if got := sampler.cpuUsage(start); got != 0 {
		t.Fatalf("CPU usage without a cgroup = %v, want 0", got)
	}
	if got := sampler.cpuUsage(start.Add(time.Second)); got != 0 {
		t.Fatalf("CPU usage without a cgroup = %v, want 0", got)
	}

	writeCgroupFile(t, root, "cpu/cpu.cfs_quota_us", "100000\n")
	writeCgroupFile(t, root, "cpu/cpu.cfs_period_us", "100000\n")
	writeCgroupFile(t, root, "cpuacct/cpuacct.usage", "0\n")
	sampler.cpuUsage(start)
	writeCgroupFile(t, root, "cpuacct/cpuacct.usage", "250000000\n")
	if got := sampler.cpuUsage(start.Add(time.Second)); got != 0.25 {
		t.Fatalf("cgroup v1 CPU usage = %v, want 0.25", got)
	}
}

func TestReportPullCapacityCarriesTheContainersMemoryUse(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "memory.max", "1073741824\n")
	writeCgroupFile(t, root, "memory.current", "805306368\n")
	writeCgroupFile(t, root, "memory.stat", "inactive_file 268435456\n")
	previousRoot := cgroupRoot
	cgroupRoot = root
	t.Cleanup(func() { cgroupRoot = previousRoot })

	server := &testEngine{capacity: make(chan *pb.ReportWorkerCapacityRequest, 1)}
	client := newTestEngineClient(t, server)
	worker := NewWorker("svc",
		WithWorkerID("worker-pull"),
		WithProjectID("proj-1"),
		WithDeploymentID("dep-1"),
	)
	var openPollSlots, activeSlots, desiredSlots atomic.Uint32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.reportPullCapacity(
		ctx,
		client,
		"session-1",
		pullSlotConfig{minSlots: 1, maxSlots: 1},
		&openPollSlots,
		&activeSlots,
		&desiredSlots,
		time.Hour,
		make(chan error, 1),
	)

	select {
	case report := <-server.capacity:
		if report.GetMemoryUsage() != 0.5 {
			t.Fatalf("reported memory usage = %v, want 0.5", report.GetMemoryUsage())
		}
		if report.GetCpuUsage() != 0 {
			t.Fatalf("first report's CPU usage = %v, want 0", report.GetCpuUsage())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a capacity report")
	}
}
