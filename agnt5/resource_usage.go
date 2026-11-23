package agnt5

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// The worker container's CPU and memory use, as fractions of its limits,
// for each capacity report.
//
// The runtime stops adding slots to a worker whose CPU or memory use has
// reached the targets in its slot policy. All of a worker's runs share one
// process, so a worker close to its memory limit that takes on more runs
// can be killed with every run it holds. Usage is read from the container's
// cgroup, v2 first and then v1. Off Linux, outside a cgroup, or with no limit
// set it reads as 0, which the runtime treats as unknown.

// cgroupRoot is where the container's cgroup is mounted; tests point it at a
// fake tree.
var cgroupRoot = "/sys/fs/cgroup"

// cgroup v1 reports an unset memory limit as a page-aligned math.MaxInt64.
const cgroupV1UnlimitedBytes = uint64(1) << 60

// resourceSampler samples the container's resource use between capacity
// reports. CPU use is a rate, so each sample measures from the one before it.
type resourceSampler struct {
	root          string
	havePrevious  bool
	previousAt    time.Time
	previousUsage uint64 // microseconds of CPU time
}

func newResourceSampler() *resourceSampler {
	return &resourceSampler{root: cgroupRoot}
}

// memoryUsage is memory in use over the container's memory limit, in [0, 1].
// Page cache the kernel can reclaim is left out, as Kubernetes does for its
// working set, so cached files don't make a worker look full.
func (s *resourceSampler) memoryUsage() float64 {
	read := func(dir, limitFile, currentFile, inactiveKey string) (uint64, uint64, bool) {
		limit, ok := readCgroupLimit(filepath.Join(dir, limitFile))
		if !ok {
			return 0, 0, false
		}
		current, ok := readCgroupUint(filepath.Join(dir, currentFile))
		if !ok {
			return 0, 0, false
		}
		inactive, _ := readCgroupStat(filepath.Join(dir, "memory.stat"), inactiveKey)
		if inactive > current {
			inactive = current
		}
		return current - inactive, limit, true
	}
	used, limit, ok := read(s.root, "memory.max", "memory.current", "inactive_file")
	if !ok {
		used, limit, ok = read(filepath.Join(s.root, "memory"), "memory.limit_in_bytes", "memory.usage_in_bytes", "total_inactive_file")
	}
	if !ok {
		return 0
	}
	return usageFraction(float64(used), float64(limit))
}

// cpuUsage is the CPU time used since the previous sample over the CPU time
// the container may use in that interval, in [0, 1]. The first sample has
// nothing to measure from and reads as 0.
func (s *resourceSampler) cpuUsage(now time.Time) float64 {
	usage, ok := s.cpuUsageMicros()
	if !ok {
		s.havePrevious = false
		return 0
	}
	previousAt, previousUsage, hadPrevious := s.previousAt, s.previousUsage, s.havePrevious
	s.previousAt, s.previousUsage, s.havePrevious = now, usage, true
	if !hadPrevious || usage < previousUsage {
		return 0
	}
	elapsed := float64(now.Sub(previousAt).Microseconds())
	return usageFraction(float64(usage-previousUsage), elapsed*s.cpuLimitCores())
}

func (s *resourceSampler) cpuUsageMicros() (uint64, bool) {
	if usage, ok := readCgroupStat(filepath.Join(s.root, "cpu.stat"), "usage_usec"); ok {
		return usage, true
	}
	nanos, ok := readCgroupUint(filepath.Join(s.root, "cpuacct", "cpuacct.usage"))
	return nanos / 1000, ok
}

// cpuLimitCores is the cores the container may use: its CPU quota, or every
// core the process can run on when it has none.
func (s *resourceSampler) cpuLimitCores() float64 {
	if raw, err := os.ReadFile(filepath.Join(s.root, "cpu.max")); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) == 2 {
			quota, quotaErr := strconv.ParseFloat(fields[0], 64)
			period, periodErr := strconv.ParseFloat(fields[1], 64)
			if quotaErr == nil && periodErr == nil && quota > 0 && period > 0 {
				return quota / period
			}
		}
	}
	quota, quotaOK := readCgroupFloat(filepath.Join(s.root, "cpu", "cpu.cfs_quota_us"))
	period, periodOK := readCgroupFloat(filepath.Join(s.root, "cpu", "cpu.cfs_period_us"))
	if quotaOK && periodOK && quota > 0 && period > 0 {
		return quota / period
	}
	return float64(runtime.NumCPU())
}

// usageFraction is used over limit, clamped to [0, 1], or 0 when there is no
// limit.
func usageFraction(used, limit float64) float64 {
	if limit <= 0 {
		return 0
	}
	fraction := used / limit
	switch {
	case fraction < 0:
		return 0
	case fraction > 1:
		return 1
	default:
		return fraction
	}
}

func readCgroupTrimmed(path string) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(raw)), true
}

func readCgroupUint(path string) (uint64, bool) {
	raw, ok := readCgroupTrimmed(path)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	return value, err == nil
}

func readCgroupFloat(path string) (float64, bool) {
	raw, ok := readCgroupTrimmed(path)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseFloat(raw, 64)
	return value, err == nil
}

// readCgroupLimit reads a memory limit in bytes, or reports none set: "max"
// on cgroup v2, a value near math.MaxInt64 on v1.
func readCgroupLimit(path string) (uint64, bool) {
	limit, ok := readCgroupUint(path)
	if !ok || limit == 0 || limit >= cgroupV1UnlimitedBytes {
		return 0, false
	}
	return limit, true
}

// readCgroupStat reads one "key value" line of a cgroup stat file such as
// memory.stat.
func readCgroupStat(path, key string) (uint64, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		name, value, found := strings.Cut(line, " ")
		if found && name == key {
			parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			return parsed, err == nil
		}
	}
	return 0, false
}
