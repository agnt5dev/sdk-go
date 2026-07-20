package agnt5

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "agnt5.dev/sdk-go/internal/pb/api/v1"
	"google.golang.org/grpc"
)

const (
	defaultServiceVersion      = "0.1.2"
	defaultServiceType         = "go"
	defaultCoordinatorEndpoint = "http://localhost:34186"
	defaultMaxReconnects       = uint32(5)
	defaultReconnectBackoff    = 250 * time.Millisecond
	defaultReconnectBackoffMax = 5 * time.Second
	defaultReconnectResetAfter = 30 * time.Second
	defaultJournalQueueSize    = uint32(1_000)
	defaultJournalBatchSize    = uint32(100)
	defaultJournalFlushMS      = int64(100)
	envCoordinatorEndpoint     = "AGNT5_COORDINATOR_ENDPOINT"
	envEngineURL               = "AGNT5_ENGINE_URL"
	envWorkerID                = "AGNT5_WORKER_ID"
	envProjectID               = "AGNT5_PROJECT_ID"
	envDeploymentID            = "AGNT5_DEPLOYMENT_ID"
	envWorkerMode              = "AGNT5_WORKER_MODE"
	envMaxConcurrency          = "AGNT5_MAX_CONCURRENCY"
	envMaxRetries              = "AGNT5_MAX_RETRIES"
	envMinSlots                = "AGNT5_MIN_SLOTS"
	envMaxSlots                = "AGNT5_MAX_SLOTS"
	envClaimTimeoutMS          = "AGNT5_CLAIM_TIMEOUT_MS"
	envJournalQueueSize        = "AGNT5_JOURNAL_QUEUE_SIZE"
	envJournalBatchSize        = "AGNT5_JOURNAL_BATCH_SIZE"
	envJournalFlushIntervalMS  = "AGNT5_JOURNAL_FLUSH_INTERVAL_MS"
)

// Worker owns component registration and, once transport is wired, runtime
// connectivity for a Go service.
type Worker struct {
	workerID            string
	serviceName         string
	serviceVersion      string
	serviceType         string
	coordinatorEndpoint string
	engineEndpoint      string
	projectID           string
	deploymentID        string
	workerMode          WorkerMode
	maxConcurrency      uint32
	metadata            map[string]string
	registry            *Registry
	grpcDialOptions     []grpc.DialOption
	eventWriter         eventWriter
	checkpointWriter    stepCheckpointWriter
	stateStore          StateStore
	maxReconnects       uint32
	reconnectBackoff    time.Duration
	reconnectBackoffMax time.Duration
	reconnectResetAfter time.Duration
	journalQueueSize    uint32
	journalBatchSize    uint32
	journalFlushEvery   time.Duration
}

// NewWorker constructs a Go worker with environment-compatible defaults.
func NewWorker(serviceName string, opts ...WorkerOption) *Worker {
	w := &Worker{
		workerID:            envOrDefault(envWorkerID, newWorkerID()),
		serviceName:         serviceName,
		serviceVersion:      defaultServiceVersion,
		serviceType:         defaultServiceType,
		coordinatorEndpoint: envOrDefault(envCoordinatorEndpoint, defaultCoordinatorEndpoint),
		engineEndpoint:      os.Getenv(envEngineURL),
		projectID:           os.Getenv(envProjectID),
		deploymentID:        os.Getenv(envDeploymentID),
		workerMode:          workerModeFromEnv(),
		maxConcurrency:      uint32FromEnv(envMaxConcurrency),
		metadata:            make(map[string]string),
		registry:            NewRegistry(),
		maxReconnects:       uint32FromEnvDefault(envMaxRetries, defaultMaxReconnects),
		reconnectBackoff:    defaultReconnectBackoff,
		reconnectBackoffMax: defaultReconnectBackoffMax,
		reconnectResetAfter: defaultReconnectResetAfter,
		journalQueueSize:    uint32FromEnvDefault(envJournalQueueSize, defaultJournalQueueSize),
		journalBatchSize:    uint32FromEnvDefault(envJournalBatchSize, defaultJournalBatchSize),
		journalFlushEvery:   time.Duration(int64FromEnvDefault(envJournalFlushIntervalMS, defaultJournalFlushMS)) * time.Millisecond,
	}
	if w.projectID != "" {
		w.metadata["project_id"] = w.projectID
	}
	if w.deploymentID != "" {
		w.metadata["deployment_id"] = w.deploymentID
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	w.syncRuntimeMetadata()
	return w
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func newWorkerID() string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil {
		return "go-" + hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("go-%d", time.Now().UnixNano())
}

func workerModeFromEnv() WorkerMode {
	switch WorkerMode(os.Getenv(envWorkerMode)) {
	case WorkerModePull:
		return WorkerModePull
	default:
		return WorkerModePush
	}
}

func uint32FromEnv(name string) uint32 {
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(parsed)
}

func uint32FromEnvDefault(name string, fallback uint32) uint32 {
	if value, ok := os.LookupEnv(name); !ok || value == "" {
		return fallback
	}
	return uint32FromEnv(name)
}

func (w *Worker) syncRuntimeMetadata() {
	if w.projectID != "" {
		w.metadata["project_id"] = w.projectID
		w.metadata["AGNT5_PROJECT_ID"] = w.projectID
	}
	if w.deploymentID != "" {
		w.metadata["deployment_id"] = w.deploymentID
		w.metadata["AGNT5_DEPLOYMENT_ID"] = w.deploymentID
	}
	if w.maxConcurrency > 0 {
		w.metadata["AGNT5_MAX_CONCURRENCY"] = fmt.Sprintf("%d", w.maxConcurrency)
	}
	w.metadata["AGNT5_MAX_RETRIES"] = fmt.Sprintf("%d", w.maxReconnects)
}

// WorkerID returns the runtime stream identity advertised by this process.
func (w *Worker) WorkerID() string {
	return w.workerID
}

// ServiceName returns the service name advertised by the worker.
func (w *Worker) ServiceName() string {
	return w.serviceName
}

// ServiceVersion returns the service version advertised by the worker.
func (w *Worker) ServiceVersion() string {
	return w.serviceVersion
}

// ServiceType returns the service type advertised by the worker.
func (w *Worker) ServiceType() string {
	return w.serviceType
}

// CoordinatorEndpoint returns the coordinator endpoint the worker will dial.
func (w *Worker) CoordinatorEndpoint() string {
	return w.coordinatorEndpoint
}

// EngineEndpoint returns the optional direct engine endpoint used for journal writes.
func (w *Worker) EngineEndpoint() string {
	return w.engineEndpoint
}

// ProjectID returns the configured project routing key.
func (w *Worker) ProjectID() string {
	return w.projectID
}

// DeploymentID returns the configured deployment routing key.
func (w *Worker) DeploymentID() string {
	return w.deploymentID
}

// WorkerMode returns the configured assignment mode.
func (w *Worker) WorkerMode() WorkerMode {
	return w.workerMode
}

// MaxConcurrency returns the configured in-flight invocation budget.
func (w *Worker) MaxConcurrency() uint32 {
	return w.maxConcurrency
}

// Metadata returns a defensive copy of service metadata.
func (w *Worker) Metadata() map[string]string {
	return cloneStringMap(w.metadata)
}

// Registry returns the component registry owned by the worker.
func (w *Worker) Registry() *Registry {
	return w.registry
}

// Components returns deterministic registration descriptors for this worker.
func (w *Worker) Components() []ComponentInfo {
	return w.registry.ComponentInfos()
}

// Run connects this worker to AGNT5 and starts the push-mode worker stream.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if w.workerMode == WorkerModePull {
		return w.runWithReconnect(ctx, func(ctx context.Context) error {
			return w.runPull(ctx)
		})
	}
	return w.runWithReconnect(ctx, func(ctx context.Context) error {
		conn, err := dialCoordinator(ctx, w.coordinatorEndpoint, w.grpcDialOptions...)
		if err != nil {
			return err
		}
		defer conn.Close()
		restore, err := w.configureRuntimeEventWriter(ctx)
		if err != nil {
			return err
		}
		defer restore()
		return w.runWorkerStream(ctx, newWorkerCoordinatorClient(conn))
	})
}

func (w *Worker) runPull(ctx context.Context) error {
	endpoint := w.engineEndpoint
	if endpoint == "" {
		endpoint = w.coordinatorEndpoint
	}
	conn, err := dialEngine(ctx, endpoint, w.grpcDialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := newEngineServiceClient(conn)
	restore := w.configureEventWriterFromClient(client)
	defer restore()
	return w.runPullWorker(ctx, client)
}

func (w *Worker) configureRuntimeEventWriter(ctx context.Context) (func(), error) {
	if (w.eventWriter != nil && w.checkpointWriter != nil) || w.engineEndpoint == "" {
		return func() {}, nil
	}
	conn, err := dialEngine(ctx, w.engineEndpoint, w.grpcDialOptions...)
	if err != nil {
		return nil, err
	}
	previousEventWriter := w.eventWriter
	previousCheckpointWriter := w.checkpointWriter
	previousStateStore := w.stateStore
	writer := newEngineEventWriter(newEngineServiceClient(conn))
	if w.eventWriter == nil {
		w.eventWriter = writer
	}
	if w.checkpointWriter == nil {
		w.checkpointWriter = writer
	}
	if w.stateStore == nil {
		w.stateStore = newEngineStateStore(writer.client, w.projectID)
	}
	return func() {
		w.eventWriter = previousEventWriter
		w.checkpointWriter = previousCheckpointWriter
		w.stateStore = previousStateStore
		_ = conn.Close()
	}, nil
}

func (w *Worker) configureEventWriterFromClient(client pb.EngineServiceClient) func() {
	if (w.eventWriter != nil && w.checkpointWriter != nil) || client == nil {
		return func() {}
	}
	previousEventWriter := w.eventWriter
	previousCheckpointWriter := w.checkpointWriter
	previousStateStore := w.stateStore
	writer := newEngineEventWriter(client)
	if w.eventWriter == nil {
		w.eventWriter = writer
	}
	if w.checkpointWriter == nil {
		w.checkpointWriter = writer
	}
	if w.stateStore == nil {
		w.stateStore = newEngineStateStore(client, w.projectID)
	}
	return func() {
		w.eventWriter = previousEventWriter
		w.checkpointWriter = previousCheckpointWriter
		w.stateStore = previousStateStore
	}
}

func (w *Worker) runWithReconnect(ctx context.Context, runOnce func(context.Context) error) error {
	backoff := w.reconnectBackoff
	retries := uint32(0)
	for {
		startedAt := time.Now()
		err := runOnce(ctx)
		if err == nil || !shouldReconnect(err) {
			return err
		}
		if w.reconnectBudgetShouldReset(time.Since(startedAt)) {
			retries = 0
			backoff = w.reconnectBackoff
		}
		if w.maxReconnects > 0 && retries >= w.maxReconnects {
			return err
		}
		retries++
		if sleepErr := sleepContext(ctx, backoff); sleepErr != nil {
			return sleepErr
		}
		backoff = nextBackoff(backoff, w.reconnectBackoffMax)
	}
}

func (w *Worker) reconnectBudgetShouldReset(elapsed time.Duration) bool {
	resetAfter := w.reconnectResetAfter
	if resetAfter <= 0 {
		resetAfter = defaultReconnectResetAfter
	}
	return elapsed >= resetAfter
}

func shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrRegistrationRejected) ||
		errors.Is(err, ErrUnexpectedRuntimeMessage) ||
		errors.Is(err, ErrTransportNotImplemented) ||
		errors.Is(err, ErrWorkerReplaced) {
		return false
	}
	return true
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current <= 0 || max <= 0 {
		return 0
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func (w *Worker) invoke(ctx context.Context, inv Invocation, streamParentCorrelationID ...string) (result InvocationResult, err error) {
	component, ok := w.registry.Get(inv.ComponentName)
	if !ok {
		return InvocationResult{}, ErrComponentNotFound
	}
	if inv.ComponentType != "" && inv.ComponentType != component.Type {
		return InvocationResult{}, ErrComponentNotFound
	}
	runCtx := newContext(ctx, inv, w.checkpointWriter, canonicalProjectID(w.invocationMetadata(inv)), w.stateStore)
	if inv.IsStreaming {
		parentCorrelationID := ""
		if len(streamParentCorrelationID) > 0 {
			parentCorrelationID = streamParentCorrelationID[0]
		}
		runCtx.setStreamEmitter(func(event Event) error {
			return w.streamInvocationEvent(ctx, inv, event, w.invocationMetadata(inv), parentCorrelationID)
		})
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = InvocationResult{
				LeaseID: inv.LeaseID,
				Events:  runCtx.Events(),
			}
			err = fmt.Errorf("agnt5: handler panic: %v", recovered)
		}
	}()
	output, err := component.invoke(runCtx, inv.Input)
	result = InvocationResult{
		Output:  output,
		LeaseID: inv.LeaseID,
		Events:  runCtx.Events(),
	}
	if err != nil {
		return result, err
	}
	return result, nil
}
