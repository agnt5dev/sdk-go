package agnt5

import "time"

// WorkerOption mutates Worker configuration during construction.
type WorkerOption func(*Worker)

// WithWorkerID sets the runtime stream identity advertised by the worker.
func WithWorkerID(workerID string) WorkerOption {
	return func(w *Worker) {
		if workerID != "" {
			w.workerID = workerID
		}
	}
}

// WithServiceVersion sets the version advertised during worker registration.
func WithServiceVersion(version string) WorkerOption {
	return func(w *Worker) {
		if version != "" {
			w.serviceVersion = version
		}
	}
}

// WithServiceType sets the service type advertised during worker registration.
func WithServiceType(serviceType string) WorkerOption {
	return func(w *Worker) {
		if serviceType != "" {
			w.serviceType = serviceType
		}
	}
}

// WithCoordinatorEndpoint sets the runtime coordinator endpoint.
func WithCoordinatorEndpoint(endpoint string) WorkerOption {
	return func(w *Worker) {
		w.coordinatorEndpoint = endpoint
	}
}

// WithEngineEndpoint sets the direct engine endpoint for durable journal writes.
// Leave empty to rely on coordinator terminal-response fallback behavior.
func WithEngineEndpoint(endpoint string) WorkerOption {
	return func(w *Worker) {
		w.engineEndpoint = endpoint
	}
}

// WithProjectID sets the project routing key for worker metadata.
func WithProjectID(projectID string) WorkerOption {
	return func(w *Worker) {
		w.projectID = projectID
		if projectID != "" {
			w.metadata["project_id"] = projectID
		}
	}
}

// WithDeploymentID sets the deployment routing key for worker metadata.
func WithDeploymentID(deploymentID string) WorkerOption {
	return func(w *Worker) {
		w.deploymentID = deploymentID
		if deploymentID != "" {
			w.metadata["deployment_id"] = deploymentID
		}
	}
}

// WithWorkerMode sets push or pull assignment mode.
func WithWorkerMode(mode WorkerMode) WorkerOption {
	return func(w *Worker) {
		switch mode {
		case WorkerModePush, WorkerModePull:
			w.workerMode = mode
		}
	}
}

// WithProtocolMode selects the runtime wire protocol. A valid API option takes
// precedence over AGNT5_PROTOCOL_MODE. Invalid values fail when the worker is
// started, preserving the existing NewWorker signature.
func WithProtocolMode(mode ProtocolMode) WorkerOption {
	return func(w *Worker) {
		parsed, err := parseProtocolMode(string(mode))
		w.protocolMode = parsed
		w.protocolConfigErr = err
	}
}

// WithMaxConcurrency sets the max in-flight invocation budget.
func WithMaxConcurrency(maxConcurrency uint32) WorkerOption {
	return func(w *Worker) {
		w.maxConcurrency = maxConcurrency
	}
}

// WithMaxReconnects sets the retry budget after transient coordinator failures.
func WithMaxReconnects(maxReconnects uint32) WorkerOption {
	return func(w *Worker) {
		w.maxReconnects = maxReconnects
	}
}

// WithReconnectBackoff sets the initial and maximum reconnect delay.
func WithReconnectBackoff(initial, max time.Duration) WorkerOption {
	return func(w *Worker) {
		if initial >= 0 {
			w.reconnectBackoff = initial
		}
		if max >= 0 {
			w.reconnectBackoffMax = max
		}
	}
}

// WithMetadata adds service-level metadata advertised during registration.
func WithMetadata(metadata map[string]string) WorkerOption {
	return func(w *Worker) {
		for key, value := range metadata {
			w.metadata[key] = value
		}
	}
}

// WithStateStore overrides the default run/session/user state backend.
func WithStateStore(store StateStore) WorkerOption {
	return func(w *Worker) {
		w.stateStore = store
	}
}

func withEventWriter(writer eventWriter) WorkerOption {
	return func(w *Worker) {
		w.eventWriter = writer
	}
}

func withCheckpointWriter(writer stepCheckpointWriter) WorkerOption {
	return func(w *Worker) {
		w.checkpointWriter = writer
	}
}
