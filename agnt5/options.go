package agnt5

import (
	"strings"
	"time"
)

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
		w.legacyRoutingSet = w.legacyRoutingSet || strings.TrimSpace(endpoint) != ""
	}
}

// WithEngineEndpoint sets the direct engine endpoint for durable journal writes.
// Leave empty to rely on coordinator terminal-response fallback behavior.
func WithEngineEndpoint(endpoint string) WorkerOption {
	return func(w *Worker) {
		w.engineEndpoint = endpoint
		w.legacyRoutingSet = w.legacyRoutingSet || strings.TrimSpace(endpoint) != ""
	}
}

// WithProjectID sets the project routing key for worker metadata.
func WithProjectID(projectID string) WorkerOption {
	return func(w *Worker) {
		w.projectID = projectID
		w.legacyRoutingSet = w.legacyRoutingSet || strings.TrimSpace(projectID) != ""
		if projectID != "" {
			w.metadata["project_id"] = projectID
		}
	}
}

// WithDeploymentID sets the deployment routing key for worker metadata.
func WithDeploymentID(deploymentID string) WorkerOption {
	return func(w *Worker) {
		w.deploymentID = deploymentID
		w.legacyRoutingSet = w.legacyRoutingSet || strings.TrimSpace(deploymentID) != ""
		if deploymentID != "" {
			w.metadata["deployment_id"] = deploymentID
		}
	}
}

// WithWorkspaceID sets the workspace identity attached to telemetry resources.
func WithWorkspaceID(workspaceID string) WorkerOption {
	return func(w *Worker) {
		w.workspaceID = workspaceID
		if workspaceID != "" {
			w.metadata["workspace_id"] = workspaceID
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

// WithDurableActivationMode selects disabled, preferred, or required startup
// behavior for durable_activation_v1 protocol negotiation.
func WithDurableActivationMode(mode DurableActivationMode) WorkerOption {
	return func(w *Worker) {
		switch mode {
		case DurableActivationDisabled, DurableActivationPreferred, DurableActivationRequired:
			w.durableActivationMode = mode
		}
	}
}

// WithDurableActivationArtifact supplies the immutable deployed artifact
// SHA-256 used in activation definition identity. Managed runtimes normally
// inject this value; local E2E workers may set it explicitly.
func WithDurableActivationArtifact(sha256 string) WorkerOption {
	return func(w *Worker) {
		if sha256 != "" {
			w.metadata[activationArtifactSHA256Metadata] = sha256
		}
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
