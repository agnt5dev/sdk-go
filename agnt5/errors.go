package agnt5

import "errors"

var (
	ErrComponentNotFound        = errors.New("agnt5: component not found")
	ErrDuplicateComponent       = errors.New("agnt5: duplicate component")
	ErrInvalidComponentName     = errors.New("agnt5: invalid component name")
	ErrNilHandler               = errors.New("agnt5: nil handler")
	ErrNilWorker                = errors.New("agnt5: nil worker")
	ErrMissingRoutingMetadata   = errors.New("agnt5: missing project or deployment routing metadata")
	ErrInvalidStepName          = errors.New("agnt5: invalid step name")
	ErrRegistrationRejected     = errors.New("agnt5: worker registration rejected")
	ErrTransportNotImplemented  = errors.New("agnt5: worker transport not implemented")
	ErrUnexpectedRuntimeMessage = errors.New("agnt5: unexpected runtime message")
	ErrWorkerReplaced           = errors.New("agnt5: worker replaced")
	ErrCoordinatorDraining      = errors.New("agnt5: coordinator draining")
	ErrAgentModelRequired       = errors.New("agnt5: agent model is required")
	ErrAgentMaxTurnsExceeded    = errors.New("agnt5: agent max turns exceeded")
	ErrToolNotFound             = errors.New("agnt5: tool not found")
	ErrMCPTransportClosed       = errors.New("agnt5: MCP transport closed")
)
