# AGNT5 Go SDK

Status: worker MVP, core gateway client, and broad application-surface parity
APIs for the Go kitchen-sink template.

This package is the Go SDK for AGNT5 durable workers. The current slice provides
the public worker, registry, context, logging, event-classification, and typed
handler API. Push-mode `Worker.Run` can dial the worker coordinator, register
the service, execute dispatched function/workflow handlers, echo lease IDs, and
honor coordinator cancellation. Pull-mode `Worker.Run` registers a parked worker
session, polls jobs, renews leases, reports capacity, and completes jobs through
`CompleteJob`. Transient transport failures retry with a bounded reconnect
backoff by default; `AGNT5_MAX_RETRIES=0` matches `sdk-core` and retries
forever. When `AGNT5_ENGINE_URL` is set, the worker writes non-terminal
lifecycle records through `EngineService.Append`, batches buffered durable
boundary events through `EngineService.AppendBatch`, and streams transient
SSE-only events through `EngineService.EventStream` for streaming runs.
Terminal `run.completed`/`run.failed` records use the coordinator response path
in push mode and `CompleteJob` in pull mode. `Step[T]` uses the runtime
checkpoint API for durable step start/completion/failure records and memoized
output replay. The package also includes a first gateway `Client` for deployed
component calls: `Run`, `Submit`, `GetStatus`, `GetResult`, `WaitForResult`,
`GetEvents`, `Stream`, `StreamEvents`, `Batch`, `BatchStream`,
`GetBatchStatus`, `CancelBatch`, and `CancelRun`. The current parity layer adds
scoped state/memory helpers, tools, HITL request errors, MCP client types,
provider-neutral LLM interfaces, agents, scorers/evals, sandbox interfaces,
chat helpers, workflow/session client proxies, and chat/eval client wrappers.
The LLM layer includes OpenAI-compatible and Anthropic adapters, and the agent
layer supports bounded model/tool loops, local tool execution, handoffs, and an
agent registry/manager. When the worker has a direct engine client,
state/session memory uses the engine state API; otherwise it falls back to
in-memory state for local tests and examples. HITL pauses emit
`workflow.paused`, `Client.ResumeWorkflow` targets the gateway resume endpoint,
and push fire-and-forget handling treats paused workflows as terminal until
resumed. MCP includes static, stdio, and HTTP/SSE transports. Sandbox includes
the deterministic in-memory provider plus `HTTPSandbox` for a runtime-backed
sandbox endpoint, with sandbox operation events emitted through the handler
context.

## Current Example Shape

```go
package main

import (
	"context"
	"log"

	"agnt5.dev/sdk-go/agnt5"
)

type GreetInput struct {
	Name string `json:"name"`
}

type GreetOutput struct {
	Message string `json:"message"`
}

func main() {
	worker := agnt5.NewWorker("go-quickstart")

	err := agnt5.RegisterFunction(worker, "greet", func(ctx *agnt5.Context, in GreetInput) (GreetOutput, error) {
		ctx.Logger().Info("greeting user", "name", in.Name)
		return GreetOutput{Message: "hello " + in.Name}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	err = agnt5.RegisterWorkflow(worker, "greet_workflow", func(ctx *agnt5.Context, in GreetInput) (GreetOutput, error) {
		message, err := agnt5.Step(ctx, "build_message", func(context.Context) (string, error) {
			ctx.Logger().Info("building workflow greeting", "name", in.Name)
			return "hello " + in.Name, nil
		})
		if err != nil {
			return GreetOutput{}, err
		}
		return GreetOutput{Message: message}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

`Worker.Run` currently supports push and pull modes. The Go quickstart template
is available under `sdk/templates/go/go-quickstart`.

For local HA runtime smoke testing, run the worker with the local coordinator
and deployment routing keys:

```bash
AGNT5_COORDINATOR_ENDPOINT=http://localhost:34182 \
AGNT5_ENGINE_URL=http://localhost:34182 \
AGNT5_PROJECT_ID=<project-id> \
AGNT5_DEPLOYMENT_ID=<deployment-id> \
go run ./examples/quickstart
```

To smoke pull mode, also set `AGNT5_WORKER_MODE=pull`.

Then invoke through the runtime gateway using the same deployment target. The
current verified smoke is:

```bash
curl -X POST http://localhost:34183/v1/functions/greet/run \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: <service-key>" \
  -H "X-DEPLOYMENT-ID: <deployment-id>" \
  -d '{"name":"Ada"}'
```

That returns `{"message":"hello Ada"}` and records `run.started`,
`function.started`, `function.completed`, and `run.completed` in the durable run
event history. `log.*`, `output.*`, `progress.*`, and LLM stream events are
transient SSE-only events: they are delivered for streaming runs and dropped for
non-streaming runs. Pull-mode smoke also verifies `dispatch_mode=pull` and
terminal completion through `CompleteJob`.

The quickstart also registers a workflow with one durable step:

```bash
curl -X POST http://localhost:34183/v1/workflows/greet_workflow/run \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: <service-key>" \
  -H "X-DEPLOYMENT-ID: <deployment-id>" \
  -d '{"name":"Step Ada"}'
```

That returns `{"message":"hello Step Ada"}` and records `checkpoint.created`
for `step:build_message:0` along with `workflow.step.started`,
`workflow.step.completed`, `workflow.completed`, and `run.completed`.

## Client Invocation

```go
client, err := agnt5.NewClient("http://localhost:34183",
	agnt5.WithAPIKey("agnt5_sk_..."),
	agnt5.WithClientDeploymentID("<deployment-id>"),
)
if err != nil {
	log.Fatal(err)
}

response, err := client.Run(context.Background(), "greet", GreetInput{Name: "Ada"},
	agnt5.WithRunSessionID("session-123"),
	agnt5.WithRunHeader("Idempotency-Key", "greet-ada-1"),
)
if err != nil {
	log.Fatal(err)
}

var output GreetOutput
if err := response.DecodeOutput(&output); err != nil {
	log.Fatal(err)
}
log.Println(output.Message)
```

For async work, call `Submit`, poll with `GetStatus`, then read the terminal
payload with `GetResult` or `WaitForResult`. `CancelRun` requests gateway
cancellation for an in-flight run. `GetEvents` returns journal records for a
run; `Stream` and `StreamEvents` consume the gateway SSE endpoint.

For batch work, call `Batch` with either plain inputs or `BatchItemInput`
values:

```go
batch, err := client.Batch(context.Background(), "greet", []any{
	GreetInput{Name: "Ada"},
	agnt5.NewBatchItem(
		GreetInput{Name: "Grace"},
		agnt5.WithBatchItemID("user-grace"),
		agnt5.WithBatchItemMetadata(map[string]string{"source": "nightly"}),
	),
}, agnt5.WithBatchMaxConcurrency(5))
if err != nil {
	log.Fatal(err)
}

status, err := client.GetBatchStatus(context.Background(), batch.BatchID, true, 30*time.Second)
if err != nil {
	log.Fatal(err)
}
log.Println(status.Status)
```

For live batch updates, call `BatchStream` and handle each SSE event until the
terminal `batch.completed` or `batch.cancelled` event.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGNT5_GATEWAY_URL` | `https://gw.agnt5.com` | Client gateway base URL |
| `AGNT5_API_KEY` | empty | Client service key, sent as `X-API-KEY` |
| `AGNT5_TENANT_ID` | empty | Client default sub-tenant, sent as `X-TENANT-ID` |
| `AGNT5_COORDINATOR_ENDPOINT` | `http://localhost:34186` | Worker coordinator or runtime endpoint |
| `AGNT5_ENGINE_URL` | empty | Direct engine endpoint for append, checkpoint, and event stream RPCs |
| `AGNT5_WORKER_ID` | generated `go-...` | Worker identity |
| `AGNT5_PROJECT_ID` | empty | Required for direct engine writes and pull workers |
| `AGNT5_DEPLOYMENT_ID` | empty | Deployment routing key |
| `AGNT5_WORKER_MODE` | `push` | Set to `pull` for parked pull workers |
| `AGNT5_MAX_CONCURRENCY` | `0` | Push in-flight concurrency limit; pull max slot default when set |
| `AGNT5_MAX_RETRIES` | `5` | Coordinator reconnect retry budget; `0` means retry forever |
| `AGNT5_MIN_SLOTS` | `1` | Pull worker minimum parked pollers |
| `AGNT5_MAX_SLOTS` | max concurrency or `1` | Pull worker maximum/desired pollers |
| `AGNT5_CLAIM_TIMEOUT_MS` | `300000` | Pull job lease duration |
| `AGNT5_JOURNAL_QUEUE_SIZE` | `1000` | Max buffered durable handler events per invocation flush |
| `AGNT5_JOURNAL_BATCH_SIZE` | `100` | `AppendBatch` chunk size |
| `AGNT5_JOURNAL_FLUSH_INTERVAL_MS` | `100` | Reserved flush cadence setting for parity with `sdk-core` |

## Step Idempotency

`Step[T]` derives a deterministic run-local key from the step name and call
count, such as `step:build_message:0`. Keep step names stable and keep the
order of step calls stable across retries. Put side effects inside `Step` so
the runtime can skip already-completed work on replay. Step outputs must be JSON
serializable because memoized outputs are stored and decoded as JSON.

## Current Limits

- The Go SDK now has concrete APIs and kitchen-sink coverage for the main
  Python/TypeScript application surfaces. Remaining parity work is narrower:
  richer agent callbacks and skills/AGENTS.md discovery, and semantic/vector
  memory beyond the state-backed memory helpers.
- The public module path is `agnt5.dev/sdk-go`, but the vanity import endpoint
  must be configured before external users can `go get` it. The intended
  release shape is a dedicated module-root repository tagged with plain
  `v0.x.y` tags.
- Generated protobufs are SDK-local under `internal/pb` for `v0.x`.
- Managed Go template deployment depends on publishing
  `ghcr.io/agnt5dev/go-worker:latest`; the repo now owns the Dockerfile and CI
  job, but registry publication still has to happen.
- Pull streaming requires the runtime to mark the job with `is_streaming=true`
  or `stream_mode=full`; SSE-only logs and output deltas then use Engine
  `EventStream`.
- Live Go kitchen-sink conformance is wired into
  `.github/workflows/sdk-go-tests.yml` as a manual `workflow_dispatch` job. It
  requires a running Go kitchen-sink worker plus `AGNT5_GO_*` repository
  secrets. The contract covers cancellation, `/stream` SSE output ordering,
  batch submit plus per-item result polling, and pull-mode metadata when the
  manual `include-pull-mode` input is enabled.

## Verification

```bash
cd sdk/sdk-go
GOCACHE=/private/tmp/agnt5-go-cache GOTOOLCHAIN=local go test ./...
```

The shared conformance harness lives under `sdk/e2e/conformance`. Run its unit tests
from the repository root:

```bash
python3 -m unittest discover -s sdk/e2e/conformance -p '*_test.py'
```

When a runtime and the Go kitchen-sink worker are running, use
`sdk/e2e/conformance/contracts/ks_analyze_text.yaml` with
`sdk/e2e/conformance/run_kitchen_sink_contract.py` for live parity checks.
