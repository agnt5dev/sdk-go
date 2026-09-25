# AGNT5 Go SDK

[![CI](https://github.com/agnt5dev/agnt5/actions/workflows/sdk-go-tests.yml/badge.svg)](https://github.com/agnt5dev/agnt5/actions/workflows/sdk-go-tests.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Build typed AGNT5 workers, durable workflows, and runtime clients in Go. The
SDK supports push and pull workers, checkpointed workflow steps, streaming,
batches, tools, agents, progressively disclosed skills, MCP, evaluation,
sandbox interfaces, and structured runtime events.

## Requirements

- Go 1.26.5 or newer
- An AGNT5 runtime for deployed execution

## Installation

The module path is `github.com/agnt5dev/sdk-go`:

```bash
go get github.com/agnt5dev/sdk-go@latest
```


## Quick start

Register typed functions and workflows with a worker:

```go
package main

import (
	"context"
	"log"

	"github.com/agnt5dev/sdk-go/agnt5"
)

type GreetInput struct {
	Name string `json:"name"`
}

type GreetOutput struct {
	Message string `json:"message"`
}

func main() {
	worker := agnt5.NewWorker("hello-go")

	err := agnt5.RegisterFunction(
		worker,
		"greet",
		func(ctx *agnt5.Context, input GreetInput) (GreetOutput, error) {
			ctx.Logger().Info("greeting user", "name", input.Name)
			return GreetOutput{Message: "Hello, " + input.Name + "!"}, nil
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

See [`examples/quickstart`](examples/quickstart) for a runnable function and a
workflow with a durable step.

## Worker logs and traces

Workers export application and lifecycle logs, invocation spans, and nested
step/model spans through OTLP gRPC. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to your
collector, or use `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` and
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` for separate destinations. Traces are disabled
when both the trace endpoint and shared endpoint are unset. Logs retain their
existing fallback to `http://grpc.agnt5.com:3418`. Export is best effort; worker
shutdown drains both queues within a shared five-second limit.

`ctx.Logger()` keeps writing durable journal events and also exports application
logs. To forward standard `log/slog` records, wrap your handler before starting
workers:

```go
logger := slog.New(agnt5.NewSlogHandler(slog.NewJSONHandler(os.Stderr, nil)))
slog.SetDefault(logger) // optional: use logger directly for a private logger

// Inside a component; derived contexts retain run and trace attribution.
slog.InfoContext(ctx, "processing order", "order_id", orderID)
```

The bridge preserves the supplied handler's output, groups, and level filter.
Pass the invocation context to `InfoContext`, `ErrorContext`, and similar calls.
Calls without that context stay with the local handler. The SDK does not replace
process-global loggers or trace providers.

Records carry `log_source=application`, `agnt5.run.id`, `run_id`, and active
`trace_id`/`span_id` fields. The worker resource supplies project, deployment,
worker, and application identity. Spans also carry canonical workspace, project,
and deployment identity for trace access checks. W3C `traceparent` metadata is continued; pull
jobs with only a runtime trace ID retain that ID without fabricating a parent.
Spans include component names and attempt/error information; they do not add
handler input/output or model prompts to telemetry.

## Invoke a deployed component

```go
client, err := agnt5.NewClient(
	"https://gw.agnt5.com",
	agnt5.WithAPIKey("agnt5_sk_..."),
	agnt5.WithClientDeploymentID("deployment-id"),
)
if err != nil {
	log.Fatal(err)
}

response, err := client.Run(
	context.Background(),
	"greet",
	GreetInput{Name: "Ada"},
)
if err != nil {
	log.Fatal(err)
}

var output GreetOutput
if err := response.DecodeOutput(&output); err != nil {
	log.Fatal(err)
}
```

The client also supports submission and polling, SSE streams, batches,
cancellation, workflow resume, chat, and evaluation.

## Evaluate components

Single and concurrent batch evaluation use the same scorer specs as the Python
and TypeScript SDKs:

```go
result, err := client.Eval(context.Background(), agnt5.EvalRequest{
	Component: "greet",
	Input: map[string]any{"name": "Ada"},
	Expected: "Hello, Ada!",
	Scorers: agnt5.NormalizeEvalScorers(
		"exact_match",
		agnt5.Correctness{},
	),
})
if err != nil {
	log.Fatal(err)
}

score, ok := result.GetScore("exact_match")
```

The SDK includes all AGNT5 deterministic and judge built-ins, versioned judge
presets, trace assertions, tool-trajectory helpers, typed scorer errors, and
`Client.BatchEval`. Pull workers advertise locally executable built-ins, and
all workers intercept built-in scorer dispatch before custom component lookup.

Custom scorer names cannot shadow AGNT5 built-ins:

```go
err := agnt5.RegisterScorer(worker, agnt5.ScorerConfig{
	Name: "quality_check",
	Scope: agnt5.ScorerScopeItem,
	Handler: func(ctx context.Context, request agnt5.ScorerRequest) (agnt5.ScorerResult, error) {
		return agnt5.PassingScorerResult("quality checks passed"), nil
	},
})
```

## Agents and chat

`NewAgent` requires an explicit `LanguageModel`; the Go SDK does not silently
select or emulate a provider. Agents default to 10 model turns, and
`WithAgentMaxTurns` can lower or raise that limit. Ordinary tool failures are
recorded in `ToolCallDetails` and returned to the model as tool messages so it
can recover. HITL pause errors still stop the loop for runtime resumption.

`RegisterChatBot` advertises the bot as an `agent` component because that is the
runtime chat routing contract. `Client.Chat` sends the gateway's `message`
request shape and decodes the returned run envelope. Chat metadata values must
be strings, matching the gateway contract.

Live MCP clients perform the initialize handshake lazily, correlate concurrent
JSON-RPC responses through a single reader, discard late canceled responses,
and propagate connection shutdown to pending calls. HTTP/SSE connections retain
the negotiated `MCP-Session-Id`.

Agents can load selected `SKILL.md` capabilities on demand and materialize their
bundled resources into a sandbox without placing full skill bodies in the
initial prompt:

```go
sandbox := agnt5.NewInMemorySandbox()
agent, err := agnt5.NewAgent(
	"researcher",
	agnt5.WithAgentModel(model),
	agnt5.WithAgentInstructions("Use the matching skill before acting."),
	agnt5.WithAgentSandbox(sandbox),
	agnt5.WithAgentSkillsFromDir("./skills", "pdf-extraction"),
)
```

`SkillFromPath`, `DiscoverSkills`, `ResolveSkills`, and `WithAgentSkills` support
pre-resolved or programmatic skill catalogs. `DiscoverAgentsMD`, `LoadAgentsMD`,
and `WithAgentGuidance` add outermost-first project guidance before the skill
catalog. A configured sandbox automatically adds the standard execute, write,
read, and list tools; its file interface also supports recursive deletion.

## Worker configuration

| Variable | Purpose |
| --- | --- |
| `AGNT5_COORDINATOR_ENDPOINT` | Worker coordinator endpoint |
| `AGNT5_ENGINE_URL` | Direct engine endpoint for checkpoints and events |
| `AGNT5_PROJECT_ID` | Project identity used by workers |
| `AGNT5_DEPLOYMENT_ID` | Deployment routing identity |
| `AGNT5_WORKER_MODE` | `push` or `pull` dispatch mode |
| `AGNT5_MAX_CONCURRENCY` | Maximum concurrent work |
| `AGNT5_API_KEY` | Service key used by the client |
| `AGNT5_GATEWAY_URL` | Gateway base URL used by the client |

### Customer-hosted workers

For a worker on a customer Docker host or Kubernetes cluster, set
`AGNT5_API_KEY_FILE` instead of coordinator, engine, project, and deployment coordinates. The SDK
recognizes the file-backed bootstrap automatically; there is no separate external-worker mode.
`AGNT5_ENVIRONMENT` is an optional
placement selector, and `AGNT5_CONTROL_PLANE_URL` defaults to
`https://api.agnt5.com`.

`Worker.Run` discovers its authorized placement, exchanges the service key for
a short-lived workload bearer, switches to pull mode, and refreshes credentials
on reconnect. The service key is never sent to the runtime. External endpoints
require verified TLS except for the explicit loopback development path.

See the package configuration types for retry, slot, lease, queue, and
streaming controls.

## Examples

- [`examples/quickstart`](examples/quickstart) — functions and workflows
- [`examples/pull-worker`](examples/pull-worker) — pull-mode worker
- [`examples/streaming`](examples/streaming) — streaming output
- [`examples/serverless-http`](examples/serverless-http) — serverless HTTP

The shared Rust foundation and cross-SDK conformance contracts live in
[`agnt5dev/sdk-core`](https://github.com/agnt5dev/sdk-core).

## Development

For local pull-worker benchmarks, set `AGNT5_CORE_METRICS_LOGS=1` to emit
`AGNT5_CORE_METRIC` JSON observations on stderr. These correlate slot occupancy
through completion acknowledgment, actual step body time, and activation RPC
time including retries. They include run and worker identifiers, never input
or output payloads. Observation logging is disabled by default.

```bash
gofmt -w .
go vet ./...
go test ./...
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues according to
[SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).

## Response wait

Run and stream calls wait up to **5 minutes** by default. Set the per-call wait
to any value from zero to 24 hours. Zero returns a pending receipt immediately
after acceptance. This controls response waiting, not the workflow execution
deadline: accepted work continues when the wait expires or the client disconnects.

```go
options := []agnt5.RunOption{
    agnt5.WithRunComponentType(agnt5.ComponentTypeWorkflow),
    agnt5.WithWaitTimeout(time.Minute),
}
result, err := client.Run(ctx, "process_order", order, options...)
err = client.StreamEvents(ctx, "process_order", order, handleEvent, options...)
```

`WithWaitTimeout` accepts durations in whole milliseconds. `Run` returns `202`
pending receipts directly, without additional polling. `StreamEvents` delivers
`stream.wait_expired` when the wait expires, or `stream.detached` for a `202`
receipt. Use the run ID to read status/results. Chunk-only `Stream` returns
`RunError` with the run ID when waiting ends.

The default HTTP timeout allows at least the wait plus 10 seconds, or the client
timeout if longer. Use `WithRunTimeout(75*time.Second)` to set it explicitly;
the context deadline may end the request earlier.

`BatchStream` and the `/batch/stream` endpoint have been removed. Use `Batch`
and `GetBatchStatus` to submit and observe batch work.

## Structured assertions

`structured_assertions` is a reserved built-in scorer. The pure-Go implementation
conforms to the [SDK-core contract](https://github.com/agnt5dev/sdk-core/tree/82e98e984749f80a31ff6302ba508d55974c608d/crates/eval-scorers)
and runs through `ScorerRegistry.Run` without user registration. It can also run locally:

```go
result := agnt5.StructuredAssertions(agnt5.ScorerRequest{
    Output: []int{1, 2, 3},
    Expected: map[string]any{"expected_length": 3},
    Config: map[string]any{"assertions": []any{
        map[string]any{"name": "unique_ids", "expr": "unique(output_json)"},
        map[string]any{"name": "count", "expr": "size(output_json) == expected.expected_length"},
    }},
})
```

The score is the fraction of assertions that pass; `score_threshold` defaults to
1. Configuration and input errors always fail. Assertions use a bounded JSON
expression language; they never execute Go code.
