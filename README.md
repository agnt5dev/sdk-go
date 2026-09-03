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
