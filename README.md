# AGNT5 Go SDK

[![CI](https://github.com/agnt5dev/agnt5/actions/workflows/sdk-go-tests.yml/badge.svg)](https://github.com/agnt5dev/agnt5/actions/workflows/sdk-go-tests.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Build typed AGNT5 workers, durable workflows, and runtime clients in Go. The
SDK supports push and pull workers, checkpointed workflow steps, streaming,
batches, tools, agents, MCP, evaluation, sandbox interfaces, and structured
runtime events.

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
