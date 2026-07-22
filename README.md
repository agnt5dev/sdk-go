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

## Worker configuration

| Variable | Purpose |
| --- | --- |
| `AGNT5_COORDINATOR_ENDPOINT` | Worker coordinator endpoint |
| `AGNT5_ENGINE_URL` | Direct engine endpoint for checkpoints and events |
| `AGNT5_PROJECT_ID` | Project identity used by workers |
| `AGNT5_DEPLOYMENT_ID` | Deployment routing identity |
| `AGNT5_WORKER_MODE` | `push` or `pull` dispatch mode |
| `AGNT5_PROTOCOL_MODE` | `auto`, `v1`, or `v2` worker protocol |
| `AGNT5_MAX_CONCURRENCY` | Maximum concurrent work |
| `AGNT5_API_KEY` | Service key used by the client |
| `AGNT5_GATEWAY_URL` | Gateway base URL used by the client |

See the package configuration types for retry, slot, lease, queue, and
streaming controls.

### Worker protocol selection

Workers expose three protocol modes through `WithProtocolMode` or
`AGNT5_PROTOCOL_MODE`:

- `auto` is the default and selects v1 during the dual-stack rollout;
- `v1` forces the existing v1 push or pull transport; and
- `v2` negotiates protocol v2.0 and uses the session-pinned pull worker.

An explicit API option takes precedence over the environment. Values are
case-sensitive, and an invalid value fails when the worker starts. Forced v2
never falls back after authentication, invalid-request, timeout, or network
errors.

```go
worker := agnt5.NewWorker(
	"hello-go",
	agnt5.WithProtocolMode(agnt5.ProtocolModeV2),
)
```

The initial v2 adapter supports registration, replay-stable polling, lease
renewal, and fenced completed/failed outcome commits. The alpha.3 runtime does
not advertise durable event append, live output, referenced payloads, durable
operation replay, or suspended/cancelled/yielded outcomes. The SDK records
omitted observability-event counts in outcome metadata and rejects unsupported
correctness-driving payload, checkpoint, and outcome paths. Workflow, event
trigger, cron trigger, and trigger-expression declarations require their
matching runtime capabilities and fail during negotiation when unavailable;
plain functions continue to use the protocol's base pull operations. Go does
not yet expose a language-local v2 run-policy declaration, so `run_policy*`
component configuration is rejected instead of being silently dropped.

The adapter enforces the negotiated message and payload limits. Natural worker
session expiry drains in-flight poll slots before registration is renewed,
while a replaced session remains terminal so an old worker does not fight its
replacement. Cancellation and stale execution authority are scoped to the
affected execution and do not tear down the worker session.

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
