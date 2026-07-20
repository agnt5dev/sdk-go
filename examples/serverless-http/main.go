package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	"agnt5.dev/sdk-go/serverless"
)

type helloInput struct {
	Name string `json:"name"`
}

func main() {
	handler := serverless.New(serverless.Options{
		ServiceName:    "agnt5-serverless-go",
		ServiceVersion: os.Getenv("GIT_SHA"),
		SigningSecret: func(*http.Request) string {
			return os.Getenv("AGNT5_SERVERLESS_SIGNING_SECRET")
		},
	})

	err := serverless.RegisterWorkflow(handler, "hello", func(ctx *serverless.Context, input helloInput) (map[string]string, error) {
		name, err := serverless.Step(ctx, "normalize-name", func(context.Context) (string, error) {
			if input.Name == "" {
				return "world", nil
			}
			return input.Name, nil
		})
		if err != nil {
			return nil, err
		}
		return map[string]string{"message": "hello " + name}, ctx.YieldIfNeeded()
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := serverless.RegisterFunction(handler, "uppercase", func(_ *serverless.Context, input helloInput) (map[string]string, error) {
		return map[string]string{"name": strings.ToUpper(input.Name)}, nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := serverless.RegisterTool(handler, serverless.Tool{
		Name:        "order-status",
		Description: "Return the status of an order",
		Schema:      map[string]any{"type": "object", "required": []string{"order_id"}},
		Handler: func(_ context.Context, input map[string]any) (any, error) {
			return map[string]any{"order_id": input["order_id"], "status": "ready"}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}
	if err := serverless.RegisterAgent(handler, serverless.Agent{
		Name: "greeter",
		Run: func(_ *serverless.Context, input serverless.AgentInput) (serverless.AgentResult, error) {
			return serverless.AgentResult{Output: "hello " + input.Message}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	log.Println("AGNT5 serverless Go endpoint listening on http://127.0.0.1:8787")
	log.Fatal(http.ListenAndServe("127.0.0.1:8787", handler))
}
