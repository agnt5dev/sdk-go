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
	worker := agnt5.NewWorker("go-quickstart",
		agnt5.WithMaxConcurrency(16),
	)

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
