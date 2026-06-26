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
	worker := agnt5.NewWorker("go-pull-example",
		agnt5.WithWorkerMode(agnt5.WorkerModePull),
	)

	err := agnt5.RegisterFunction(worker, "greet", func(ctx *agnt5.Context, in GreetInput) (GreetOutput, error) {
		ctx.Logger().Info("greeting from pull worker", "name", in.Name)
		return GreetOutput{Message: "hello " + in.Name}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
