package main

import (
	"context"
	"log"

	"github.com/agnt5dev/sdk-go/agnt5"
)

type StreamInput struct {
	Name string `json:"name"`
}

type StreamOutput struct {
	Message string `json:"message"`
}

func main() {
	worker := agnt5.NewWorker("go-streaming-example")

	err := agnt5.RegisterFunction(worker, "stream_greet", func(ctx *agnt5.Context, in StreamInput) (StreamOutput, error) {
		ctx.Logger().Info("starting streaming greeting", "name", in.Name)
		ctx.Output("hello")
		ctx.Output(" ")
		ctx.Output(in.Name)
		return StreamOutput{Message: "hello " + in.Name}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
