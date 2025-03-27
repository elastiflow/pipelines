package pipelines_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// squareOdds is a basic function to demonstrate transformations in the pipeline.
func squareOdds(v int) (int, error) {
	return v * v, nil
}

// mapTransformFunc formats the squared integer as a string.
func mapTransformFunc(p int) (string, error) {
	return fmt.Sprintf("I'm a squared number: %d", p), nil
}

func Example_transformations() {
	inChan := make(chan int) // Setup channels
	errChan := make(chan error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()

	// Create a new Pipeline of int->string
	pl := pipelines.New[int, string](
		context.Background(),
		sources.FromArray(createIntArr(10)),
		errChan,
	).Start(func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
		// OrDone -> FanOut(2) -> Run (squareOdds) -> Map to string
		return datastreams.Map(
			p.OrDone().FanOut(
				datastreams.Params{Num: 2},
			).Run(
				squareOdds,
			),
			mapTransformFunc,
		)
	})

	// Handle errors
	go func(errReceiver <-chan error) {
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.Errors())

	// Read pipeline output
	for out := range pl.Out() {
		slog.Info("received simple pipeline output", slog.String("out", out))
	}

	// Output (example):
	// {"out":"I'm a squared number: 0"}
	// {"out":"I'm a squared number: 1"}
	// {"out":"I'm a squared number: 4"}
	// {"out":"I'm a squared number: 9"}
	// ...
	// {"out":"I'm a squared number: 81"}
}
