package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

func squareOdds(v int) (int, error) {
	return v * v, nil
}

func mapFunc(p int) (string, error) {
	return fmt.Sprintf("Im a squared number: %d", p), nil
}

func main() {
	inChan := make(chan int) // Setup channels and cleanup
	errChan := make(chan error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	pl := pipelines.New[int, string]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)),
		errChan,
	).Start(func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
		return datastreams.Map(
			p.OrDone().FanOut(
				datastreams.Params{Num: 2},
			).Run(
				squareOdds,
			),
			mapFunc,
		)
	})

	go func(errReceiver <-chan error) { // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.Errors())
	for out := range pl.Out() { // Read Pipeline output
		slog.Info("received simple pipeline output", slog.String("out", out))
	}

	// Output:
	// {"out":"I'm a squared number: 0"}
	// {"out":"I'm a squared number: 1"}
	// {"out":"I'm a squared number: 4"}
	// {"out":"I'm a squared number: 9"}
	// {"out":"I'm a squared number: 16"}
	// {"out":"I'm a squared number: 25"}
	// {"out":"I'm a squared number: 36"}
	// {"out":"I'm a squared number: 49"}
	// {"out":"I'm a squared number: 64"}
	// {"out":"I'm a squared number: 81"}
}
