package main

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"github.com/elastiflow/pipelines/sources"
	"log/slog"

	"github.com/elastiflow/pipelines"
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

func exProcess(p pipe.DataStream[int]) pipe.DataStream[int] {
	return p.OrDone().FanOut(
		pipe.Params{Num: 2},
	).Run(
		squareOdds,
	)
}

func mapFunc(p int) (string, error) {
	return fmt.Sprintf("Im a squared number: %d", p), nil
}

func main() {
	inChan := make(chan int) // Setup channels and cleanup
	errChan := make(chan errors.Error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	pl := pipelines.FromSource[int, string]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)),
		errChan,
	).With(exProcess).
		Map(mapFunc)

	go func(errReceiver <-chan errors.Error) { // Handle Pipeline errors
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
