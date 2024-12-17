package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/sources"
)

func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

func squareOdds(v int) (int, error) {
	if v%2 == 0 {
		return v, fmt.Errorf("even number error: %v", v)
	}
	return v * v, nil
}

func exProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
	return p.OrDone().FanOut(
		datastreams.Params{Num: 2},
	).Run(
		squareOdds,
	)
}

func main() {
	errChan := make(chan error, 10)
	defer close(errChan)

	pl := pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)),
		errChan,
	).With(exProcess)

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
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}

	// Output:
	// {"out":0}
	// {"out":1}
	// {"out":4}
	// {"out":9}
	// {"out":16}
	// {"out":25}
	// {"out":36}
	// {"out":49}
	// {"out":64}
	// {"out":81}
}
