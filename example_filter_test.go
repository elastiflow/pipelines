package pipelines_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// filter returns true if the input int is even, false otherwise.
func filter(p int) (bool, error) {
	return p%2 == 0, nil
}

// mapFunc transforms an even integer into a descriptive string.
func mapFunc(p int) (string, error) {
	return fmt.Sprintf("I'm an even number: %d", p), nil
}

func Example_filter() {
	errChan := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer func() {
		close(errChan)
		cancel()
	}()

	// Build a pipeline that fans out, then filters even numbers, then maps to string
	pl := pipelines.New[int, string](
		ctx,
		sources.FromArray(createIntArr(10)),
		errChan,
	).Start(func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
		return datastreams.Map(
			p.FanOut(
				datastreams.Params{Num: 3},
			).Filter(
				filter,
			),
			mapFunc,
		)
	})

	for val := range pl.Out() {
		slog.Info("received simple pipeline output", slog.String("out", val))
	}

	// Output (example):
	// {"out":"I'm an even number: 0"}
	// {"out":"I'm an even number: 2"}
	// {"out":"I'm an even number: 4"}
	// {"out":"I'm an even number: 6"}
	// {"out":"I'm an even number: 8"}
}
