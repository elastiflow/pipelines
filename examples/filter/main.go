package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

func filter(p int) (bool, error) {
	return p%2 == 0, nil
}

func mapFunc(p int) (string, error) {
	return fmt.Sprintf("Im an even number: %d", p), nil
}

func main() {
	errChan := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer func() {
		close(errChan)
		cancel()
	}()

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

	// Output:
	// {"out":"I'm an even number: 0"}
	// {"out":"I'm an even number: 2"}
	// {"out":"I'm an even number: 4"}
	// {"out":"I'm an even number: 6"}
	// {"out":"I'm an even number: 8"}
}
