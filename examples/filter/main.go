package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/elastiflow/pipelines/pipe"
)

func filter(p int) (bool, error) {
	return p%2 == 0, nil
}

func mapFunc(p int) (string, error) {
	return fmt.Sprintf("Im an even number: %d", p), nil
}

func main() {
	inChan := make(chan int)
	errChan := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer func() {
		close(errChan)
		cancel()
	}()

	out := pipe.New[int, string](
		ctx,
		inChan,
		errChan,
	).FanOut(pipe.Params{Num: 3}).
		Filter(filter).
		Map(mapFunc).
		OrDone().
		Out()

	go func() {
		// Process to seed the pipeline
		for i := range 10 {
			inChan <- i
		}
		close(inChan)
	}()

	for val := range out {
		slog.Info("received simple pipeline output", slog.String("out", val))
	}

	// Output:
	// {"out":"I'm an even number: 0"}
	// {"out":"I'm an even number: 2"}
	// {"out":"I'm an even number: 4"}
	// {"out":"I'm an even number: 6"}
	// {"out":"I'm an even number: 8"}
}
