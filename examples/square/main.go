package main

import (
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

func squareOdds(v int) (int, error) {
	if v%2 == 0 {
		return v, fmt.Errorf("even number error: %v", v)
	}
	return v * v, nil
}

func exProcess[T any](p pipe.Pipe[T], params *pipe.Params) pipe.Pipe[T] {
	return p.OrDone(nil).FanOut(params).Run("squareOdds", nil)
}

func seedPipeline(inChan chan<- int) {
	for i := 0; i < 10; i++ {
		inChan <- i
	}
}

func main() {
	inChan := make(chan int) // Setup channels and cleanup
	errChan := make(chan error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	props := pipelines.NewProps[int]( // Create new Pipeline properties
		pipe.ProcessRegistry[int]{
			"squareOdds": squareOdds,
		},
		inChan,
		errChan,
		2,
	)
	pl := pipelines.New[int](props, exProcess[int]) // Create a new Pipeline
	go func(errReceiver <-chan error) {             // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(errChan)
	go seedPipeline(inChan)
	var i int                       // Seed Pipeline inputs
	for out := range pl.Open(nil) { // Read Pipeline output
		if i == 9 {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.Int("out", out))
		i++
	}
}
