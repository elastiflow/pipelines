package main

import (
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

func duplicateProcess[T any](p pipe.Pipe[T], params *pipe.Params) pipe.Pipe[T] {
	return p.Broadcast(params).FanIn(pipe.NoParams()) // Broadcasting by X then Fanning In will create X duplicates per T.
}

func seedPipeline(inChan chan<- int) {
	for i := 0; i < 10; i++ {
		inChan <- i
	}
}

func main() {
	inChan := make(chan int) // Setup channels and cleanup
	errChan := make(chan error)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	props := pipelines.NewProps[int]( // Create new Pipeline properties
		pipe.ProcessRegistry[int]{},
		inChan,
		errChan,
		2,
	)
	pl := pipelines.New[int](props, duplicateProcess[int]) // Create a new Pipeline
	go func(errReceiver <-chan error) {                    // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				return
			}
		}
	}(errChan)
	go seedPipeline(inChan)         // Seed Pipeline inputs
	for out := range pl.Open(nil) { // Read Pipeline output
		if out == 9 {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}
}
