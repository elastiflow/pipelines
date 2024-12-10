package main

import (
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

func duplicateProcess(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
	return p.Broadcast(
		pipe.Params{Num: 2},
	).FanIn() // Broadcasting by X then Fanning In will create X duplicates per T.
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
	pl := pipelines.New[int]( // Create a new Pipeline
		inChan,
		errChan,
		duplicateProcess,
	)
	go func(errReceiver <-chan error) { // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				return
			}
		}
	}(errChan)
	go seedPipeline(inChan)      // Seed Pipeline inputs
	for out := range pl.Open() { // Read Pipeline output
		if out == 9 {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}
}
