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

func exProcess(p pipe.Pipe[int]) pipe.Pipe[int] {
	return p.OrDone().FanOut(
		pipe.Params{Num: 2},
	).Run(
		squareOdds,
	)
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
	pl := pipelines.New[int]( // Create a new Pipeline
		inChan,
		errChan,
		exProcess,
	)
	go func(errReceiver <-chan error) { // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(errChan)
	go seedPipeline(inChan)
	var i int                    // Seed Pipeline inputs
	for out := range pl.Open() { // Read Pipeline output
		if i == 9 {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.Int("out", out))
		i++
	}
}
