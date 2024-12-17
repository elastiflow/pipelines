package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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

// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
type PipelineWrapper struct {
	mu          sync.Mutex
	errChan     chan error
	evenCounter int
	oddCounter  int
	pipeline    *pipelines.Pipeline[int, int]
}

// NewPipelineWrapper creates a new PipelineWrapper with counters set to 0
func NewPipelineWrapper() *PipelineWrapper {
	// Setup channels and return PipelineWrapper
	errChan := make(chan error, 10)
	return &PipelineWrapper{
		errChan:     errChan,
		evenCounter: 0,
		oddCounter:  0,
	}
}

// Run runs the PipelineWrapper
func (pl *PipelineWrapper) Run() {
	defer close(pl.errChan)

	pl.pipeline = pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)), // Create a new source
		make(chan error, 10),
	).With(pl.exampleProcess)

	defer pl.pipeline.Close()
	go func(errReceiver <-chan error) { // Handle Pipeline errors
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.errChan)
	for out := range pl.pipeline.Out() { // Read Pipeline output
		slog.Info("received simple pipeline output", slog.Int("out", out))
		// Continue processing if needed
	}
}

// squareOdds is a method used in the PipelineWrapper.exProcess method
func (pl *PipelineWrapper) squareOdds(v int) (int, error) {
	if v%2 == 0 {
		pl.mu.Lock()
		pl.evenCounter++
		pl.mu.Unlock()
		return v, fmt.Errorf("even number error: %v", v)
	}
	pl.mu.Lock()
	pl.oddCounter++
	pl.mu.Unlock()
	return v * v, nil
}

// exampleProcess is the pipeline.ProcessorFunc method used in this example.
func (pl *PipelineWrapper) exampleProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
	return p.OrDone().FanOut(
		datastreams.Params{Num: 2},
	).Run(
		pl.squareOdds,
	)
}

func main() {
	plWrapper := NewPipelineWrapper()
	plWrapper.Run()
}
