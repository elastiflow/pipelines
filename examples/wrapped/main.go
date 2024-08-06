package main

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
type PipelineWrapper struct {
	mu          sync.Mutex
	errChan     chan error
	inChan      chan int
	evenCounter int
	oddCounter  int
	pipeline    *pipelines.Pipeline[int]
}

// NewPipelineWrapper creates a new PipelineWrapper with counters set to 0
func NewPipelineWrapper() *PipelineWrapper {
	// Setup channels and return PipelineWrapper
	inChan := make(chan int)
	errChan := make(chan error, 10)
	return &PipelineWrapper{
		errChan:     errChan,
		inChan:      inChan,
		evenCounter: 0,
		oddCounter:  0,
	}
}

// Run runs the PipelineWrapper
func (pl *PipelineWrapper) Run() {
	defer func() {
		close(pl.inChan)
		close(pl.errChan)
	}()
	pl.pipeline = pipelines.New[int]( // Create a new Pipeline
		pl.inChan,
		pl.errChan,
		pl.exampleProcess,
	)
	go func(errReceiver <-chan error) { // Handle Pipeline errors
		defer pl.pipeline.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.errChan)
	go seedPipeline(pl.inChan) // Seed Pipeline inputs
	var i int
	for out := range pl.pipeline.Open() { // Read Pipeline output
		if i == 9 {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.Int("out", out))
		i++
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

// exampleProcess is the pipeline.Processor method used in this example.
func (pl *PipelineWrapper) exampleProcess(p pipe.Pipe[int]) pipe.Pipe[int] {
	return p.OrDone().FanOut(
		pipe.Params{Num: 2},
	).Run(
		pl.squareOdds,
	)
}

func seedPipeline(inChan chan<- int) {
	for i := 0; i < 10; i++ {
		inChan <- i
	}
}

func main() {
	plWrapper := NewPipelineWrapper()
	plWrapper.Run()
}
