package main

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"log/slog"
	"sync"

	"github.com/elastiflow/pipelines"
)

type IntConsumer struct {
	num int
	out chan int
}

func (c *IntConsumer) Consume(ctx context.Context, errs chan<- errors.Error) {
	defer close(c.out)
	for i := 0; i < c.num; i++ {
		c.out <- i
	}
}

func (c *IntConsumer) Out() <-chan int {
	return c.out
}

// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
type PipelineWrapper struct {
	mu          sync.Mutex
	errChan     chan error
	inChan      chan int
	evenCounter int
	oddCounter  int
	pipeline    *pipelines.Pipeline[int, int]
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
	pl.pipeline = pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		&IntConsumer{num: 10, out: make(chan int, 10)},
		make(chan errors.Error, 10),
	).Connect(pl.exampleProcess)
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
func (pl *PipelineWrapper) exampleProcess(p pipe.DataStream[int]) pipe.DataStream[int] {
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
