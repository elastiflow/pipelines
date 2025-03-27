package pipelines_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// PipelineWrapper is an example struct embedding a pipeline with shared counters.
//
// The pipeline will read from a channel source, process data, and sink into
// another channel. The "evenCounter" and "oddCounter" track how many evens
// or odds we've encountered.
type PipelineWrapper struct {
	mu          sync.Mutex
	errChan     chan error
	evenCounter int
	oddCounter  int
	pipeline    *pipelines.Pipeline[int, int]
}

// NewPipelineWrapper initializes a PipelineWrapper.
func NewPipelineWrapper() *PipelineWrapper {
	errChan := make(chan error, 10)
	return &PipelineWrapper{
		errChan:     errChan,
		evenCounter: 0,
		oddCounter:  0,
	}
}

// squareOdds increments counters, squares odd numbers, and returns an error on even numbers.
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

// exampleProcess shows the entire pipeline flow within a single method,
// including how to produce data (source) and consume it (sink).
func (pl *PipelineWrapper) exampleProcess(ctx context.Context) {
	// 1) Create channels for input and output.
	inChan := make(chan int, 10)
	outChan := make(chan int, 10)

	// 2) Build the pipeline: inChan -> (FanOut + squareOdds) -> sink -> outChan
	pl.pipeline = pipelines.New[int, int](
		ctx,
		sources.FromChannel(inChan), // source
		pl.errChan,
	).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
		return ds.OrDone().FanOut(
			datastreams.Params{Num: 2},
		).Run(
			pl.squareOdds,
		).Sink( // Sink
			sinks.ToChannel(outChan),
		)
	})

	var wg sync.WaitGroup
	wg.Add(10)

	// 3) Feed data into source inChan in a separate goroutine.
	go func() {
		for _, val := range createIntArr(10) {
			inChan <- val
		}
		close(inChan)
	}()

	// 4) Read results from sink outChan
	go func() {
		for out := range outChan {
			slog.Info("received simple pipeline output", slog.Int("out", out))
			wg.Done()
		}
	}()
	wg.Wait()
	pl.pipeline.Close()
}

// Run creates a background error handler, then calls exampleProcess.
func (pl *PipelineWrapper) Run() {
	defer close(pl.errChan)

	// Handle pipeline errors
	go func(errReceiver <-chan error) {
		for err := range errReceiver {
			if err != nil {
				slog.Error("pipeline error: " + err.Error())
			}
		}
	}(pl.errChan)

	// Run the pipeline flow
	pl.exampleProcess(context.Background())

	// Inspect counters after pipeline completes
	slog.Info("pipeline counters",
		slog.Int("evenCounter", pl.evenCounter),
		slog.Int("oddCounter", pl.oddCounter),
	)
}

// Example_sourceSink constructs and starts the Pipeline
func Example_sourceSink() {
	pl := NewPipelineWrapper()
	pl.Run()
}
