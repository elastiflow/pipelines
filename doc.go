// Package pipelines provides a set of utilities for creating and managing
// concurrent data processing pipelines in Go.
//
// The library uses channels under the hood to pass data between pipeline stages.
// Each stage runs in its own goroutine, ensuring concurrency and separation of concerns.
//
// Below is an example of an application utilizing pipelines for squaring an odd int
// and managing shared state counters:
//
//		package yourpipeline
//
//		import (
//		    "context"
//		    "fmt"
//		    "log/slog"
//		    "sync"
//
//		    "github.com/elastiflow/pipelines"
//		    "github.com/elastiflow/pipelines/datastreams"
//		    "github.com/elastiflow/pipelines/datastreams/sources"
//		)
//
//		// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
//		type PipelineWrapper struct {
//		    mu          sync.Mutex
//		    errChan     chan error
//		    evenCounter int
//		    oddCounter  int
//		}
//
//		// NewPipelineWrapper creates a new PipelineWrapper with counters set to 0
//		func NewPipelineWrapper() *PipelineWrapper {
//		    // Setup channels and return PipelineWrapper
//		    errChan := make(chan error, 10)
//		    return &PipelineWrapper{
//		        errChan:     errChan,
//		        evenCounter: 0,
//		        oddCounter:  0,
//		    }
//		}
//
//		// Run the PipelineWrapper
//		func (pl *PipelineWrapper) Run() {
//		    defer close(pl.errChan)
//
//		    pipeline := pipelines.New[int, int]( // Create a new Pipeline
//		        context.Background(),
//		        sources.FromArray(createIntArr(10)), // Create a source to start the pipeline
//		        pl.errChan,
//		    ).Start(pl.exampleProcess)
//
//		    go func(errReceiver <-chan error) { // Handle Pipeline errors
//		        defer pipeline.Close()
//		        for err := range errReceiver {
//		            if err != nil {
//		                slog.Error("demo error: " + err.Error())
//		                // return // if you wanted to close the pipeline during error handling.
//		            }
//		        }
//		    }(pl.errChan)
//
//		    for out := range pipeline.Out() { // Read Pipeline output
//		        slog.Info("received simple pipeline output", slog.Int("out", out))
//		    }
//		}
//
//		func (pl *PipelineWrapper) squareOdds(v int) (int, error) {
//		    if v%2 == 0 {
//		        pl.mu.Lock()
//		        pl.evenCounter++
//		        pl.mu.Unlock()
//		        return v, fmt.Errorf("even number error: %v", v)
//		    }
//		    pl.mu.Lock()
//		    pl.oddCounter++
//		    pl.mu.Unlock()
//		    return v * v, nil
//		}
//
//		func (pl *PipelineWrapper) exampleProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
//	     // datastreams.DataStream.OrDone will stop the pipeline if the input channel is closed
//		    return p.OrDone().FanOut(
//		        datastreams.Params{Num: 2}, // datastreams.DataStream.FanOut will run subsequent ds.Pipe stages in parallel
//		    ).Run(
//		        pl.squareOdds,  // datastreams.DataStream.Run will execute the ds.Pipe process: "squareOdds"
//		    ) // datastreams.DataStream.Out automatically FanIns to a single output channel if needed
//		}
//
//		func createIntArr(num int) []int {
//		    var arr []int
//		    for i := 0; i < num; i++ {
//		        arr = append(arr, i)
//		    }
//		    return arr
//		}
package pipelines
