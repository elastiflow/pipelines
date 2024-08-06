// Package pipelines provides a set of utilities for creating and managing
// concurrent data processing pipelines in Go.
//
// The package includes various functions to create, manipulate, and control
// the flow of data through channels, allowing for flexible and efficient
// data processing.
//
// Below is an example of an application utilizing pipelines for squaring an odd int and managing shared state counters:
//
//	package yourpipeline
//
//	import (
//		"fmt"
//		"log/slog"
//		"sync"
//
//		"github.com/elastiflow/pipelines"
//		"github.com/elastiflow/pipelines/pipe"
//	)
//
//	// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
//	type PipelineWrapper struct {
//		mu          sync.Mutex
//		errChan     chan error
//		inChan      chan int
//		evenCounter int
//		oddCounter  int
//		pipeline    *pipelines.Pipeline[int]
//	}
//
//	// NewPipelineWrapper creates a new PipelineWrapper with counters set to 0
//	func NewPipelineWrapper() *PipelineWrapper {
//		// Setup channels and return PipelineWrapper
//		inChan := make(chan int)
//		errChan := make(chan error, 10)
//		return &PipelineWrapper{
//			errChan:     errChan,
//			inChan:      inChan,
//			evenCounter: 0,
//			oddCounter:  0,
//		}
//	}
//
//	// Run runs the PipelineWrapper
//	func (pl *PipelineWrapper) Run() {
//		defer func() {
//			close(pl.inChan)
//			close(pl.errChan)
//		}()
//		pl.pipeline = pipelines.New[int]( // Create a new Pipeline
//			pl.inChan,
//			pl.errChan,
//			pl.exampleProcess,
//		)
//		go func(errReceiver <-chan error) {	// Handle Pipeline errors
//			defer pl.pipeline.Close()
//			for err := range errReceiver {
//				if err != nil {
//					slog.Error("demo error: " + err.Error())
//					// return // if you wanted to close the pipeline during error handling.
//				}
//			}
//		}(pl.errChan)
//		var i int
//		for out := range pl.pipeline.Open() {	// Read Pipeline output
//			if i == 9 {
//				slog.Info("received simple pipeline output", slog.Int("out", out))
//				return
//			}
//			slog.Info("received simple pipeline output", slog.Int("out", out))
//			i++
//		}
//	}
//
//	// squareOdds is a method used in the PipelineWrapper.exProcess method
//	func (pl *PipelineWrapper) squareOdds(v int) (int, error) {
//		if v%2 == 0 {
//			pl.mu.Lock()
//			pl.evenCounter++
//			pl.mu.Unlock()
//			return v, fmt.Errorf("even number error: %v", v)
//		}
//		pl.mu.Lock()
//		pl.oddCounter++
//		pl.mu.Unlock()
//		return v * v, nil
//	}
//
//	// exampleProcess is the pipeline.Processor method used in this example
//	func (pl *PipelineWrapper) exampleProcess(p pipe.Pipe[int]) pipe.Pipe[int] {
//		return p.OrDone().FanOut(	// pipe.Pipe.OrDone will stop the pipeline if the input channel is closed
//			pipe.Params{Num: 2},	// pipe.Pipe.FanOut will run subsequent pipe.Pipe stages in parallel
//		).Run(						// pipe.Pipe.Run will execute the pipe.Pipe process: "squareOdds"
//			pl.squareOdds,
//		)							// pipe.Pipe.Out automatically FanIns to a single output channel if needed
//	}
package pipelines
