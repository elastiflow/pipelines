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
//		"context"
//		"fmt"
//		"log/slog"
//		"sync"
//
//		"github.com/elastiflow/pipelines"
//		"github.com/elastiflow/pipelines/datastreams"
//		"github.com/elastiflow/pipelines/sources"
//	)
//
//	// PipelineWrapper is an example of a pipelines.Pipeline wrapper implementation. It includes shared state via counters.
//	type PipelineWrapper struct {
//		mu          sync.Mutex
//		errChan     chan error
//		evenCounter int
//		oddCounter  int
//	}
//
//	// NewPipelineWrapper creates a new PipelineWrapper with counters set to 0
//	func NewPipelineWrapper() *PipelineWrapper {
//		// Setup channels and return PipelineWrapper
//		errChan := make(chan error, 10)
//		return &PipelineWrapper{
//			errChan:     errChan,
//			evenCounter: 0,
//			oddCounter:  0,
//		}
//	}
//
//	// Run runs the PipelineWrapper
//	func (pl *PipelineWrapper) Run() {
//		defer close(pl.errChan)
//
//		pipeline_ := pipelines.FromSource[int, int]( // Create a new Pipeline
//			context.Background(),
//			sources.FromArray(createIntArr(10)), // Create a source to start the pipeline
//			pl.errChan,
//		).With(pl.exampleProcess)
//
//		go func(errReceiver <-chan error) { // Handle Pipeline errors
//			defer pipeline_.Close()
//			for err := range errReceiver {
//				if err != nil {
//					slog.Error("demo error: " + err.Error())
//					// return // if you wanted to close the pipeline during error handling.
//				}
//			}
//		}(pl.errChan)
//
//		for out := range pipeline_.Out() { // Read Pipeline output
//			slog.Info("received simple pipeline output", slog.Int("out", out))
//		}
//	}
//
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
//	func (pl *PipelineWrapper) exampleProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
//		return p.OrDone().FanOut( // datastreams.DataStream.OrDone will stop the pipeline if the input channel is closed
//			datastreams.Params{Num: 2}, // datastreams.DataStream.FanOut will run subsequent ds.Pipe stages in parallel
//		).Run( // datastreams.DataStream.Run will execute the ds.Pipe process: "squareOdds"
//			pl.squareOdds,
//		) // datastreams.DataStream.Out automatically FanIns to a single output channel if needed
//	}
package pipelines
