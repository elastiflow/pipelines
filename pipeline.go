// Package pipelines provides a set of utilities for creating and managing
// concurrent data processing pipelines in Go.
//
// The package includes various functions to create, manipulate, and control
// the flow of data through channels, allowing for flexible and efficient
// data processing.
//
// The main components of this package are:
// - Pipeline: A struct that defines a generic stream process.
// - ProcessorFunc: A function type used by Pipeline to define the sequence of pipe.Pipe operations.
//
// Pipeline is the main entry point for using the package. It initializes
// and manages a sequence of Pipe. It provides methods to open
// the pipeline process, split a pipeline into two pipelines, and shutdown the
// flow of data in the Pipes gracefully.
//
// Example:
//
//	package main
//
//	import (
//		"log/slog"
//
//		"github.com/elastiflow/pipelines"
//		"github.com/elastiflow/pipelines/pipe"
//	)
//
//	func duplicateProcess(p pipe.Pipe[int]) pipe.Pipe[int] {
//		return p.Broadcast(
//			pipe.Params{Num: 2},
//		).FanIn() // Broadcasting by X then Fanning In will create X duplicates per T.
//	}
//
//	func main() {
//		inChan := make(chan int) // Setup channels and cleanup
//		errChan := make(chan error)
//		defer func() {
//			close(inChan)
//			close(errChan)
//		}()
//		pl := pipelines.New[int]( // Create a new Pipeline
//			inChan,
//			errChan,
//			duplicateProcess,
//		)
//		go func(errReceiver <-chan error) { // Handle Pipeline errors
//			defer pl.Close()
//			for err := range errReceiver {
//				if err != nil {
//					slog.Error("demo error: " + err.Error())
//					return
//				}
//			}
//		}(errChan)
//		for out := range pl.Open() { // Read Pipeline output
//			if out == 9 {
//				slog.Info("received simple pipeline output", slog.Int("out", out))
//				return
//			}
//			slog.Info("received simple pipeline output", slog.Int("out", out))
//		}
//	}

package pipelines

import (
	"context"

	"github.com/elastiflow/pipelines/pipe"
)

// ProcessFunc is a function type used by Pipeline to define the sequence of pipe.Pipe operations
type ProcessFunc[T any] func(pipe.Pipe[T, T]) pipe.Pipe[T, T]

// Pipeline is a struct that defines a generic stream process
type Pipeline[T any] struct {
	process    ProcessFunc[T]
	errorChan  chan<- error // Streams errors from the Pipeline to the caller
	inputChan  <-chan T     // Streams input data to the Pipeline for processing
	cancelFunc context.CancelFunc
}

// New constructs a new Pipeline of a given type by passing in the properties and process function
func New[T any](
	inChan <-chan T,
	errChan chan<- error,
	process ProcessFunc[T],
) *Pipeline[T] {
	return &Pipeline[T]{
		process:   process,
		inputChan: inChan,
		errorChan: errChan,
	}
}

// Open a Pipeline with a given set of parameters and return the output channel
func (p *Pipeline[T]) Open() <-chan T {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	return p.process(
		pipe.New[T, T](ctx, p.inputChan, p.errorChan),
	).Out()
}

// Tee a Pipeline with a given set of parameters and return two output channels with copied data
func (p *Pipeline[T]) Tee(params pipe.Params) (<-chan T, <-chan T) {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	out1, out2 := p.process(
		pipe.New[T, T](ctx, p.inputChan, p.errorChan),
	).Tee(params)
	return out1.Out(), out2.Out()
}

// Close a Pipeline and safely stop processing
func (p *Pipeline[T]) Close() {
	p.cancelFunc()
}
