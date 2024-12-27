// Package pipelines provides a set of utilities for creating and managing
// concurrent data processing pipelines in Go.
//
// The package includes various functions to create, manipulate, and control
// the flow of data through channels, allowing for flexible and efficient
// data processing.
//
// The main components of this package are:
// - Pipeline: A struct that defines a generic connection of data streams.
// - ds: A segment of a given pipeline.
//
// Pipeline is the main entry point for using the package. It initializes
// and manages a sequence of DataStreams. It provides methods to manage
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
//	func duplicateProcess(ds pipe.ds[int]) pipe.ds[int] {
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
//		pl := pipelines.New[int, int]( // Create a new Pipeline
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

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// StreamFunc is a function that takes a datastreams.DataStream and returns a datastreams.DataStream
type StreamFunc[T any, U any] func(stream datastreams.DataStream[T]) datastreams.DataStream[U]

type Opts struct {
	BufferSize int
}

// Pipeline is a struct that defines a generic stream process
type Pipeline[T any, U any] struct {
	sourceDS   datastreams.DataStream[T]
	sinkDS     datastreams.DataStream[U]
	ctx        context.Context
	cancelFunc context.CancelFunc
	errorChan  chan error
}

// New constructs a new Pipeline of a given type by passing in a datastreams.Sourcer
func New[T any, U any](
	ctx context.Context,
	sourcer datastreams.Sourcer[T],
	errStream chan error,
) *Pipeline[T, U] {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &Pipeline[T, U]{
		cancelFunc: cancelFunc,
		ctx:        ctx,
		sourceDS:   sourcer.Source(ctx, errStream),
		errorChan:  errStream,
	}
}

// Close a Pipeline and safely stop processing
func (p *Pipeline[T, U]) Close() {
	p.cancelFunc()
}

// Errors returns the error channel of the Pipeline
func (p *Pipeline[T, U]) Errors() <-chan error {
	return p.errorChan
}

// Map creates a new pipeline by applying a mapper function to each message in the stream
func (p *Pipeline[T, U]) Map(
	mapper datastreams.TransformFunc[T, U],
	params ...datastreams.Params,
) datastreams.DataStream[U] {
	return datastreams.Map[T, U](p.sourceDS, mapper, params...)
}

// Stream inputs a streamFunc, starts the Pipeline, and returns the sink datastreams.DataStream
func (p *Pipeline[T, U]) Stream(streamFunc StreamFunc[T, U]) datastreams.DataStream[U] {
	p.sinkDS = streamFunc(p.sourceDS)
	return p.sinkDS
}

// Start inputs a streamFunc, starts the Pipeline, and returns the Pipeline
func (p *Pipeline[T, U]) Start(streamFunc StreamFunc[T, U]) *Pipeline[T, U] {
	p.sinkDS = streamFunc(p.sourceDS)
	return p
}

// Sink inputs a Sinker and returns the error channel
func (p *Pipeline[T, U]) Sink(sinker datastreams.Sinker[U]) error {
	return sinker.Sink(p.ctx, p.sinkDS)
}

// In returns the input channel of the source datastreams.DataStream of a Pipeline
func (p *Pipeline[T, U]) In() <-chan T {
	return p.sourceDS.Out()
}

// Out returns the output channel of the source datastreams.DataStream of a Pipeline
func (p *Pipeline[T, U]) Out() <-chan U {
	return p.sinkDS.Out()
}

// Process creates a new pipeline by applying a processor function to each message in the stream
func (p *Pipeline[T, U]) Process(processor datastreams.ProcessFunc[T], params ...datastreams.Params) *Pipeline[T, U] {
	return New[T, U](p.ctx, sources.FromDataStream(p.sourceDS.Run(processor, params...)), p.errorChan)
}

// Tee creates a fork in the datastreams.DataStream, allowing for two separate streams to be created from the original
func (p *Pipeline[T, U]) Tee(params ...datastreams.Params) (datastreams.DataStream[U], datastreams.DataStream[U]) {
	return p.sinkDS.Tee(params...)
}

// ToSource converts a Pipeline to a Source to use in another Pipeline
func (p *Pipeline[T, U]) ToSource() datastreams.Sourcer[U] {
	return sources.FromDataStream[U](p.sinkDS)
}

// Wait blocks until the stream has consumed all the messages
func (p *Pipeline[T, U]) Wait() {
	defer close(p.errorChan)
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.sourceDS.OrDone().Out():
		}
	}
}
