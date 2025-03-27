// Package pipelines provides a set of utilities for creating and managing
// concurrent data processing pipelines in Go.
//
// The package includes various functions to create, manipulate, and control
// the flow of data through channels, allowing for flexible and efficient
// data processing.
//
// The main components of this package are:
// - Pipeline: A struct that defines a generic connection of data streams.
// - DataStream (in subpackage "datastreams"): Provides the methods to build concurrency stages.
//
// Pipelines work by connecting a "Source" (an upstream data producer) with an
// optional chain of transformations or filters before optionally "Sinking" (sending
// the output to a consumer). Under the hood, all data flows through Go channels
// with concurrency managed by goroutines. Each transformation or filter is
// effectively run in parallel, communicating via channels.
//
// For more in-depth usage, see the examples below and the doc.go file.
package pipelines

import (
	"context"
	"sync"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// StreamFunc is a function that takes a datastreams.DataStream and returns a datastreams.DataStream.
//
// A StreamFunc represents a logical segment of a pipeline, where data can be
// consumed, transformed, filtered, or otherwise processed. The output of
// a StreamFunc is another DataStream that can be further chained.
type StreamFunc[T any, U any] func(stream datastreams.DataStream[T]) datastreams.DataStream[U]

// Opts defines optional configuration parameters for certain pipeline operations.
// Currently unused in this example, but reserved for future expansions.
type Opts struct {
	BufferSize int
}

// Pipeline is a struct that defines a generic stream process.
//
// Pipeline[T, U] represents a flow of data of type T at the input,
// ultimately producing data of type U at the output. Under the hood,
// a Pipeline orchestrates DataStream stages connected by channels.
//
// Usage typically begins by calling New(...) to create a pipeline with a Source,
// and then applying transformations via Map, Process, or Start/Stream with a StreamFunc.
// Finally, you can read from the Out() channel, or Sink the output via a Sinker.
type Pipeline[T any, U any] struct {
	sourceDS   datastreams.DataStream[T]
	sinkDS     datastreams.DataStream[U]
	ctx        context.Context
	cancelFunc context.CancelFunc
	errorChan  chan error
	wg         *sync.WaitGroup
}

// New constructs a new Pipeline of a given type by passing in a datastreams.Sourcer.
//
//   - ctx: a context.Context used to cancel or manage the lifetime of the pipeline
//   - sourcer: a datastreams.Sourcer[T] that provides the initial data stream
//   - errStream: an error channel to which the pipeline can send errors
//
// Example usage:
//
//	p := pipelines.New[int, int](context.Background(), someSource, errChan)
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
		wg:         &sync.WaitGroup{},
	}
}

// Close gracefully closes a Pipeline by canceling its internal context.
// This signals all data streams to terminate reading or writing to channels.
func (p *Pipeline[T, U]) Close() {
	p.cancelFunc()
}

// Errors returns the error channel of the Pipeline.
//
// This channel receives errors from any stage (e.g. transformations, filters, sinks).
// The caller should consume from it to handle errors appropriately.
func (p *Pipeline[T, U]) Errors() <-chan error {
	return p.errorChan
}

// Map creates a new DataStream by applying a mapper function (TransformFunc) to each
// message in the pipeline's source. This is a convenience method that directly calls
// datastreams.Map on the pipeline's source.
//
//   - mapper: a TransformFunc[T, U] that takes an input T and returns output U
//   - params: optional datastreams.Params to configure buffer sizes, skipping errors, etc.
func (p *Pipeline[T, U]) Map(
	mapper datastreams.TransformFunc[T, U],
	params ...datastreams.Params,
) datastreams.DataStream[U] {
	return datastreams.Map[T, U](p.sourceDS, mapper, params...)
}

// Stream applies a StreamFunc to the pipeline's source, storing the resulting
// DataStream as the pipeline's sink (final stage). It then returns that sink
// DataStream for further chaining.
//
// Typically used for quickly connecting a pipeline to a processing function.
//
// Example:
//
//	pipeline.Stream(func(ds DataStream[int]) DataStream[int] { ... })
func (p *Pipeline[T, U]) Stream(streamFunc StreamFunc[T, U]) datastreams.DataStream[U] {
	p.sinkDS = streamFunc(p.sourceDS.WithWaitGroup(p.wg))
	return p.sinkDS
}

// Start applies the given StreamFunc to the pipeline's source and returns
// the Pipeline itself. This is useful for chaining multiple calls on the pipeline
// while still returning the Pipeline.
//
// Example:
//
//	pipeline.Start(func(ds DataStream[int]) DataStream[int] { ... }).Start(...)
func (p *Pipeline[T, U]) Start(streamFunc StreamFunc[T, U]) *Pipeline[T, U] {
	p.sinkDS = streamFunc(p.sourceDS.WithWaitGroup(p.wg))
	return p
}

// Sink consumes the pipeline's sink DataStream using a specified datastreams.Sinker.
//
//   - sinker: a Sinker that will receive data of type U
//   - returns an error if the sink fails; otherwise nil.
//
// Typically used to push output to a custom location or channel.
func (p *Pipeline[T, U]) Sink(sinker datastreams.Sinker[U]) error {
	return sinker.Sink(p.ctx, p.sinkDS)
}

// In returns the input channel of the source datastreams.DataStream of a Pipeline.
//
// This channel can be read from externally if needed, though typically one
// supplies a Sourcer when constructing a Pipeline. Use with care if manually
// sending data into the pipeline.
func (p *Pipeline[T, U]) In() <-chan T {
	return p.sourceDS.Out()
}

// Out returns the output channel (sink) of the pipeline.
//
// Reading from this channel lets you consume the final processed data of type U.
func (p *Pipeline[T, U]) Out() <-chan U {
	return p.sinkDS.Out()
}

// Process creates a new pipeline by applying a ProcessFunc to each message
// in the current pipeline's source. It internally starts a new pipeline with
// the processed DataStream. This is a convenience for quickly chaining transformations.
//
//   - processor: a ProcessFunc[T] that may transform T in place
//   - params: optional datastreams.Params
func (p *Pipeline[T, U]) Process(
	processor datastreams.ProcessFunc[T],
	params ...datastreams.Params,
) *Pipeline[T, U] {
	newSource := p.sourceDS.WithWaitGroup(p.wg).Run(processor, params...)
	return &Pipeline[T, U]{
		sourceDS:   newSource,
		ctx:        p.ctx,
		cancelFunc: p.cancelFunc,
		errorChan:  p.errorChan,
		wg:         p.wg,
	}
}

// Tee creates a fork in the pipeline's sink DataStream, returning two DataStreams
// that each receive the same data from the sink.
//
// This is useful for sending the same processed output to multiple consumers.
//
//   - params: optional datastreams.Params for buffer sizing, etc.
//   - returns two DataStreams of type U, each receiving the same data from the pipeline sink.
func (p *Pipeline[T, U]) Tee(params ...datastreams.Params) (datastreams.DataStream[U], datastreams.DataStream[U]) {
	// We attach the WaitGroup so the Tee goroutine gets tracked.
	return p.sinkDS.WithWaitGroup(p.wg).Tee(params...)
}

// ToSource converts the pipeline's sink into a datastreams.Sourcer[U], allowing it to be used
// as a source in another pipeline.
func (p *Pipeline[T, U]) ToSource() datastreams.Sourcer[U] {
	return sources.FromDataStream[U](p.sinkDS)
}

// Wait blocks until the pipeline's source has consumed all messages or the context
// is canceled.
//
// This method helps ensure you process all items before shutting down. Once Wait
// returns, the Pipeline is effectively drained.
func (p *Pipeline[T, U]) Wait() {
	p.wg.Wait() // Blocks until all goroutines managed by p.wg have returned
	close(p.errorChan)
}
