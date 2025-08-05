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
	"fmt"
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
// Pipeline[T, U] represents a flow of data type T at the input,
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

// Errors method returns the error channel of the Pipeline.
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

// Expand creates a new DataStream by applying an expander function (ExpandFunc) to each
// message in the pipeline's source. This is a convenience method that directly calls
// datastreams.Expand on the pipeline's source.
//
//   - mapper: an Expand[T, U] that takes an input T and returns an array of output U
//   - params: optional datastreams.Params to configure buffer sizes, skipping errors, etc.
func (p *Pipeline[T, U]) Expand(
	expander datastreams.ExpandFunc[T, U],
	params ...datastreams.Params,
) datastreams.DataStream[U] {
	return datastreams.Expand[T, U](p.sourceDS, expander, params...)
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
//
// Parameters:
//   - streamFunc: A function that takes a data stream and returns a new one.
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
//
// Parameters:
//   - streamFunc: A function that takes a data stream and returns a new one.
func (p *Pipeline[T, U]) Start(streamFunc StreamFunc[T, U]) *Pipeline[T, U] {
	p.sinkDS = streamFunc(p.sourceDS.WithWaitGroup(p.wg))
	return p
}

// Sink consumes the pipeline's sink DataStream using a specified datastreams.Sinker.
// Typically used to push output to a custom location or channel.
//
// Parameters:
//   - sinker: A Sinker that will receive data of type U.
//
// Returns:
//   - An error if the sink fails; otherwise nil.
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

// Process adds a new processing stage to the pipeline, applying a function
// to each item. The processing function must take and return an item of the
// same type, making this method ideal for in-place transformations like
// data enrichment or filtering.
//
// This method returns a new Pipeline instance that incorporates the new
// processing stage, allowing multiple Process calls to be fluently chained
// together. The final output type of the pipeline (U) remains unchanged.
//
// The provided 'processor' function is of type datastreams.ProcessFunc[T],
// with the signature 'func(T) (T, error)'. If the function returns a non-nil
// error, processing for that item halts, and the error is sent to the
// pipeline's error channel.
//
// Example:
//
//	// Assume a pipeline processes User objects.
//	type User struct {
//		ID        int
//		Name      string
//		IsValid   bool
//		IsAudited bool
//	}
//
//	// p is an existing pipeline, e.g., p := pipelines.New[User, User](...)
//
//	// We can chain multiple Process calls to create a multi-stage workflow.
//	finalPipeline := p.Process(func(u User) (User, error) {
//		// Stage 1: Validate the user.
//		if u.Name != "" {
//			u.IsValid = true
//		}
//		return u, nil
//	}).Process(func(u User) (User, error) {
//		// Stage 2: Audit the user.
//		if u.IsValid {
//			u.IsAudited = true
//			fmt.Printf("Audited user %d\n", u.ID)
//		}
//		return u, nil
//	})
//
//	// The finalPipeline now contains both processing stages.
//	// You can then consume the results from finalPipeline.Out().
//
// Parameters:
//   - processor: a datastreams.ProcessFunc[T] to apply to each item.
//   - params: optional datastreams.Params to configure the processing stage,
//     i.e., for setting concurrency.
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
// This is useful when you want to take the output of one pipeline and use it as the input
// for another pipeline.
func (p *Pipeline[T, U]) ToSource() datastreams.Sourcer[U] {
	return sources.FromDataStream[U](p.sinkDS)
}

// Copy creates multiple copies of the current pipeline's source DataStream,
// allowing the same data to be processed in parallel across multiple pipelines.
//
// Parameters:
//   - num: the number of copies to create.
func (p *Pipeline[T, U]) Copy(num int) Pipelines[T, U] {
	next := p.sourceDS.Broadcast(datastreams.Params{
		Num: num,
	})
	out := make([]*Pipeline[T, U], num)
	for i := 0; i < num; i++ {
		out[i] = New[T, U](
			p.ctx,
			sources.FromDataStream[T](next.Listen(i)),
			p.errorChan,
		)
	}

	return out
}

// Broadcast creates multiple copies of the current pipeline's source DataStream,
// allowing the same data to be processed in parallel across multiple pipelines
// and allows for different sinks or processing logic to be applied to each copy.
//
// Parameters:
//   - num: the number of copies to create.
//   - streamFunc: a StreamFunc[T, U] that will be applied to each copy of the source DataStream.
func (p *Pipeline[T, U]) Broadcast(num int, streamFunc StreamFunc[T, U]) Pipelines[T, U] {
	pls := p.Copy(num)
	pls.Start(streamFunc)
	return pls
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

// Pipelines represents a collection of Pipeline instances, designed to simplify
// the management of concurrent, parallel data processing workflows. It is
// typically created when a single data source is split into multiple streams
// using methods like Broadcast.
//
// By grouping related pipelines, this type allows for collective operations
// such as starting, stopping, or waiting for all of them with a single method call.
// This is particularly useful for scaling up processing by distributing work
// across multiple concurrent workers that share the same processing logic.
//
// Example usage:
//
//	pipeline := mainPipeline.Broadcast(3)
//	pipelines.Start(myProcessingFunc)
//	pipelines.Wait()
type Pipelines[T any, U any] []*Pipeline[T, U]

// Count returns the number of pipelines in the collection.
func (p Pipelines[T, U]) Count() int {
	return len(p)
}

// Get returns the pipeline at the specified index. It returns an error if the
// index is out of bounds.
func (p Pipelines[T, U]) Get(index int) (*Pipeline[T, U], error) {
	if index < 0 || index >= p.Count() {
		return nil, fmt.Errorf("pipelines: index out of range [%d] with pipelines length %d", index, p.Count())
	}
	return p[index], nil
}

// Start applies a StreamFunc to each pipeline in the collection, initiating
// the data processing flow. It is typically used after a Broadcast to run the
// same logic in parallel across multiple pipelines.
//
// The method returns the collection itself to allow for method chaining.
//
// Example:
//
//	// p is a single pipeline from which we broadcast
//	workers := p.Broadcast(3) // Create 3 worker pipelines
//
//	// Define the processing logic for each worker
//	processingFunc := func(stream datastreams.DataStream[int]) datastreams.DataStream[int] {
//		// For example, map values to multiply by 2
//		return datastreams.Map(stream, func(i int) int {
//			return i * 2
//		})
//	}
//
//	// Start all workers with the defined logic
//	workers.Start(processingFunc)
//
//	// Now, results can be gathered from each pipeline in the 'workers' collection.
func (p Pipelines[T, U]) Start(streamFunc StreamFunc[T, U]) Pipelines[T, U] {
	for _, pipeline := range p {
		if pipeline != nil {
			pipeline.Start(streamFunc)
		}
	}
	return p
}

// Process applies a processing function to each pipeline in the collection,
// adding a new processing stage to each one. This is a convenient way to apply
// the same transformation logic to multiple parallel streams, such as those
// created by Broadcast.
//
// Each pipeline in the collection is replaced by a new pipeline that includes
// the additional processing step. The method returns the modified collection
// to allow for method chaining.
//
// Example:
//
//	// p is a single pipeline from which we broadcast.
//	workers := p.Broadcast(2) // Create 2 worker pipelines.
//
//	// Define a processor that doubles an integer.
//	double := func(i int) (int, error) {
//		return i * 2, nil
//	}
//
//	// Apply the processor to all worker pipelines.
//	workers.Process(double)
//
//	// The pipelines in the 'workers' collection now each have an additional
//	// stage that doubles the numbers passing through them.
//
// Parameters:
//   - processor: a datastreams.ProcessFunc[T] to apply to each item.
//   - params: optional datastreams.Params to configure the processing stage,
//     i.e., for setting concurrency.
func (p Pipelines[T, U]) Process(
	processor datastreams.ProcessFunc[T],
	params ...datastreams.Params,
) Pipelines[T, U] {
	for i, pipeline := range p {
		if pipeline != nil {
			p[i] = pipeline.Process(processor, params...)
		}
	}
	return p
}

// Wait blocks until all pipelines in the collection have finished processing.
// This is useful for ensuring that all data has been consumed before the
// application exits.
func (p Pipelines[T, U]) Wait() {
	for _, pipeline := range p {
		if pipeline != nil {
			pipeline.wg.Wait()
		}
	}
}

// Close gracefully shuts down all pipelines in the collection by canceling their
// underlying contexts.
func (p Pipelines[T, U]) Close() {
	for _, pipeline := range p {
		if pipeline != nil {
			pipeline.Close()
		}
	}
}
