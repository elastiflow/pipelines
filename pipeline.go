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
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/sources"

	"github.com/elastiflow/pipelines/pipe"
)

type ProcessFunc[T any] func(stream pipe.DataStream[T]) pipe.DataStream[T]

type Source[T any] interface {
	Consume(ctx context.Context, errs chan<- errors.Error)
	Out() <-chan T
}

type Sink[T any] interface {
	Publish(context.Context, <-chan T, chan<- errors.Error)
}

type Opts struct {
	BufferSize int
}

// Pipeline is a struct that defines a generic stream process
type Pipeline[T any, U any] struct {
	ds         pipe.DataStream[T]
	ctx        context.Context
	cancelFunc context.CancelFunc
	errors     chan errors.Error
	source     Source[T]
}

// FromSource creates a new Pipeline from a Source
func FromSource[T any, U any](
	ctx context.Context,
	source Source[T],
	errChan chan errors.Error,
) *Pipeline[T, U] {
	go source.Consume(ctx, errChan)
	return &Pipeline[T, U]{
		ctx:    ctx,
		ds:     pipe.NewDataStream[T](ctx, source.Out(), errChan),
		errors: errChan,
		source: source,
	}
}

// New constructs a new Pipeline of a given type by passing in the properties and process function
func New[T any, U any](
	ctx context.Context,
	ds pipe.DataStream[T],
	errChan chan errors.Error,
) *Pipeline[T, U] {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &Pipeline[T, U]{
		cancelFunc: cancelFunc,
		ctx:        ctx,
		ds:         pipe.NewDataStream[T](ctx, ds.Out(), errChan),
		errors:     errChan,
	}
}

// Close a Pipeline and safely stop processing
func (p *Pipeline[T, U]) Close() {
	p.cancelFunc()
}

// Errors returns the error channel of the Pipeline
func (p *Pipeline[T, U]) Errors() <-chan errors.Error {
	return p.errors
}

// Map creates a new pipeline by applying a mapper function to each message in the stream
func (p *Pipeline[T, U]) Map(mapper pipe.Transformer[T, U], params ...pipe.Params) *Pipeline[U, U] {
	outPipe := pipe.Map[T, U](p.ds, mapper, params...)
	return New[U, U](p.ctx, outPipe, p.errors)
}

// Connect send the messages from the current pipeline to another pipeline
func (p *Pipeline[T, U]) Connect(process ProcessFunc[T]) *Pipeline[T, U] {
	return New[T, U](p.ctx, process(p.ds), p.errors)
}

// Out returns the output channel of the Pipeline
func (p *Pipeline[T, U]) Out() <-chan T {
	return p.ds.Out()
}

// Process creates a new pipeline by applying a processor function to each message in the stream
func (p *Pipeline[T, U]) Process(processor pipe.Processor[T], params ...pipe.Params) *Pipeline[T, U] {
	return New[T, U](p.ctx, p.ds.Run(processor, params...), p.errors)
}

// Sink is a blocking operation. It will block until the sink has consumed all the messages
func (p *Pipeline[T, U]) Sink(sink Sink[T]) {
	sink.Publish(p.ctx, p.ds.Out(), p.errors)
}

// Tee creates a fork in the stream, allowing for two separate streams to be created from the original
func (p *Pipeline[T, U]) Tee(params ...pipe.Params) (*Pipeline[T, U], *Pipeline[T, U]) {
	ds1, ds2 := p.ds.Tee(params...)
	return New[T, U](p.ctx, ds1, p.errors),
		New[T, U](p.ctx, ds2, p.errors)
}

// ToSource converts a Pipeline to a Source to use in other pipelines
func (p *Pipeline[T, U]) ToSource() Source[T] {
	return sources.FromDataStream[T](p.ds)
}

// Wait blocks until the stream has consumed all the messages
func (p *Pipeline[T, U]) Wait() {
	defer close(p.errors)
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.ds.OrDone().Out():
		}
	}
}
