package sinks

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

// EventProducer defines a generic interface for any component that produces or sends
// a message of type T. This provides a simple abstraction for outputs like message
// queue clients or other event stream publishers.
type EventProducer[T any] interface {
	// Produce sends a single message to the underlying destination.
	Produce(msg T)
}

// eventProducerSinker is the internal implementation of a Sinker that wraps an EventProducer.
type eventProducerSinker[T any] struct {
	producer EventProducer[T]
	params   Params
}

// ToEventProducer creates a datastreams.Sinker that wraps a generic EventProducer.
// This serves as an adapter, allowing any component that implements the EventProducer
// interface to be used as a final sink in a datastreams pipeline.
//
// The optional `params` argument can be used for configuration; if multiple are
// provided, only the last one in the list will be applied.
func ToEventProducer[T any](producer EventProducer[T], params ...Params) datastreams.Sinker[T] {
	var p Params
	for _, param := range params {
		p = param
	}
	return &eventProducerSinker[T]{
		producer: producer,
		params:   p,
	}
}

// Sink continuously reads items from the input DataStream and sends each one
// to the wrapped EventProducer by calling its Produce method. The sink will
// run until the context is canceled or the input stream is closed.
func (p *eventProducerSinker[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	outStream := ds.Out()
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-outStream:
			if !ok {
				// Input channel is closed, pipeline is finished.
				return nil
			}
			p.producer.Produce(v)
		}
	}
}
