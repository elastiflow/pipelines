package sinks

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

type EventProducer[T any] interface {
	Produce(msg T)
}

type eventProducerSinker[T any] struct {
	producer EventProducer[T]
	params   Params
}

// ToEventProducer creates a new EventProducerSinker
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

// Sink reads the payload from the input datastreams.DataStream and sends it to the output channel
func (p *eventProducerSinker[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	outStream := ds.Out()
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-outStream:
			if !ok {
				return nil
			}
			p.producer.Produce(v)
		}
	}
}
