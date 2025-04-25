package sinks

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

type channelSink[T any] struct {
	sender chan<- T
	params Params
}

// ToChannel creates a new channelSink
func ToChannel[T any](sender chan<- T, params ...Params) datastreams.Sinker[T] {
	var p Params
	for _, param := range params {
		p = param
	}
	return &channelSink[T]{
		sender: sender,
		params: p,
	}
}

// Sink reads the payload from the input datastreams.DataStream and sends it to the output channel
func (p *channelSink[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	outStream := ds.Out()
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-outStream:
			if !ok {
				return nil
			}
			p.sender <- v
		}
	}
}
