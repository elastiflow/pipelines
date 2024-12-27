package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

type channelSource[T any] struct {
	receiver <-chan T
	params   Params
}

// FromChannel creates a new channelSource
func FromChannel[T any](rec <-chan T, params ...Params) datastreams.Sourcer[T] {
	var p Params
	for _, param := range params {
		p = param
	}
	return &channelSource[T]{
		receiver: rec,
		params:   p,
	}
}

// Source reads the payload from the HTTP endpoint and sends it to the output channel
func (p *channelSource[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	outChan := make(chan T, p.params.BufferSize)
	ds := datastreams.New[T](ctx, outChan, errSender)
	go func(outSender chan<- T) {
		defer close(outSender)
		for in := range p.receiver {
			select {
			case <-ctx.Done():
				return
			default:
				outSender <- in
			}

		}
	}(outChan)
	return ds
}
