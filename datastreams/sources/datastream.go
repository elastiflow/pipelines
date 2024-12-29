package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

// Pipe is a consumer that reads a payload from an HTTP endpoint and returns it as a channel
type datastream[T any] struct {
	out     chan T
	inputDS datastreams.DataStream[T]
}

// FromDataStream creates a new Pipe Consumer
func FromDataStream[T any](ds datastreams.DataStream[T]) datastreams.Sourcer[T] {
	return &datastream[T]{
		out:     make(chan T, 128),
		inputDS: ds,
	}
}

// Source reads the payload from the HTTP endpoint and sends it to the output channel
func (p *datastream[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	ds := datastreams.New[T](ctx, p.out, errSender)
	go func(outSender chan<- T) {
		defer close(outSender)
		for in := range p.inputDS.Out() {
			select {
			case <-ctx.Done():
				return
			default:
				outSender <- in
			}

		}
	}(p.out)
	return ds
}
