package sources

import (
	"context"

	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
)

// Pipe is a consumer that reads a payload from an HTTP endpoint and returns it as a channel
type Pipe[T any] struct {
	out chan T
	ds  pipe.DataStream[T]
}

// FromDataStream creates a new Pipe Consumer
func FromDataStream[T any](ds pipe.DataStream[T]) *Pipe[T] {
	return &Pipe[T]{
		out: make(chan T, 128),
		ds:  ds,
	}
}

// Consume reads the payload from the HTTP endpoint and sends it to the output channel
func (p *Pipe[T]) Consume(ctx context.Context, errs chan<- errors.Error) {
	defer close(p.out)
	for in := range p.ds.Out() {
		select {
		case <-ctx.Done():
			return
		default:
			p.out <- in
		}

	}
}

// Out returns the output channel
func (p *Pipe[T]) Out() <-chan T {
	return p.out
}
