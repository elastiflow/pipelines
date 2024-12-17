package sources

import (
	"context"

	"github.com/elastiflow/pipelines/errors"
)

// Array is a source that reads the values from an array and returns it as a channel
type Array[T any] struct {
	out    chan T
	values []T
}

// FromArray creates a new Pipe Consumer
func FromArray[T any](slice []T) *Array[T] {
	return &Array[T]{
		out:    make(chan T, len(slice)),
		values: slice,
	}
}

// Consume reads the elements the values array and sends it to the output channel
func (s *Array[T]) Consume(ctx context.Context, errs chan<- errors.Error) {
	defer close(s.out)
	for _, val := range s.values {
		select {
		case <-ctx.Done():
			return
		default:
			s.out <- val
		}

	}
}

// Out returns the output channel
func (p *Array[T]) Out() <-chan T {
	return p.out
}
