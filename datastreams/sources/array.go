package sources

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

// array is a source that emits the provided values into the pipeline.
type array[T any] struct {
	out    chan T
	values []T
	params Params
}

// FromArray creates a source that emits the given values.
// Optional Params:
//   - BufferSize: outbound channel buffer size (defaults to 128)
//   - Throttle:   sleep interval after each send (0 = no throttle)
func FromArray[T any](values []T, params ...Params) datastreams.Sourcer[T] {
	var p Params
	if len(params) > 0 {
		p = params[0]
	}
	return &array[T]{
		values: values,
		params: p,
	}
}

// Source emits the configured values and respects context cancellation.
// IMPORTANT: We first do a non-blocking cancel pre-check, then perform a blocking send.
// This matches prior semantics and ensures that if ctx is already done we emit nothing.
func (s *array[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	size := s.params.BufferSize
	if size <= 0 {
		size = 128
	}
	s.out = make(chan T, size)
	ds := datastreams.New[T](ctx, s.out, errSender)

	go func(outSender chan<- T) {
		defer close(outSender)
		for _, val := range s.values {
			// Non-blocking cancel pre-check to avoid emitting when ctx is already done.
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Blocking send; if the receiver is slow, we backpressure here (as before).
			outSender <- val

			// Sleep throttle unconditionally (0 duration is a no-op).
			time.Sleep(s.params.Throttle)
		}
	}(s.out)

	return ds
}
