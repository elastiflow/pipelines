package sources

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

// array is a source that reads the values from an array and returns it as a channel
type array[T any] struct {
	out    chan T
	values []T
	params Params
}

// FromArray creates a new Pipe Consumer
func FromArray[T any](slice []T, params ...Params) datastreams.Sourcer[T] {
	var p Params
	for _, param := range params {
		p = param
	}

	return &array[T]{
		out:    make(chan T, len(slice)),
		values: slice,
		params: p,
	}
}

// Source reads the elements the values array and sends it to the output channel
func (s *array[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	ds := datastreams.New[T](ctx, s.out, errSender)
	go func(outSender chan<- T) {
		defer close(outSender)
		for _, val := range s.values {
			select {
			case <-ctx.Done():
				return
			default:
				outSender <- val
				time.Sleep(s.params.Throttle)
			}
		}
	}(s.out)
	return ds
}
