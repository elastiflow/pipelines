package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

// datastream wraps an existing datastreams.DataStream[T] so it can be used as a Sourcer.
// NOTE: We keep the 'out' field for backward compatibility with existing tests/types,
// but the zero-copy Source implementation below no longer uses it.
type datastream[T any] struct {
	out     chan T
	inputDS datastreams.DataStream[T]
}

// FromDataStream wraps an existing DataStream as a Sourcer without an extra hop.
// This avoids an additional goroutine and channel send per element.
func FromDataStream[T any](ds datastreams.DataStream[T]) datastreams.Sourcer[T] {
	return &datastream[T]{inputDS: ds}
}

// Source returns a DataStream that directly reuses the upstream channel.
// No re-buffering or forwarding goroutine is created.
func (p *datastream[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	return datastreams.New[T](ctx, p.inputDS.Out(), errSender)
}
