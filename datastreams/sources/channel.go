package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

const defaultChannelBufferSize = 128

type channelSource[T any] struct {
	receiver <-chan T
	params   Params
}

// FromChannel creates a new channel-based source.
// It forwards values from the provided receiver to the pipeline,
// closing the outbound when the source exits.
func FromChannel[T any](rec <-chan T, params ...Params) datastreams.Sourcer[T] {
	var p Params
	if len(params) > 0 {
		p = params[0]
	}
	return &channelSource[T]{
		receiver: rec,
		params:   p,
	}
}

// Source starts a forwarding goroutine that:
//   - ranges the input channel OR exits promptly if ctx is canceled,
//   - sends each value to the pipeline using a two-case select (no default),
//   - closes the outbound channel on return.
//
// We keep this small forwarding goroutine to preserve cancellation semantics
// (ctx cancellation will close the pipeline’s outbound even if the upstream
// channel remains open).
func (p *channelSource[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	size := p.params.BufferSize
	if size <= 0 {
		size = defaultChannelBufferSize
	}
	outChan := make(chan T, size)

	ds := datastreams.New[T](ctx, outChan, errSender)

	go func(outSender chan<- T) {
		defer close(outSender)
		for {
			select {
			case <-ctx.Done():
				// Stop promptly on cancellation.
				return
			case in, ok := <-p.receiver:
				if !ok {
					// Upstream closed; we are done.
					return
				}
				// Two-case select: perform the (potentially blocking) send,
				// or abort if the context is canceled concurrently.
				select {
				case outSender <- in:
				case <-ctx.Done():
					return
				}
			}
		}
	}(outChan)

	return ds
}
