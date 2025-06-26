package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

type MessageMarker[T any] interface {
	MarkSuccess(msg T)
}

type ErrorMarker[T any] interface {
	MarkError(msg T, err error)
}

type MessageReader[T any] interface {
	Messages() <-chan T
}

type Runner interface {
	Run(ctx context.Context)
}

type EventConsumer[T any] interface {
	MessageMarker[T]
	MessageReader[T]
	ErrorMarker[T]
	Runner
}

type EventSourcer[T any] struct {
	consumer   EventConsumer[T]
	streamSize int
}

func NewEventSourcer[T any](
	streamSize int,
	eventConsumer EventConsumer[T],
) *EventSourcer[T] {
	return &EventSourcer[T]{
		streamSize: streamSize,
		consumer:   eventConsumer,
	}
}

func (k *EventSourcer[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	out := make(chan T, k.streamSize)
	go k.consumer.Run(ctx)
	go func(ctx context.Context, msgStream <-chan T, outStream chan<- T) {
		defer close(out)
		for msg := range k.consumer.Messages() {
			select {
			case <-ctx.Done():
				return
			default:
				out <- msg
			}
		}
	}(ctx, k.consumer.Messages(), out)
	return datastreams.New[T](ctx, out, errSender)
}

func (k *EventSourcer[T]) MarkSuccess(msg T) {
	k.consumer.MarkSuccess(msg)
}

func (k *EventSourcer[T]) MarkError(msg T, err error) {
	k.consumer.MarkError(msg, err)
}
