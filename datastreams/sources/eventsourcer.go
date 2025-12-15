package sources

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

// MessageMarker defines an interface for acknowledging a message as successfully processed.
// This is often used to commit an offset in a message queue.
type MessageMarker[T any] interface {
	MarkSuccess(msg T)
}

// ErrorMarker defines an interface for marking that an error occurred while processing a message.
type ErrorMarker[T any] interface {
	MarkError(msg T, err error)
}

// MessageReader defines an interface for components that provide a read-only channel of messages.
type MessageReader[T any] interface {
	Messages() <-chan T
}

// Runner defines an interface for components that have a long-running process
// that can be started and managed with a context.
type Runner interface {
	Run(ctx context.Context)
}

// EventConsumer is a composite interface that represents a complete message source,
// such as a consumer for a message queue like Kafka. It encapsulates the logic for
// running the consumer, providing messages, and acknowledging their processing status.
type EventConsumer[T any] interface {
	MessageMarker[T]
	MessageReader[T]
	ErrorMarker[T]
	Runner
}

// EventSourcer acts as an adapter to bridge an EventConsumer with a datastreams pipeline.
// It implements the datastreams.Source interface, allowing any EventConsumer to be
// the starting point of a pipeline.
type EventSourcer[T any] struct {
	consumer   EventConsumer[T]
	streamSize int
}

// NewEventSourcer creates a new EventSourcer that wraps the given EventConsumer.
// The streamSize parameter defines the buffer size for the output datastream.
func NewEventSourcer[T any](
	streamSize int,
	eventConsumer EventConsumer[T],
) *EventSourcer[T] {
	return &EventSourcer[T]{
		streamSize: streamSize,
		consumer:   eventConsumer,
	}
}

// Source starts the underlying EventConsumer and begins streaming its messages
// into a new datastreams.DataStream. This method launches goroutines to run the
// consumer and pipe its data into the pipeline.
func (k *EventSourcer[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	out := make(chan T, k.streamSize)

	// Start the consumer's main run loop in the background.
	go k.consumer.Run(ctx)

	// Start a background goroutine to pipe messages from the consumer to the output stream.
	go func(ctx context.Context, msgStream <-chan T, outStream chan<- T) {
		defer close(out)
		for msg := range k.consumer.Messages() {
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}(ctx, k.consumer.Messages(), out)

	return datastreams.New[T](ctx, out, errSender)
}

// MarkSuccess delegates the acknowledgment of a successfully processed message
// back to the underlying EventConsumer.
func (k *EventSourcer[T]) MarkSuccess(msg T) {
	k.consumer.MarkSuccess(msg)
}

// MarkError delegates the marking of a failed message and its associated error
// back to the underlying EventConsumer.
func (k *EventSourcer[T]) MarkError(msg T, err error) {
	k.consumer.MarkError(msg, err)
}
