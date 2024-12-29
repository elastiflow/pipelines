package datastreams

import "context"

// Sourcer is an interface that defines the Source method
type Sourcer[T any] interface {
	Source(ctx context.Context, errSender chan<- error) DataStream[T]
}
