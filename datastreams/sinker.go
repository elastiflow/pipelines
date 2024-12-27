package datastreams

import "context"

// Sinker is an interface that defines the Sink method
type Sinker[T any] interface {
	Sink(ctx context.Context, ds DataStream[T]) error
}
