package joiner

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"time"
)

type joiner[T any, R any] struct {
	*partition.Base[T, R] // We'll publish a slice of T as the batch
	procFunc              func(T) (R, error)
}

// NewJoiner constructs a new timed tumbling window that does not
// start the timer until the first item arrives.
func NewJoiner[T any, R any](
	ctx context.Context,
	errs chan<- error,
	procFunc func(T) (R, error),
) partition.Partition[T, R] {
	return &joiner[T, R]{
		Base:     partition.NewBase[T, R](ctx, errs),
		procFunc: procFunc,
	}
}

// NewJoinerFactory constructs a factory function that can be used
// to create new instances of joiner. This is useful for
// creating multiple instances with the same processing function and
// window duration.
func NewJoinerFactory[T any, R any](
	procFunc func(T) (R, error),
) partition.Factory[T, R] {
	return func(ctx context.Context, errs chan<- error) partition.Partition[T, R] {
		return NewJoiner[T, R](ctx, errs, procFunc)
	}
}

func (j joiner[T, R]) Push(item T, eventTime time.Time) {
	go func() {
		// Process the item and send it to the output channel
		result, err := j.procFunc(item)
		if err != nil {
			j.Errs <- err
			return
		}
		j.Out <- result
	}()
}
