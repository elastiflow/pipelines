package windower

import (
	"context"
	"errors"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/elastiflow/pipelines/datastreams/partitioner"
)

// timedInterval accumulates items and publishes them every `interval`.
type timedInterval[T any] struct {
	*partitioner.Base[T]
	interval time.Duration
}

// newInterval starts publishing *immediately*. If no items
// arrive in a window, we publish an empty batch.
func newInterval[T any](
	ctx context.Context,
	out pipes.Senders[[]T],
	errs chan<- error,
	interval time.Duration,
) partitioner.Partition[T] {
	w := &timedInterval[T]{
		Base:     partitioner.NewBase[T](out, errs),
		interval: interval,
	}
	// Start the background ticker right away
	go w.startInterval(ctx)
	return w
}

// startInterval continuously ticks every `w.interval`, publishes whatever
// items are in the buffer, and clears it.
func (t *timedInterval[T]) startInterval(ctx context.Context) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go t.FlushNext(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// NewIntervalFactory constructs a factory function that can be used
// to create new instances of timedInterval. This is useful for
// creating multiple instances with the same processing function and
// interval duration.
func NewIntervalFactory[T any](
	interval time.Duration,
) (partitioner.Factory[T], error) {
	if interval <= 0 {
		return nil, errors.New("interval must be greater than 0")
	}
	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partitioner.Partition[T] {
		return newInterval[T](ctx, out, errs, interval)
	}, nil
}
