package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"time"
)

// timedInterval accumulates items and publishes them every `interval`.
type timedInterval[T any] struct {
	*partition.Base[T]
	interval time.Duration
}

// NewInterval starts publishing *immediately*. If no items
// arrive in a window, we publish an empty batch.
func NewInterval[T any](
	ctx context.Context,
	out pipes.Senders[[]T],
	errs chan<- error,
	interval time.Duration,
) partition.Partition[T] {
	w := &timedInterval[T]{
		Base:     partition.NewBase[T](ctx, out, errs),
		interval: interval,
	}
	// Start the background ticker right away
	go w.startInterval()
	return w
}

// startInterval continuously ticks every `w.interval`, publishes whatever
// items are in the buffer, and clears it.
func (t *timedInterval[T]) startInterval() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if t.Batch.Len() == 0 {
				continue
			}
			go t.Flush(t.Ctx, t.Batch.Next())
		case <-t.Ctx.Done():
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
) partition.Factory[T] {
	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partition.Partition[T] {
		return NewInterval[T](ctx, out, errs, interval)
	}
}
