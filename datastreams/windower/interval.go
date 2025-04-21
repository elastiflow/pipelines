package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"time"
)

// timedInterval accumulates items and publishes them every `interval`.
type timedInterval[T any, R any] struct {
	*partition.Base[T, R]
	interval time.Duration
	procFunc func([]T) (R, error)
}

// NewInterval starts publishing *immediately*. If no items
// arrive in a window, we publish an empty batch.
func NewInterval[T any, R any](
	ctx context.Context,
	outChannels pipes.Senders[R],
	procFunc func([]T) (R, error),
	errs chan<- error,
	interval time.Duration,
) partition.Partition[T, R] {
	w := &timedInterval[T, R]{
		Base:     partition.NewBase[T, R](ctx, outChannels, errs),
		interval: interval,
		procFunc: procFunc,
	}
	// Start the background ticker right away
	go w.startInterval()
	return w
}

// startInterval continuously ticks every `w.interval`, publishes whatever
// items are in the buffer, and clears it.
func (t *timedInterval[T, R]) startInterval() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if t.Batch.Len() == 0 {
				continue
			}
			go t.Flush(t.Ctx, t.Batch.Next(), t.procFunc, t.Errs)
		case <-t.Ctx.Done():
			return
		}
	}
}

// NewIntervalFactory constructs a factory function that can be used
// to create new instances of timedInterval. This is useful for
// creating multiple instances with the same processing function and
// interval duration.
func NewIntervalFactory[T any, R any](
	procFunc func([]T) (R, error),
	interval time.Duration,
) partition.Factory[T, R] {
	return func(
		ctx context.Context,
		out pipes.Senders[R],
		errs chan<- error,
	) partition.Partition[T, R] {
		return NewInterval[T, R](ctx, out, procFunc, errs, interval)
	}
}
