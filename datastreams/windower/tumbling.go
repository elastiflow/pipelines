package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"time"
)

// tumbling buffers items for `windowDuration` once the first item arrives.
// After the timer fires, it publishes the batch and clears the buffer.
// Then it waits again for the *next* item to start a new window.
type tumbling[T any, R any] struct {
	*partition.Base[T, R] // We'll publish a slice of T as the batch
	windowDuration        time.Duration
	timerStarted          bool
	timer                 *time.Timer
	procFunc              func([]T) (R, error)
}

// NewTumbling constructs a new timed tumbling window that does not
// start the timer until the first item arrives.
func NewTumbling[T any, R any](
	ctx context.Context,
	procFunc func([]T) (R, error),
	errs chan<- error,
	windowDuration time.Duration,
) partition.Partition[T, R] {
	return &tumbling[T, R]{
		Base:           partition.NewBase[T, R](ctx, errs),
		windowDuration: windowDuration,
		timerStarted:   false,
		procFunc:       procFunc,
	}
}

// NewTimedTumblingFactory constructs a factory function that can be used
// to create new instances of tumbling. This is useful for
// creating multiple instances with the same processing function and
// window duration.
func NewTimedTumblingFactory[T any, R any](
	procFunc func([]T) (R, error),
	windowDuration time.Duration,
) partition.Factory[T, R] {
	return func(ctx context.Context, errs chan<- error) partition.Partition[T, R] {
		return NewTumbling[T, R](ctx, procFunc, errs, windowDuration)
	}
}

// Push receives each item. If the timer hasn’t started yet, we start it now.
// Otherwise, we just buffer the item.
func (t *tumbling[T, R]) Push(item T, _ time.Time) {
	t.Batch.Push(item)
	if !t.timerStarted {
		t.timerStarted = true
		go t.startTimer()
	}
}

func (t *tumbling[T, R]) startTimer() {
	timer := time.NewTimer(t.windowDuration)
	defer timer.Stop()
	for {
		select {
		case <-t.Ctx.Done():
		case <-timer.C:
			if t.Batch.Len() == 0 {
				continue
			}
			go partition.Flush[[]T, R](t.Ctx, t.Batch.Next(), t.Out, t.procFunc, t.Errs)
			t.timerStarted = false
		}
	}
}
