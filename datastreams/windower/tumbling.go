package windower

import (
	"context"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
)

// tumbling buffers items for windowDuration once the first item arrives.
// After windowDuration, it publishes the batch and clears it, then waits
// for the next item to start a new timer.
type tumbling[T any, R any] struct {
	*partition.Base[T, R]

	windowDuration time.Duration
	procFunc       func([]T) (R, error)

	mu           sync.Mutex
	timerStarted bool
}

// newTumbling constructs a tumbling window partition. The first Push
// after a flush will start a new window timer.
func newTumbling[T any, R any](
	ctx context.Context,
	out chan R,
	procFunc func([]T) (R, error),
	errs chan<- error,
	windowDuration time.Duration,
) partition.Partition[T, R] {
	if windowDuration <= 0 {
		panic("windowDuration must be > 0")
	}

	t := &tumbling[T, R]{
		Base:           partition.NewBase[T, R](ctx, out, errs),
		windowDuration: windowDuration,
		procFunc:       procFunc,
	}
	return t
}

// Push adds to the current batch; if no timer is running, start one.
func (t *tumbling[T, R]) Push(item T) {
	t.Base.Push(item)

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.timerStarted {
		t.timerStarted = true
		go t.waitAndFlush()
	}
}

// waitAndFlush sleeps windowDuration, then flushes whatever is in the batch.
func (t *tumbling[T, R]) waitAndFlush() {
	timer := time.NewTimer(t.windowDuration)
	defer timer.Stop()

	select {
	case <-t.Ctx.Done():
		return
	case <-timer.C:
		// snapshot & clear
		next := t.Batch.Next()
		// publish
		t.Flush(t.Ctx, next, t.procFunc, t.Errs)
		// allow a new window to start
		t.mu.Lock()
		t.timerStarted = false
		t.mu.Unlock()
	}
}

// NewTumblingFactory constructs a factory function that can be used
// to create new instances of tumbling. This is useful for
// creating multiple instances with the same processing function and
// window duration.
func NewTumblingFactory[T any, R any](
	procFunc func([]T) (R, error),
	windowDuration time.Duration,
) partition.Factory[T, R] {
	return func(
		ctx context.Context,
		out chan R,
		errs chan<- error,
	) partition.Partition[T, R] {
		return newTumbling(ctx, out, procFunc, errs, windowDuration)
	}
}
