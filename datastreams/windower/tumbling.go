package windower

import (
	"context"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

type status struct {
	started bool
	mu      sync.Mutex
}

func newStatus() *status {
	return &status{
		started: false,
	}
}

func (t *status) running() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.started
}

func (t *status) set(state bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.started = state
}

// tumbling buffers items for windowDuration once the first item arrives.
// After windowDuration, it publishes the batch and clears it, then waits
// for the next item to start a new timer.
type tumbling[T any] struct {
	*partition.Base[T]
	ctx            context.Context
	status         *status
	windowDuration time.Duration
}

// newTumbling constructs a tumbling window partition. The first Push
// after a flush will start a new window timer.
func newTumbling[T any](
	ctx context.Context,
	out pipes.Senders[[]T],
	errs chan<- error,
	windowDuration time.Duration,
) partition.Partition[T] {
	if windowDuration <= 0 {
		panic("interval must be > 0")
	}

	t := &tumbling[T]{
		Base:           partition.NewBase[T](out, errs),
		windowDuration: windowDuration,
		ctx:            ctx,
		status:         newStatus(),
	}
	return t
}

// Push adds to the current batch; if no timer is running, start one.
func (t *tumbling[T]) Push(item T) {
	t.Base.Push(item)

	if !t.status.running() {
		t.status.set(true)
		go t.waitAndFlush()
	}
}

// waitAndFlush sleeps windowDuration, then flushes whatever is in the batch.
func (t *tumbling[T]) waitAndFlush() {
	timer := time.NewTimer(t.windowDuration)
	defer timer.Stop()

	select {
	case <-t.ctx.Done():
		return
	case <-timer.C:
		t.Base.FlushNext(t.ctx)
		t.status.set(false)
	}
}

// NewTumblingFactory constructs a factory function that can be used
// to create new instances of tumbling. This is useful for
// creating multiple instances with the same processing function and
// window duration.
func NewTumblingFactory[T any](
	windowDuration time.Duration,
) partition.Factory[T] {
	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partition.Partition[T] {
		return newTumbling(ctx, out, errs, windowDuration)
	}
}
