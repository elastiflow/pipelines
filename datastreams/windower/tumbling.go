package windower

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

type status struct {
	started atomic.Bool
}

func newStatus() *status {
	return &status{
		started: atomic.Bool{},
	}
}

func (s *status) tryStop() bool {
	return s.started.CompareAndSwap(true, false)
}

func (s *status) tryStart() bool {
	return s.started.CompareAndSwap(false, true)
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

	if t.status.tryStart() {
		go t.waitAndFlush()
	}
}

// waitAndFlush sleeps windowDuration, then flushes whatever is in the batch.
func (t *tumbling[T]) waitAndFlush() {
	timer := time.NewTimer(t.windowDuration)
	defer timer.Stop()

	select {
	case <-t.ctx.Done():
		t.status.tryStop()
		return
	case <-timer.C:
		t.Base.FlushNext(t.ctx)
		t.status.tryStop()
	}
}

// NewTumblingFactory constructs a factory function that can be used
// to create new instances of tumbling. This is useful for
// creating multiple instances with the same processing function and
// window duration.
func NewTumblingFactory[T any](
	windowDuration time.Duration,
) (partition.Factory[T], error) {
	if windowDuration <= 0 {
		return nil, errors.New("window duration must be greater than 0")
	}
	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partition.Partition[T] {
		return newTumbling(ctx, out, errs, windowDuration)
	}, nil
}
