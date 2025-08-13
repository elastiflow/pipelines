package windower

import (
	"context"
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

// Tumbling implements fixed-size, non-overlapping, sequential windows that are
// triggered by the arrival of data. A new window is
// created only when the first element arrives after a previous window has completed.
//
// The lifecycle is as follows:
//  1. The window is idle, awaiting data.
//  2. The first element arrives, immediately starting a timer for WindowDuration.
//  3. All subsequent elements that arrive before the timer fires are collected into the active window.
//  4. When the timer fires, the entire batch of collected elements is emitted (flushed).
//  5. The window returns to an idle state, waiting for the next element to begin the cycle again.
//
//
// Visualization
//
// ---[Item 1 Arrives]---(items being collected)---[Timer Fires, Window 1 Emits]---(idle)---[Item N Arrives]---
//   |-------------------- Window 1 -------------------|                                  |---- Window 2 ----
//   <------------------ WindowDuration ---------------->

type Tumbling[T any] struct {
	WindowDuration time.Duration
	base           *partition.Base[T]
	status         *status
	ctx            context.Context
}

// NewTumbling constructs a Tumbling window partitioner.
// Parameters:
//   - windowDuration : The duration of each tumbling window.
//     This is the time after which the collected items will be flushed.
func NewTumbling[T any](
	windowDuration time.Duration,
) *Tumbling[T] {
	return &Tumbling[T]{
		WindowDuration: windowDuration,
	}
}

// Create initializes a new partition that will collect items until the
// WindowDuration has elapsed. It returns a new partition that can be used to
// push items into the buffer.
func (t *Tumbling[T]) Create(ctx context.Context, out pipes.Senders[[]T], errs chan<- error) partition.Partition[T] {
	newPartition := NewTumbling[T](t.WindowDuration)
	newPartition.base = partition.NewBase[T](out, errs)
	newPartition.status = newStatus()
	newPartition.ctx = ctx
	return newPartition
}

// Push adds to the current batch; if no timer is running, start one.
func (t *Tumbling[T]) Push(item T) {
	t.base.Push(item)

	if t.status.tryStart() {
		go t.waitAndFlush()
	}
}

func (t *Tumbling[T]) waitAndFlush() {
	timer := time.NewTimer(t.WindowDuration)
	defer timer.Stop()

	select {
	case <-t.ctx.Done():
		t.status.tryStop()
		return
	case <-timer.C:
		t.base.FlushNext(t.ctx)
		t.status.tryStop()
	}
}
