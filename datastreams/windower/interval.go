package windower

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
)

// TimedInterval implements a traditional, time-aligned, fixed-size window,
// It continuously partitions data into sequential, non-overlapping windows of a fixed 'Interval'.
// .
//
// It's ideal for periodic, regular reporting, such as "calculate the total
// number of events every 5 minutes".
//
//
//
// Visualization
//
// |---- Window 1 ----|---- Window 2 ----|---- Window 3 ----|
//

type TimedInterval[T any] struct {
	base     *partition.Base[T]
	Interval time.Duration
}

// NewInterval starts publishing *immediately*. If no items
// arrive in a window, we publish an empty batch.
func NewInterval[T any](
	interval time.Duration,
) *TimedInterval[T] {
	return &TimedInterval[T]{
		Interval: interval,
	}
}

// Create initializes a new partition that will publish items
// every `w.Interval` duration. It returns a new partition
// that can be used to push items into the buffer.
func (t *TimedInterval[T]) Create(
	ctx context.Context, out pipes.Senders[[]T], errs chan<- error,
) partition.Partition[T] {
	newPartition := NewInterval[T](t.Interval)
	newPartition.base = partition.NewBase[T](out, errs)
	go newPartition.startInterval(ctx)
	return newPartition
}

func (t *TimedInterval[T]) Push(item T) {
	t.base.Push(item)
}

// startInterval continuously ticks every `w.Interval`, publishes whatever
// items are in the buffer, and clears it.
func (t *TimedInterval[T]) startInterval(ctx context.Context) {
	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go t.base.FlushNext(ctx)
		case <-ctx.Done():
			return
		}
	}
}
