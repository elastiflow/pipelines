package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"sort"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
)

// record pairs a value with the moment it arrived.
type record[T any] struct {
	val T
	ts  time.Time
}

type slidingBatch[T any] struct {
	items []record[T]
	mu    sync.RWMutex
}

// NewSlidingBatch constructs a new Sliding batch.
func newSlidingBatch[T any]() *slidingBatch[T] {
	return &slidingBatch[T]{
		items: make([]record[T], 0),
	}
}

// Push appends the item with its arrival timestamp.
func (s *slidingBatch[T]) push(item T, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, record[T]{val: item, ts: now})
}

// next evicts old records and return the current window.
func (s *slidingBatch[T]) next(
	windowDuration time.Duration,
	now time.Time,
	final bool,
) []T {
	threshold := now.Add(-windowDuration)

	s.mu.Lock()
	// find cut index via binary search
	cut := sort.Search(len(s.items), func(i int) bool {
		return !s.items[i].ts.Before(threshold)
	})
	if cut > 0 {
		// reallocate so we don't hold old large backing arrays
		tail := s.items[cut:]
		newBuf := make([]record[T], len(tail))
		copy(newBuf, tail)
		s.items = newBuf
	}

	if len(s.items) == 0 {
		s.mu.Unlock()
		return []T{}
	}

	// snapshot values
	window := make([]T, len(s.items))
	for i, rec := range s.items {
		window[i] = rec.val
	}
	if final {
		s.items = nil
	}

	s.mu.Unlock()
	// publish
	return window
}

// Sliding implements time-based, overlapping windows of a fixed duration.
//
//
// The relationship between WindowDuration and SlideInterval determines the behavior:
//
//  ** (SlideInterval < WindowDuration): ** This is the classic
//     sliding window where elements can belong to multiple windows.
//
//     Time ->
//     |------- Window 1 -------|
//         |------- Window 2 -------|
//             |------- Window 3 -------|
//     <--SI-->
//     <---------- WindowDuration ---------->
//
// ** (SlideInterval == WindowDuration):** The windows are adjacent
//     and do not overlap. This configuration behaves exactly an interval window.
//
//     |-- Window 1 --|-- Window 2 --|-- Window 3 --|
//
// ** (SlideInterval > WindowDuration): ** Some elements will be
//     dropped as they fall in the gaps between the end of one window and the start of the next.
//
//     |-- Win 1 --|   (gap)   |-- Win 2 --|
//

type Sliding[T any] struct {
	WindowDuration time.Duration
	SlideInterval  time.Duration

	base *partition.Base[T]
	sb   *slidingBatch[T]
}

// NewSliding constructs a Tumbling partitioner.
// Parameters:
//   - windowDuration : The duration of each tumbling window.
//     This is the time after which the collected items will be flushed.
//   - slideInterval  : The interval at which the window is slid forward.
//     This determines how often the window is evaluated and flushed.
func NewSliding[T any](
	windowDuration, slideInterval time.Duration,
) *Sliding[T] {
	s := &Sliding[T]{
		WindowDuration: windowDuration,
		SlideInterval:  slideInterval,
	}
	return s
}

// Create initializes a new partition that will publish items
// every `s.SlideInterval` duration, flushing the items collected in the
// last `s.WindowDuration` duration. It returns a new partition
// that can be used to push items into the buffer.
func (s *Sliding[T]) Create(ctx context.Context, out pipes.Senders[[]T], errs chan<- error) partition.Partition[T] {
	newPartition := NewSliding[T](s.WindowDuration, s.SlideInterval)
	newPartition.base = partition.NewBase[T](out, errs)
	newPartition.sb = newSlidingBatch[T]()
	go newPartition.run(ctx)
	return newPartition
}

// Push appends the item with its arrival timestamp.
func (s *Sliding[T]) Push(item T) {
	s.sb.push(item, time.Now())
}

// run drives the periodic flushes.
func (s *Sliding[T]) run(ctx context.Context) {
	ticker := time.NewTicker(s.SlideInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if next := s.sb.next(s.WindowDuration, time.Now(), true); len(next) > 0 {
				s.base.Flush(ctx, next)
			}
			return
		case now := <-ticker.C:
			if next := s.sb.next(s.WindowDuration, now, false); len(next) > 0 {
				s.base.Flush(ctx, next)
			}
		}
	}
}
