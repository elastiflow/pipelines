package windower

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/elastiflow/pipelines/datastreams/partitioner"
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

// NewSlidingBatch constructs a new sliding batch.
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

// sliding emits overlapping windows of size windowDuration,
// every slideInterval apart.
type sliding[T any] struct {
	*partitioner.Base[T]

	windowDuration time.Duration
	slideInterval  time.Duration
	sb             *slidingBatch[T]
}

// newSliding constructs a sliding window partition that starts ticking immediately.
func newSliding[T any](
	ctx context.Context,
	out pipes.Senders[[]T],
	errs chan<- error,
	windowDuration, slideInterval time.Duration,
) partitioner.Partition[T] {
	s := &sliding[T]{
		Base:           partitioner.NewBase[T](out, errs),
		windowDuration: windowDuration,
		slideInterval:  slideInterval,
		sb:             newSlidingBatch[T](),
	}
	go s.run(ctx)
	return s
}

// Push appends the item with its arrival timestamp.
func (s *sliding[T]) Push(item T) {
	s.sb.push(item, time.Now())
}

// run drives the periodic flushes.
func (s *sliding[T]) run(ctx context.Context) {
	ticker := time.NewTicker(s.slideInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if next := s.sb.next(s.windowDuration, time.Now(), true); len(next) > 0 {
				s.Flush(ctx, next)
			}
			return
		case now := <-ticker.C:
			if next := s.sb.next(s.windowDuration, now, false); len(next) > 0 {
				s.Flush(ctx, next)
			}
		}
	}
}

// NewSlidingFactory constructs a sliding window factory.
func NewSlidingFactory[T any](
	windowDuration, slideInterval time.Duration,
) (partitioner.Factory[T], error) {
	if windowDuration <= 0 || slideInterval <= 0 {
		return nil, errors.New("window duration and slide interval must be greater than 0")

	}
	if slideInterval > windowDuration {
		return nil, errors.New("slide interval must be less than or equal to window duration")
	}

	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partitioner.Partition[T] {
		return newSliding(ctx, out, errs, windowDuration, slideInterval)
	}, nil
}
