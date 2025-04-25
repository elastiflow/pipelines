package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/partition"
)

// record pairs a value with the moment it arrived.
type record[T any] struct {
	val T
	ts  time.Time
}

// sliding emits overlapping windows of size windowDuration,
// every slideInterval apart.
type sliding[T any] struct {
	*partition.Base[T]

	windowDuration time.Duration
	slideInterval  time.Duration

	mu     sync.Mutex
	buffer []record[T]
}

// newSliding constructs a sliding window partition that starts ticking immediately.
func newSliding[T any](
	ctx context.Context,
	out pipes.Senders[[]T],
	errs chan<- error,
	windowDuration, slideInterval time.Duration,
) partition.Partition[T] {
	if windowDuration <= 0 || slideInterval <= 0 {
		panic("windowDuration and slideInterval must be > 0")
	}
	if slideInterval > windowDuration {
		panic("slideInterval cannot be larger than windowDuration")
	}

	s := &sliding[T]{
		Base:           partition.NewBase[T](ctx, out, errs),
		windowDuration: windowDuration,
		slideInterval:  slideInterval,
	}
	go s.run()
	return s
}

// Push appends the item with its arrival timestamp.
func (s *sliding[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffer = append(s.buffer, record[T]{val: item, ts: time.Now()})
}

// run drives the periodic flushes.
func (s *sliding[T]) run() {
	ticker := time.NewTicker(s.slideInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.Ctx.Done():
			// final, non‑overlapping flush
			s.flush(time.Now(), true)
			return

		case now := <-ticker.C:
			s.flush(now, false)
		}
	}
}

// flush evicts old records and publishes the current window.
func (s *sliding[T]) flush(now time.Time, final bool) {
	threshold := now.Add(-s.windowDuration)

	s.mu.Lock()
	// find cut index via binary search
	cut := 0
	for cut < len(s.buffer) && s.buffer[cut].ts.Before(threshold) {
		cut++
	}
	if cut > 0 {
		// reallocate so we don't hold old large backing arrays
		tail := s.buffer[cut:]
		newBuf := make([]record[T], len(tail))
		copy(newBuf, tail)
		s.buffer = newBuf
	}

	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return
	}

	// snapshot values
	window := make([]T, len(s.buffer))
	for i, rec := range s.buffer {
		window[i] = rec.val
	}
	if final {
		s.buffer = nil
	}
	s.mu.Unlock()

	// publish
	s.Flush(s.Ctx, window)
}

// NewSlidingFactory constructs a sliding window factory.
func NewSlidingFactory[T any](
	windowDuration, slideInterval time.Duration,
) partition.Factory[T] {
	return func(
		ctx context.Context,
		out pipes.Senders[[]T],
		errs chan<- error,
	) partition.Partition[T] {
		return newSliding(ctx, out, errs, windowDuration, slideInterval)
	}
}
