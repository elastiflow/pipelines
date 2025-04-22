package windower

import (
	"context"
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
type sliding[T any, R any] struct {
	*partition.Base[T, R]

	windowDuration time.Duration
	slideInterval  time.Duration
	procFunc       func([]T) (R, error)

	mu     sync.Mutex
	buffer []record[T]
}

// newSliding constructs a sliding window partition that starts ticking immediately.
func newSliding[T any, R any](
	ctx context.Context,
	out chan R,
	procFunc func([]T) (R, error),
	errs chan<- error,
	windowDuration, slideInterval time.Duration,
) partition.Partition[T, R] {
	if windowDuration <= 0 || slideInterval <= 0 {
		panic("windowDuration and slideInterval must be > 0")
	}
	if slideInterval > windowDuration {
		panic("slideInterval cannot be larger than windowDuration")
	}

	s := &sliding[T, R]{
		Base:           partition.NewBase[T, R](ctx, out, errs),
		windowDuration: windowDuration,
		slideInterval:  slideInterval,
		procFunc:       procFunc,
	}
	go s.run()
	return s
}

// Push appends the item with its arrival timestamp.
func (s *sliding[T, R]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffer = append(s.buffer, record[T]{val: item, ts: time.Now()})
}

// run drives the periodic flushes.
func (s *sliding[T, R]) run() {
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
func (s *sliding[T, R]) flush(now time.Time, final bool) {
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
	s.Flush(s.Ctx, window, s.procFunc, s.Errs)
}

// NewSlidingFactory constructs a sliding window factory.
func NewSlidingFactory[T any, R any](
	procFunc func([]T) (R, error),
	windowDuration, slideInterval time.Duration,
) partition.Factory[T, R] {
	return func(
		ctx context.Context,
		out chan R,
		errs chan<- error,
	) partition.Partition[T, R] {
		return newSliding(ctx, out, procFunc, errs, windowDuration, slideInterval)
	}
}
