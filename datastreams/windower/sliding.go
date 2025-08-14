package windower

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

var slidingBatchPool = sync.Pool{
	New: func() interface{} {
		return &SlidingBatch[any]{
			items: make([]record[any], 0),
		}
	},
}

// record pairs a value with the moment it arrived.
type record[T any] struct {
	val T
	ts  time.Time
}

type SlidingBatch[T any] struct {
	items []record[T]
	mu    sync.RWMutex
}

// NewSlidingBatch constructs a new Sliding batch.
func NewSlidingBatch[T any]() *SlidingBatch[T] {
	return &SlidingBatch[T]{
		items: make([]record[T], 0),
	}
}

// Push appends the item with its arrival timestamp.
func (s *SlidingBatch[T]) Push(item T, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, record[T]{val: item, ts: now})
}

// Next evicts old records and return the current window.
func (s *SlidingBatch[T]) Next(
	windowDuration time.Duration,
	now time.Time,
	final bool,
) []T {
	s.mu.Lock()
	defer s.mu.Unlock()

	if final {
		w := make([]T, len(s.items))
		for i, rec := range s.items {
			w[i] = rec.val
		}
		return w
	}

	threshold := now.Add(-windowDuration)

	cut := sort.Search(len(s.items), func(i int) bool {
		return !s.items[i].ts.Before(threshold)
	})

	if cut > 0 {
		tail := s.items[cut:]
		newBuf := make([]record[T], len(tail))
		copy(newBuf, tail)
		s.items = newBuf
	}

	if len(s.items) == 0 {
		return nil
	}

	w := make([]T, len(s.items))
	for i, rec := range s.items {
		w[i] = rec.val
	}
	return w
}

func (s *SlidingBatch[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.items == nil {
		return 0
	}
	return len(s.items)
}

func (s *SlidingBatch[T]) release() {
	s.items = s.items[:0]
	slidingBatchPool.Put(s)
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
//     and do not overlap. This configuration behaves exactly an windowInterval window.
//
//     |-- Window 1 --|-- Window 2 --|-- Window 3 --|
//
// ** (SlideInterval > WindowDuration): ** Some elements will be
//     dropped as they fall in the gaps between the end of one window and the start of the Next.
//
//     |-- Win 1 --|   (gap)   |-- Win 2 --|
//

type Sliding[T any, K comparable] struct {
	WindowDuration time.Duration
	SlideInterval  time.Duration // This defines the cadence
	wg             *sync.WaitGroup
	done           chan struct{} // Used to signal shutdown
}

// NewSliding constructs a Sliding partitioner.
// Parameters:
//   - windowDuration : The duration of each tumbling window.
//     This is the time after which the collected items will be flushed.
//   - slideInterval  : The windowInterval at which the window is slid forward.
//     This determines how often the window is evaluated and flushed.
func NewSliding[T any, K comparable](
	windowDuration, slideInterval time.Duration,
) *Sliding[T, K] {
	if windowDuration <= 0 {
		windowDuration = 1 * time.Nanosecond // default to 1 Nanosecond
	}
	if slideInterval <= 0 {
		slideInterval = 1 * time.Nanosecond // default to 1 Nanosecond
	}

	return &Sliding[T, K]{
		WindowDuration: windowDuration,
		SlideInterval:  slideInterval,
		wg:             &sync.WaitGroup{},
		done:           make(chan struct{}),
	}
}

// Create initializes a new partition that will publish items
// every `s.SlideInterval` duration, flushing the items collected in the
// last `s.WindowDuration` duration. It returns a new partition
// that can be used to Push items into the buffer.
func (s *Sliding[T, K]) Create(ctx context.Context, out chan<- []T) datastreams.Partition[T, K] {
	p := &slidingPartition[T, K]{
		out:            out,
		WindowDuration: s.WindowDuration,
		batch:          NewSlidingBatch[T](),
		timer:          time.NewTimer(s.SlideInterval),
		interval:       s.SlideInterval,
		wg:             s.wg,
		done:           s.done,
	}

	p.wg.Add(1)
	go p.run()
	return p
}

func (s *Sliding[T, K]) Close() {
	close(s.done)
	s.wg.Wait()
}

type slidingPartition[T any, K comparable] struct {
	WindowDuration time.Duration
	out            chan<- []T
	batch          *SlidingBatch[T]
	timer          *time.Timer
	interval       time.Duration // SlideInterval defines the cadence at which the window is slid forward
	wg             *sync.WaitGroup
	done           chan struct{} // Used to signal shutdown
}

func (p *slidingPartition[T, K]) Push(item datastreams.TimedKeyableElement[T, K]) {
	p.batch.Push(item.Value(), item.Time())
}

func (p *slidingPartition[T, K]) Close() {
	p.batch.release()
}

func (p *slidingPartition[T, K]) run() {
	defer p.wg.Done()
	for {
		select {
		case now := <-p.timer.C:
			if window := p.batch.Next(p.WindowDuration, now, false); len(window) > 0 {
				p.out <- window
			}
			p.timer.Reset(p.interval)
		case <-p.done:
			p.timer.Stop()
			if window := p.batch.Next(p.WindowDuration, time.Now(), true); len(window) > 0 {
				p.out <- window
			}
			return
		}
	}
}

// Interval is a convenience constructor for a sliding window
// It continuously partitions data into sequential, non-overlapping windows of a fixed 'Interval'.
//
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

// NewInterval is a convenience constructor for a sliding window
// where the SlideInterval equals the WindowDuration.
func NewInterval[T any, K comparable](
	interval time.Duration,
) *Sliding[T, K] {
	return NewSliding[T, K](interval, interval)
}
