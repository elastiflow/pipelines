package windower

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

// Tumbling implements fixed-size, non-overlapping, sequential windows that are
// triggered by the arrival of data. A new window is
// created only when the first element arrives after a previous window has completed.
//
// The lifecycle is as follows:
//  1. The window is idle, awaiting data.
//  2. The first element arrives, immediately starting a ticker for WindowDuration.
//  3. All subsequent elements that arrive before the ticker fires are collected into the active window.
//  4. When the ticker fires, the entire batch of collected elements is emitted (flushed).
//  5. The window returns to an idle state, waiting for the Next element to begin the cycle again.
//
//
// Visualization
//
// ---[Item 1 Arrives]---(items being collected)---[Timer Fires, Window 1 Emits]---(idle)---[Item N Arrives]---
//   |-------------------- Window 1 -------------------|                                  |---- Window 2 ----
//   <------------------ WindowDuration ---------------->

type Tumbling[T any, K comparable] struct {
	WindowDuration time.Duration
	wg             *sync.WaitGroup // WaitGroup to manage goroutines
	done           chan struct{}   // Channel to signal shutdown
	closeDone      *sync.Once      // Ensures we only close done once
}

// NewTumbling constructs a Tumbling window partitioner.
// Parameters:
//   - windowDuration : The duration of each tumbling window.
//     This is the time after which the collected items will be flushed.
func NewTumbling[T any, K comparable](
	windowDuration time.Duration,
) *Tumbling[T, K] {
	return &Tumbling[T, K]{
		WindowDuration: windowDuration,
		wg:             &sync.WaitGroup{},
		done:           make(chan struct{}),
		closeDone:      &sync.Once{},
	}
}

// Create initializes a new partition that will collect items until the
// WindowDuration has elapsed. It returns a new partition that can be used to
// Push items into the buffer.
func (t *Tumbling[T, K]) Create(ctx context.Context, out chan<- []T) datastreams.Partition[T, K] {
	// Create the instance that will manage the state for one key.
	p := &tumblingPartition[T, K]{
		ctx:            ctx,
		out:            out,
		windowDuration: t.WindowDuration,
		batch:          NewBatch[T](),
		windowSwitch:   newStatus(),
		timer:          time.NewTimer(t.WindowDuration),
		done:           t.done,
		wg:             t.wg,
	}
	return p
}

// Close signals the Tumbling partitioner to stop accepting new items and flush any remaining data.
// It waits for all active partitions to finish processing before returning.
func (t *Tumbling[T, K]) Close() {
	t.closeDone.Do(func() {
		close(t.done)
	})
	t.wg.Wait()
}

type tumblingPartition[T any, K comparable] struct {
	ctx            context.Context
	out            chan<- []T
	windowDuration time.Duration
	batch          *Batch[T]
	windowSwitch   *status // Atomically tracks if a window is active
	done           chan struct{}
	wg             *sync.WaitGroup
	timer          *time.Timer
}

// Push adds an item to the batch. If no window is active, it starts one.
func (p *tumblingPartition[T, K]) Push(item datastreams.TimedKeyableElement[T, K]) {
	p.batch.Push(item.Value())
	if p.windowSwitch.tryStart() {
		// Starting a new window - reset the timer to fire after windowDuration
		p.timer.Reset(p.windowDuration)
		p.wg.Add(1)
		go p.waitAndFlush()
	}
}

func (p *tumblingPartition[T, K]) waitAndFlush() {
	defer p.wg.Done()
	defer p.windowSwitch.tryStop()

	select {
	case <-p.done:
		p.timer.Stop()
		p.flush()
		return
	case <-p.timer.C:
		if !p.windowSwitch.started.Load() {
			return // No active window, nothing to flush
		}
		p.flush()
		// Don't reset timer here - this goroutine exits after flushing.
		// A new window (and new waitAndFlush goroutine) will start on the next Push.
	}

}

func (p *tumblingPartition[T, K]) flush() {
	if batch := p.batch.Next(); len(batch) > 0 {
		select {
		case p.out <- batch:
		case <-p.ctx.Done():
			// Context canceled - stop trying to send
			return
		}
	}
}

// status is a helper for atomic boolean state.
type status struct {
	started atomic.Bool
}

func newStatus() *status         { return &status{} }
func (s *status) tryStart() bool { return s.started.CompareAndSwap(false, true) }
func (s *status) tryStop() bool  { return s.started.CompareAndSwap(true, false) }
func (s *status) Stop() bool {
	if !s.started.Load() {
		return false // Already stopped
	}
	s.started.Store(false)
	return true
}

type Batch[T any] struct {
	items []T
	mu    sync.RWMutex
}

func NewBatch[T any]() *Batch[T] { return &Batch[T]{items: make([]T, 0)} }
func (s *Batch[T]) Push(item T)  { s.mu.Lock(); s.items = append(s.items, item); s.mu.Unlock() }
func (s *Batch[T]) Next() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.items
	s.items = make([]T, 0)
	return b
}
