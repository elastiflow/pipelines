package windower

import (
	"context"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

// Tumbling implements fixed-size, non-overlapping windows that start on
// first element and flush the accumulated batch when the window elapses.
type Tumbling[T any, K comparable] struct {
	WindowDuration time.Duration

	wg        sync.WaitGroup
	done      chan struct{}
	closeOnce sync.Once
}

// NewTumbling constructs a Tumbling window with the provided duration.
func NewTumbling[T any, K comparable](windowDuration time.Duration) *Tumbling[T, K] {
	return &Tumbling[T, K]{
		WindowDuration: windowDuration,
		done:           make(chan struct{}),
	}
}

// Create initializes a new partition for key K that owns all mutable state
// in a single goroutine to avoid data races. The partition receives a
// cancelable child context that is also canceled when Tumbling.Close()
// is called.
func (t *Tumbling[T, K]) Create(ctx context.Context, out chan<- []T) datastreams.Partition[T, K] {
	// Derive a cancelable context per partition.
	pctx, cancel := context.WithCancel(ctx)

	// Bridge Tumbling.Close() -> partition context cancellation.
	go func() {
		select {
		case <-ctx.Done():
		case <-t.done:
		}
		cancel()
	}()

	p := &tumblingPartition[T, K]{
		ctx:    pctx,
		cancel: cancel,
		out:    out,
		window: t.WindowDuration,
		in:     make(chan datastreams.TimedKeyableElement[T, K], 128),
		batch:  NewBatch[T](),
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		p.run()
	}()

	return p
}

// Close cancels all partitions created from this Tumbling and waits
// for their goroutines to exit.
func (t *Tumbling[T, K]) Close() {
	t.closeOnce.Do(func() {
		close(t.done)
		t.wg.Wait()
	})
}

// -------------------- partition --------------------

type tumblingPartition[T any, K comparable] struct {
	ctx    context.Context
	cancel context.CancelFunc
	out    chan<- []T
	window time.Duration

	in    chan datastreams.TimedKeyableElement[T, K]
	batch *Batch[T]

	closeOnce sync.Once
}

// Push enqueues the element for the partition, or returns promptly if canceled.
func (p *tumblingPartition[T, K]) Push(it datastreams.TimedKeyableElement[T, K]) {
	select {
	case <-p.ctx.Done():
		return
	case p.in <- it:
	}
}

// Close cancels the partition's context, causing run() to exit.
func (p *tumblingPartition[T, K]) Close() {
	p.closeOnce.Do(func() { p.cancel() })
}

// run owns the timer and batch; it is the only goroutine that mutates them.
func (p *tumblingPartition[T, K]) run() {
	// Lazy-start timer on first element; until then, the timer case is disabled.
	var timer *time.Timer
	var timerC <-chan time.Time

	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			// Final flush on cancellation.
			p.flush()
			return

		case it := <-p.in:
			p.batch.Push(it.Value())
			// Start the timer when we see the first element of a window.
			if timer == nil {
				timer = time.NewTimer(p.window)
				timerC = timer.C // enabling the timer case
			}

		case <-timerC:
			p.flush()
			// Restart the window timer.
			if timer != nil {
				timer.Reset(p.window)
			}
		}
	}
}

func (p *tumblingPartition[T, K]) flush() {
	if b := p.batch.Next(); len(b) > 0 {
		select {
		case <-p.ctx.Done():
			return
		case p.out <- b:
		}
	}
}

// -------------------- single-owner Batch --------------------

// Batch is a single-owner container for collecting items in a window.
// Since only run() touches it, no mutex is required.
type Batch[T any] struct {
	items []T
}

func NewBatch[T any]() *Batch[T] { return &Batch[T]{items: make([]T, 0, 256)} }

func (b *Batch[T]) Push(item T) { b.items = append(b.items, item) }

func (b *Batch[T]) Next() []T {
	out := b.items
	// Reuse capacity; avoid allocs in steady state.
	b.items = b.items[:0]
	return out
}
