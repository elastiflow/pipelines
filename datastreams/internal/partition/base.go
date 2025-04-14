package partition

import (
	"context"
	"sync"
)

type Batch[T any] struct {
	items []T
	mu    sync.RWMutex
}

// NewBatch constructs a new Batch with an initial capacity.
func NewBatch[T any]() *Batch[T] {
	return &Batch[T]{
		items: make([]T, 0),
	}
}

func (s *Batch[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *Batch[T]) Next() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.items
	s.items = make([]T, 0)
	return b
}

func (s *Batch[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.items == nil {
		return 0
	}
	return len(s.items)

}

// Base provides a shared Publish method to send aggregated results
// into the output channels.
type Base[T any, R any] struct {
	Out   chan R
	Ctx   context.Context
	Batch *Batch[T]
	Errs  chan<- error
}

// NewBase creates a Base given the output "senders" where
// final aggregate results should be published.
func NewBase[T any, R any](
	ctx context.Context,
	errs chan<- error,
) *Base[T, R] {
	return &Base[T, R]{
		Ctx:   ctx,
		Batch: NewBatch[T](),
		Errs:  errs,
		Out:   make(chan R, 10), // TODO: make this configurable
	}
}

func (b *Base[T, R]) Initialize() chan R {
	return b.Out
}

func (b *Base[T, R]) Close() {
	if b.Out != nil {
		close(b.Out)
	}
}

func Flush[T any, R any](
	ctx context.Context,
	next T,
	out chan<- R,
	procFunc func(T) (R, error),
	errs chan<- error,
) {
	select {
	case <-ctx.Done():
	default:
		agg, err := procFunc(next)
		if err != nil {
			errs <- err
			return
		}
		out <- agg
	}
}
