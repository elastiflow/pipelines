package windower

import (
	"context"
	"sync"
)

type batch[T any] struct {
	items []T
	mu    sync.RWMutex
}

// newBatch constructs a new batch with an initial capacity.
func newBatch[T any]() *batch[T] {
	return &batch[T]{
		items: make([]T, 0),
	}
}

func (s *batch[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *batch[T]) Next() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.items
	s.items = make([]T, 0)
	return b
}

func (s *batch[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.items == nil {
		return 0
	}
	return len(s.items)

}

// base provides a shared Publish method to send aggregated results
// into the output channels.
type base[T any, R any] struct {
	Out      chan R
	ProcFunc func([]T) (R, error)
	Ctx      context.Context
	Batch    *batch[T]
	Errs     chan<- error
}

// newBase creates a base given the output "senders" where
// final aggregate results should be published.
func newBase[T any, R any](
	ctx context.Context,
	procFunc func([]T) (R, error),
	errs chan<- error,
) *base[T, R] {
	return &base[T, R]{
		ProcFunc: procFunc,
		Ctx:      ctx,
		Batch:    newBatch[T](),
		Errs:     errs,
		Out:      make(chan R, 10),
	}
}

func (b *base[T, R]) Initialize() chan R {
	return b.Out
}

func (b *base[T, R]) Close() {
	if b.Out != nil {
		close(b.Out)
	}
}

func (b *base[T, R]) flush(
	ctx context.Context,
	next []T,
	out chan<- R,
	procFunc func([]T) (R, error),
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
