package partition

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
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
	Ctx   context.Context
	Batch *Batch[T]
	Errs  chan<- error
	out   pipes.Senders[R]
}

// NewBase constructs a new Base instance.
func NewBase[T any, R any](
	ctx context.Context,
	out pipes.Senders[R],
	errs chan<- error,
) *Base[T, R] {
	return &Base[T, R]{
		Ctx:   ctx,
		Batch: NewBatch[T](),
		Errs:  errs,
		out:   out,
	}
}

// Push adds an item to the in-memory buffer.
// The separate ticker goroutine will publish periodically.
func (b *Base[T, R]) Push(item T) {
	b.Batch.Push(item)
}

func (b *Base[T, R]) Flush(
	ctx context.Context,
	next []T,
	procFunc func([]T) (R, error),
	errs chan<- error,
) {
	agg, err := procFunc(next)
	if err != nil {
		errs <- err
		return
	}

	for _, outChannel := range b.out {
		select {
		case <-ctx.Done():
		default:
			outChannel <- agg
		}
	}
}
