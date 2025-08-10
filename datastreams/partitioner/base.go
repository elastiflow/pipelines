package partitioner

import (
	"context"
	"sync"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

type Keyable[T any, K comparable] interface {
	Key() K
	Value() T
}

type keyable[T any, K comparable] struct {
	value T
	key   K
}

func NewKeyable[T any, K comparable](t T, key K) Keyable[T, K] {
	return &keyable[T, K]{
		value: t,
		key:   key,
	}
}

func (k keyable[T, K]) Key() K {
	return k.key
}

func (k keyable[T, K]) Value() T {
	return k.value
}

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
type Base[T any] struct {
	Batch *Batch[T]
	Errs  chan<- error
	Out   pipes.Senders[[]T]
}

// NewBase constructs a new Base instance.
func NewBase[T any](
	out pipes.Senders[[]T],
	errs chan<- error,
) *Base[T] {
	return &Base[T]{
		Batch: NewBatch[T](),
		Errs:  errs,
		Out:   out,
	}
}

// Push adds an item to the in-memory buffer.
// The separate ticker goroutine will publish periodically.
func (b *Base[T]) Push(item T) {
	b.Batch.Push(item)
}

func (b *Base[T]) FlushNext(ctx context.Context) {
	next := b.Batch.Next()
	if len(next) == 0 {
		return
	}

	b.Flush(ctx, next)
}

func (b *Base[T]) Flush(ctx context.Context, next []T) {
	for _, out := range b.Out {
		select {
		case <-ctx.Done():
			return
		case out <- next:
		}
	}
}
