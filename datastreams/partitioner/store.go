package partitioner

import (
	"sync"
)

type store[T any, K comparable] struct {
	partitions map[K]Partition[T]
	mu         sync.RWMutex
}

func newStore[T any, K comparable]() *store[T, K] {
	return &store[T, K]{
		partitions: make(map[K]Partition[T]),
	}
}

func (s *store[T, K]) get(k K) (Partition[T], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.partitions[k]
	return w, ok
}

func (s *store[T, K]) set(k K, w Partition[T]) Partition[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions[k] = w
	return w
}
