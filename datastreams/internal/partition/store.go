package partition

import (
	"sync"
)

type store[T any, K comparable, R any] struct {
	partitions map[K]Partition[T, R]
	mu         sync.RWMutex
}

func newStore[T any, K comparable, R any]() *store[T, K, R] {
	return &store[T, K, R]{
		partitions: make(map[K]Partition[T, R]),
	}
}

func (s *store[T, K, R]) get(k K) (Partition[T, R], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.partitions[k]
	return w, ok
}

func (s *store[T, K, R]) set(k K, w Partition[T, R]) Partition[T, R] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions[k] = w
	return w
}
