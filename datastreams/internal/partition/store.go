package partition

import (
	"sync"
)

type partition[T any, R any] struct {
	Partition[T, R]
	out chan R
}

type store[T any, K comparable, R any] struct {
	partitions map[K]partition[T, R]
	mu         sync.RWMutex
}

func newStore[T any, K comparable, R any]() *store[T, K, R] {
	return &store[T, K, R]{
		partitions: make(map[K]partition[T, R]),
	}
}

func (s *store[T, K, R]) get(k K) (partition[T, R], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.partitions[k]
	return w, ok
}

func (s *store[T, K, R]) set(k K, w Partition[T, R], out chan R) partition[T, R] {
	s.mu.Lock()
	defer s.mu.Unlock()
	newPartition := partition[T, R]{
		Partition: w,
		out:       out,
	}
	s.partitions[k] = newPartition
	return newPartition
}

func (s *store[T, K, R]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.partitions {
		w.Close()
	}
}
