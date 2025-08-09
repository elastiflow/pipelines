package partitioner

import "sync"

type store[T any, K comparable] struct {
	partitions sync.Map
}

func newStore[T any, K comparable]() *store[T, K] {
	return &store[T, K]{}
}

func (s *store[T, K]) LoadOrStore(k K, p Partition[T]) (actual Partition[T], loaded bool) {
	res, loaded := s.partitions.LoadOrStore(k, p)
	return res.(Partition[T]), loaded
}
