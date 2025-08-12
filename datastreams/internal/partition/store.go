package partition

import (
	"sync"
)

type ShardKeyFunc[K comparable] func(k K, shardCount int) uint64

type ShardedStore[T any, K comparable] struct {
	store store[K]
	*ShardedStoreOpts[K]
}

type ShardedStoreOpts[K comparable] struct {
	ShardCount   int
	ShardKeyFunc ShardKeyFunc[K]
}

func NewShardedStore[T any, K comparable](
	opts *ShardedStoreOpts[K],
) *ShardedStore[T, K] {
	if opts.ShardCount == 0 {
		opts.ShardCount = 1
	}
	store := make(store[K], opts.ShardCount)
	store.Initialize()
	return &ShardedStore[T, K]{
		store:            store,
		ShardedStoreOpts: opts,
	}
}

func (s *ShardedStore[T, K]) Get(k K) (Partition[T], bool) {
	var zero Partition[T]
	index, found := s.store.Get(k, s.ShardKeyFunc)
	if !found {
		return zero, false
	}

	if value, ok := index.(Partition[T]); ok {
		return value, ok
	}
	return zero, false
}

func (s *ShardedStore[T, K]) Set(k K, v Partition[T]) (shardIndex int) {
	shardIndex = s.store.Set(k, v, s.ShardKeyFunc)
	return
}

func (s *ShardedStore[T, K]) Keys() []K {
	keys := make([]K, 0)
	for shardIndex := range s.store {
		shard := s.store[shardIndex]
		if shard == nil {
			continue
		}
		shard.Range(func(k, _ interface{}) bool {
			keys = append(keys, k.(K))
			return true
		})
	}
	return keys
}

type store[K comparable] []*sync.Map

func (s store[K]) Set(k K, V interface{}, keyFunc ShardKeyFunc[K]) int {
	index := int(keyFunc(k, len(s)))
	if index < 0 || index >= len(s) {
		index = 0 // Fallback to the first shard if index is out of bounds
	}

	shard := s[index]
	if shard == nil {
		shard = &sync.Map{}
		s[index] = shard
	}
	shard.Store(k, V)
	return index
}

func (s store[K]) Get(k interface{}, shardKeyFunc ShardKeyFunc[K]) (interface{}, bool) {
	index := int(shardKeyFunc(k.(K), len(s)))
	shard := s[index]
	if shard == nil {
		return nil, false
	}
	value, ok := shard.Load(k)
	if !ok {
		return nil, false
	}
	return value, true
}

func (s store[K]) Initialize() {
	for i := range s {
		s[i] = &sync.Map{}
	}
}
