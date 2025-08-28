package datastreams

import (
	"fmt"
	"hash/fnv"
	"sync"
)

// ShardedStore is a thread-safe, in-memory generic key-value store for per-key Partitions.
// It distributes data across a fixed number of internal shards to reduce lock contention.
type ShardedStore[T any, K comparable] struct {
	store store[K]
	*ShardedStoreOpts[K]
}

// ShardedStoreOpts configures a ShardedStore.
type ShardedStoreOpts[K comparable] struct {
	// ShardCount is the number of shards. If zero, it defaults to 1.
	ShardCount int
	// ShardKeyFunc maps a key to a shard index.
	ShardKeyFunc ShardKeyFunc[K]
}

// NewShardedPartitionStore constructs a new ShardedStore.
func NewShardedPartitionStore[T any, K comparable](opts *ShardedStoreOpts[K]) *ShardedStore[T, K] {
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}
	return &ShardedStore[T, K]{
		store:            make(store[K], opts.ShardCount),
		ShardedStoreOpts: opts,
	}
}

// Initialize allocates shard maps.
func (s *ShardedStore[T, K]) Initialize() { s.store.Initialize() }

// partitionHolder is a lazy-initialization wrapper used by GetOrCreate.
// We publish a holder atomically via sync.Map.LoadOrStore and initialize
// the actual Partition exactly once using sync.Once.
type partitionHolder[T any, K comparable] struct {
	once sync.Once
	p    Partition[T, K]
}

// Get returns the Partition for key k, if present.
// It unwraps a stored *partitionHolder if already initialized.
func (s *ShardedStore[T, K]) Get(k K) (Partition[T, K], bool) {
	var zero Partition[T, K]
	v, ok := s.store.Get(k, s.ShardKeyFunc)
	if !ok {
		return zero, false
	}
	switch vv := v.(type) {
	case Partition[T, K]:
		return vv, true
	case *partitionHolder[T, K]:
		if vv.p != nil {
			return vv.p, true
		}
		return zero, false
	default:
		return zero, false
	}
}

// Set stores a Partition under key k (not typically used once GetOrCreate is available).
func (s *ShardedStore[T, K]) Set(k K, p Partition[T, K]) int {
	return s.store.Set(k, p, s.ShardKeyFunc)
}

// GetOrCreate returns the Partition for key k if it exists, otherwise it atomically
// creates it using the provided creator. The creation is guarded so that at most one
// Partition is constructed per key even under concurrent calls.
func (s *ShardedStore[T, K]) GetOrCreate(k K, creator func() Partition[T, K]) Partition[T, K] {
	idx := int(s.ShardKeyFunc(k, len(s.store)))
	if idx < 0 || idx >= len(s.store) {
		idx = 0
	}
	sh := s.store[idx]
	// Defensive: ensure shard exists in case Initialize wasn't called.
	if sh == nil {
		sh = &sync.Map{}
		s.store[idx] = sh
	}

	// Fast path
	if existing, ok := sh.Load(k); ok {
		switch v := existing.(type) {
		case Partition[T, K]:
			return v
		case *partitionHolder[T, K]:
			v.once.Do(func() {
				if v.p == nil {
					v.p = creator()
				}
			})
			return v.p
		default:
			p := creator()
			sh.Store(k, p)
			return p
		}
	}

	// Slow path: publish a holder, or observe another holder/publication.
	h := &partitionHolder[T, K]{}
	actual, loaded := sh.LoadOrStore(k, h)
	if loaded {
		switch v := actual.(type) {
		case Partition[T, K]:
			return v
		case *partitionHolder[T, K]:
			v.once.Do(func() {
				if v.p == nil {
					v.p = creator()
				}
			})
			return v.p
		default:
			p := creator()
			sh.Store(k, p)
			return p
		}
	}

	// We stored the holder; initialize once.
	h.once.Do(func() { h.p = creator() })
	return h.p
}

// Keys returns all keys currently stored.
func (s *ShardedStore[T, K]) Keys() []K { return s.store.Keys() }

// Close clears all entries in the ShardedStore and calls Close() on values that implement
// the local Closeable interface (if any). We don't assume that Partition has Close() in
// its interface; many concrete partitions do implement Closeable (e.g., window partitions).
func (s *ShardedStore[T, K]) Close() {
	for _, sh := range s.store {
		if sh == nil {
			continue
		}
		sh.Range(func(k, v any) bool {
			if c, ok := v.(Closeable); ok {
				c.Close()
			} else if holder, ok := v.(*partitionHolder[T, K]); ok {
				if holder.p != nil {
					if c2, ok := any(holder.p).(Closeable); ok {
						c2.Close()
					}
				}
			}
			// It is valid to delete while ranging a sync.Map.
			sh.Delete(k)
			return true
		})
	}
}

// === Internal shard machinery ===

type store[K comparable] []*sync.Map

func (s store[K]) Initialize() {
	for i := range s {
		if s[i] == nil {
			s[i] = &sync.Map{}
		}
	}
}

func (s store[K]) Set(k K, V any, keyFunc ShardKeyFunc[K]) int {
	index := int(keyFunc(k, len(s)))
	if index < 0 || index >= len(s) {
		index = 0
	}
	sh := s[index]
	if sh == nil {
		sh = &sync.Map{}
		s[index] = sh
	}
	sh.Store(k, V)
	return index
}

func (s store[K]) Get(k K, keyFunc ShardKeyFunc[K]) (any, bool) {
	index := int(keyFunc(k, len(s)))
	if index < 0 || index >= len(s) {
		index = 0
	}
	sh := s[index]
	if sh == nil {
		return nil, false
	}
	return sh.Load(k)
}

func (s store[K]) Keys() []K {
	keys := make([]K, 0)
	for _, sh := range s {
		if sh == nil {
			continue
		}
		sh.Range(func(k, _ any) bool {
			keys = append(keys, k.(K))
			return true
		})
	}
	return keys
}

// ModulusHash maps a key to a shard using a 64-bit hash modulo.
// Uses deterministic FNV-1a over fmt.Sprint(k) for stable, testable results.
func ModulusHash[K comparable](k K, shardCount int) uint64 {
	if shardCount <= 1 {
		return 0
	}
	return toHashKey(k) % uint64(shardCount)
}

// JumpHash implements Jump Consistent Hash (Lamping & Veach, 2014).
// It operates on a uint64 key so the right shift is logical (not arithmetic).
func JumpHash[K comparable](k K, shardCount int) uint64 {
	if shardCount <= 1 {
		return 0
	}
	key := toHashKey(k) // uint64
	var b int64 = -1
	var j int64 = 0
	for j < int64(shardCount) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return uint64(b)
}

// toHashKey converts a comparable key into a 64-bit hash using FNV-1a on fmt.Sprint(k).
// This mirrors the previous implementation and keeps shard indexes deterministic for tests.
func toHashKey[K comparable](k K) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprint(k)))
	return h.Sum64()
}
