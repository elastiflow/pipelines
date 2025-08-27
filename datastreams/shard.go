package datastreams

import (
	"fmt"
	"hash/fnv"
	"sync"
)

// ShardedStore is a thread-safe, in-memory generic key-value store.
// It distributes data across a fixed number of internal shards to reduce
// lock contention and improve concurrency in high-throughput environments.
// The sharding strategy is determined by the provided ShardKeyFunc.
type ShardedStore[T any, K comparable] struct {
	store store[K] // The unexported slice of actual shard stores.
	*ShardedStoreOpts[K]
}

// ShardedStoreOpts provides the configuration for creating a new ShardedStore.
type ShardedStoreOpts[K comparable] struct {
	// ShardCount is the total number of concurrent shards to create.
	// Must be greater than zero.
	ShardCount int
	// ShardKeyFunc is the hashing function used to map a key to a shard index.
	ShardKeyFunc ShardKeyFunc[K]
}

// NewShardedPartitionStore creates and initializes a new ShardedStore with the provided options.
// If the ShardCount in the options is zero, it defaults to 1 to ensure a functional store.
func NewShardedPartitionStore[T any, K comparable](
	opts *ShardedStoreOpts[K],
) *ShardedStore[T, K] {
	if opts.ShardCount == 0 {
		opts.ShardCount = 1
	}
	return &ShardedStore[T, K]{
		store:            make(store[K], opts.ShardCount),
		ShardedStoreOpts: opts,
	}
}

// Get retrieves a Partition[T] by its key K from the store.
// It returns the Partition and a boolean indicating whether the key was found.
// If the key does not exist, it returns a zero value of Partition[T] and false
func (s *ShardedStore[T, K]) Get(k K) (Partition[T, K], bool) {
	var zero Partition[T, K]
	index, found := s.store.Get(k, s.ShardKeyFunc)
	if !found {
		return zero, false
	}

	if value, ok := index.(Partition[T, K]); ok {
		return value, ok
	}
	return zero, false
}

// Set stores a Partition[T] under the key K in the store.
// It returns the index of the shard where the value was stored.
// The shard index is determined by the ShardKeyFunc provided in the store's options.
// If the key already exists, it updates the value in the corresponding shard.
func (s *ShardedStore[T, K]) Set(k K, v Partition[T, K]) (shardIndex int) {
	shardIndex = s.store.Set(k, v, s.ShardKeyFunc)
	return
}

// Keys returns a slice of all keys currently stored in the ShardedStore.
// It iterates through all shards and collects the keys from each shard's sync.Map.
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

// Close clears all entries in the ShardedStore.
// It iterates through all shards and deletes each key-value pair.
func (s *ShardedStore[T, K]) Close() {
	for shardIndex := range s.store {
		shard := s.store[shardIndex]
		if shard == nil {
			continue
		}
		shard.Range(func(k, v interface{}) bool {
			if p, ok := v.(Closeable); ok {
				p.Close()
			}
			shard.Delete(k)
			return true
		})
	}
}

func (s *ShardedStore[T, K]) Initialize() {
	s.store.Initialize()
}

// store is a slice of sync.Map, where each map represents a shard.

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

// ModulusHash implements a simple sharding strategy using the modulo operator (%).
//
// It works by converting the key into a 64-bit hash and then finding the
// remainder when that hash is divided by the total number of shards.
//
// While very fast, its major drawback is that changing the shard count causes
// most keys to be remapped to new shards. It is best used when the number of
// shards is fixed and will not change.
func ModulusHash[K comparable](k K, shardCount int) uint64 {
	if shardCount <= 1 {
		return 0
	}

	key := toHashKey(k)
	return key % uint64(shardCount)
}

// JumpHash implements Jump Consistent Hash, a fast, minimalist algorithm from a
// 2014 Google paper by Lamping and Ringenburg.
//
// Its primary advantage is that it requires the minimum number of keys to be
// remapped when the number of shards (buckets) changes, unlike modulus hashing.
//
// It works by converting the key into a 64-bit integer which is used as a
// random seed. It then deterministically "jumps" forward through bucket indices
// until it lands on the one designated for the key within the given shardCount.
func JumpHash[K comparable](k K, shardCount int) uint64 {
	if shardCount <= 1 {
		return 0
	}

	key := toHashKey(k)
	var b int64 = -1
	var j int64 = 0
	for j < int64(shardCount) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return uint64(b)
}

// toHashKey converts a comparable key into a 64-bit hash using FNV-1a hashing.
// If the key is written to the hash fails, it returns 0 and get added to the default shard.
func toHashKey[K comparable](k K) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprint(k)))
	return h.Sum64()
}
