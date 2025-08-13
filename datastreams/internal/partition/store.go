package partition

import (
	"sync"
)

// ShardKeyFunc defines the function signature for a sharding algorithm.
// It takes a key and the total number of shards and must return a deterministic
// shard index (typically in the range [0, shardCount-1]). Implementations can
// include modulus hashing, jump hashing, or other consistent hashing algorithms.
type ShardKeyFunc[K comparable] func(k K, shardCount int) uint64

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

// NewShardedStore creates and initializes a new ShardedStore with the provided options.
// If the ShardCount in the options is zero, it defaults to 1 to ensure a functional store.
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

// Get retrieves a Partition[T] by its key K from the store.
// It returns the Partition and a boolean indicating whether the key was found.
// If the key does not exist, it returns a zero value of Partition[T] and false
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

// Set stores a Partition[T] under the key K in the store.
// It returns the index of the shard where the value was stored.
// The shard index is determined by the ShardKeyFunc provided in the store's options.
// If the key already exists, it updates the value in the corresponding shard.
func (s *ShardedStore[T, K]) Set(k K, v Partition[T]) (shardIndex int) {
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
