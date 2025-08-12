package partition

import (
	"fmt"
	"hash/fnv"
)

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
func toHashKey[K comparable](k K) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprint(k)))
	return h.Sum64()
}
