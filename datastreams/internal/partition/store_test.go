package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockPartition is a test double for Partition[T, R]
func TestStore_SetAndGet(t *testing.T) {
	s := NewShardedStore[int, string](&ShardedStoreOpts[string]{
		ShardCount:   2,
		ShardKeyFunc: JumpHash[string],
	})

	p1 := &mockPartition[int]{}
	p2 := &mockPartition[int]{}

	s.Set("foo", p1)
	s.Set("bar", p2)

	got1, ok1 := s.Get("foo")
	got2, ok2 := s.Get("bar")

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, p1, got1)
	assert.Equal(t, p2, got2)
}

func TestStore_GetMissingKey(t *testing.T) {
	s := NewShardedStore[int, string](&ShardedStoreOpts[string]{
		ShardCount:   2,
		ShardKeyFunc: ModulusHash[string],
	})
	_, ok := s.Get("nonexistent")
	assert.False(t, ok)
}
