package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockPartition is a test double for Partition[T, R]
func TestStore_SetAndGet(t *testing.T) {
	s := newStore[int, string]()

	p1 := &mockPartition[int]{}
	p2 := &mockPartition[int]{}

	s.set("foo", p1)
	s.set("bar", p2)

	got1, ok1 := s.get("foo")
	got2, ok2 := s.get("bar")

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, p1, got1)
	assert.Equal(t, p2, got2)
}

func TestStore_GetMissingKey(t *testing.T) {
	s := newStore[int, string]()
	_, ok := s.get("nonexistent")
	assert.False(t, ok)
}
