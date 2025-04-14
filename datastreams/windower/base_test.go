package windower

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Push(t *testing.T) {
	s := newBatch[int]()
	s.Push(1)
	s.Push(2)
	s.Push(3)

	assert.Len(t, s.items, 3)
}

func Test_Next(t *testing.T) {
	s := newBatch[int]()
	s.Push(1)
	s.Push(2)
	s.Push(3)

	batch := s.Next()

	assert.Len(t, batch, 3)
	assert.Len(t, s.items, 0)
}
