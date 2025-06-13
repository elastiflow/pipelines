package sinks

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockFlusher is a thread-safe helper for testing onFlush calls.
type mockFlusher[T any] struct {
	mock.Mock
}

func (m *mockFlusher[T]) onFlush(ctx context.Context, batch []T) error {
	args := m.Called(batch)
	return args.Error(0)
}

func TestNewSinker(t *testing.T) {
	mockFlushFunc := func(context.Context, []int) error { return nil }
	errs := make(chan error, 1)
	batchSize := 10

	sinker := NewSinker(mockFlushFunc, batchSize, errs)

	require.NotNil(t, sinker)
	assert.NotNil(t, sinker.onFlush)
	assert.NotNil(t, sinker.batch)
	assert.Equal(t, 0, len(sinker.batch), "Batch slice should be initialized with a length of 0")
	assert.Equal(t, batchSize, cap(sinker.batch), "Batch slice should have capacity equal to batchSize")
}

func TestBatchSinker_Sink(t *testing.T) {
	testcases := []struct {
		name        string
		batchSize   int
		input       []int
		expected    [][]int
		assertFlush func(t *testing.T, flusher *mockFlusher[int])
	}{
		{
			name:      "call the onFlush function with a full batch",
			batchSize: 3,
			input:     []int{1, 2, 3},
			expected:  [][]int{{1, 2, 3}},
			assertFlush: func(t *testing.T, flusher *mockFlusher[int]) {
				flusher.AssertCalled(t, "onFlush", []int{1, 2, 3})
			},
		},
		{
			name:      "if full batch is not reached before timeout, flush the remaining items",
			batchSize: 5,
			input:     []int{1, 2, 3},
			expected:  [][]int{{1, 2, 3}},
			assertFlush: func(t *testing.T, flusher *mockFlusher[int]) {
				flusher.AssertCalled(t, "onFlush", []int{1, 2, 3})
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			flusher := &mockFlusher[int]{}
			errChan := make(chan error, 1)
			flusher.On("onFlush", mock.Anything).Return(nil)
			sinker := NewSinker(flusher.onFlush, tc.batchSize, errChan)
			sinker.batch = make([]int, 0, tc.batchSize)

			inChan := make(chan int, len(tc.input))
			sourceDS := datastreams.New[int](ctx, inChan, errChan)
			go func() {
				for _, val := range tc.input {
					inChan <- val
				}
			}()

			// Send data
			_ = sinker.Sink(ctx, sourceDS)

			// Assertions
			tc.assertFlush(t, flusher)
		})
	}
}
