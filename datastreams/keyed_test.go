package datastreams

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockPartition is a mock implementation of the Partition interface.
// It collects all items pushed to it and flushes them on Close().
type MockPartition[T any, K comparable] struct {
	mock.Mock // Embed mock for call assertion capabilities if needed
	out       chan<- []T
	items     []TimedKeyableElement[T, K]
	mu        sync.Mutex
}

func (m *MockPartition[T, K]) Push(item TimedKeyableElement[T, K]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, item)
}

func (m *MockPartition[T, K]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.items) > 0 {
		// Create the window from all collected items.
		window := make([]T, len(m.items))
		for i, it := range m.items {
			window[i] = it.Value()
		}
		m.out <- window
	}
}

// MockPartitioner is a mock factory that creates MockPartition instances.
type MockPartitioner[T any, K comparable] struct {
	// You can add fields here if you need to configure its behavior,
	// but for this test, none are needed.
}

// Create returns a new MockPartition instance ready for testing.
func (m *MockPartitioner[T, K]) Create(ctx context.Context, out chan<- []T) Partition[T, K] {
	return &MockPartition[T, K]{
		out: out,
	}
}

// Close
func (m *MockPartitioner[T, K]) Close() {
}

// NewMockPartitioner is the constructor you called in your test.
// The 'size' parameter is ignored as this mock batches everything.
func NewMockPartitioner[T any, K comparable](size int) *MockPartitioner[T, K] {
	return &MockPartitioner[T, K]{}
}

type testStruct struct {
	ID   int
	Name string
}

func TestKeyBy(t *testing.T) {
	tests := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		process  ProcessFunc[testStruct]
		elements []testStruct
	}{
		{
			name: "should key by even/odd",
			keyBy: func(t testStruct) int {
				return t.ID % 2
			},
			process: func(t testStruct) (testStruct, error) {
				return testStruct{ID: t.ID * 2, Name: t.Name}, nil
			},
			elements: []testStruct{
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			// Some source of integers.
			input := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				defer close(input)
				for _, elem := range inputElements {
					select {
					case <-ctx.Done():
						return
					default:
						input <- elem
					}
				}
			}(ctx, tt.elements)

			// Key the DataStream by "even"/"odd".
			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, input, errCh).WithWaitGroup(&sync.WaitGroup{}),
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1, // only 1 output channel per key
				},
			)

			out := make([]int, 0)
			for res := range kds.OrDone().Out() {
				out = append(out, res.Value().ID)
			}
			assert.Len(t, out, 5)
		})
	}
}

func TestWindow(t *testing.T) {
	testCases := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		elements []testStruct
		process  func(t []testStruct) (testStruct, error)
		expected []testStruct
		errLen   int
	}{
		{
			name: "should key given key by even/odd and process",
			keyBy: func(t testStruct) int {
				return t.ID % 2 // Keys will be 0 for even, 1 for odd
			},
			process: func(t []testStruct) (testStruct, error) {
				newStr := testStruct{}
				for _, elem := range t {
					newStr.ID += elem.ID
					newStr.Name += elem.Name
				}
				return newStr, nil
			},
			elements: []testStruct{
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			},
			expected: []testStruct{
				// Expected result for key 0 (even): 0+2+4=6, "024"
				{ID: 6, Name: "024"},
				// Expected result for key 1 (odd): 1+3=4, "13"
				{ID: 4, Name: "13"},
			},
		},
		{
			name:     "An empty input stream should produce no output",
			keyBy:    func(t testStruct) int { return t.ID },
			process:  func(t []testStruct) (testStruct, error) { return testStruct{}, nil },
			elements: []testStruct{},
			expected: []testStruct{},
		},
		{
			name: "All elements mapping to a single key should produce one window",
			keyBy: func(t testStruct) int {
				return 1 // All items map to the same key
			},
			process: func(t []testStruct) (testStruct, error) {
				newStr := testStruct{}
				for _, elem := range t {
					newStr.ID += elem.ID
					newStr.Name += elem.Name
				}
				return newStr, nil
			},
			elements: []testStruct{
				{ID: 10, Name: "A"},
				{ID: 20, Name: "B"},
				{ID: 30, Name: "C"},
			},
			expected: []testStruct{
				{ID: 60, Name: "ABC"}, // 10+20+30, "A"+"B"+"C"
			},
		},
		{
			name:  "An error in the processing function should send to err channel and produce no output",
			keyBy: func(t testStruct) int { return 1 },
			process: func(t []testStruct) (testStruct, error) {
				// This function always returns an error
				return testStruct{}, fmt.Errorf("processing failed")
			},
			elements: []testStruct{
				{ID: 1, Name: "A"},
				{ID: 2, Name: "B"},
			},
			expected: []testStruct{}, // No results should be sent to the output
			errLen:   1,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			input := make(chan testStruct, 10)
			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, input, errCh), // Simplified this line for clarity
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1,
				},
			)

			// Use the new mock partitioner here
			out := Window[testStruct, int, testStruct](
				kds,
				tt.process,
				NewMockPartitioner[testStruct, int](3), // This now works
				Params{
					BufferSize: 50,
				},
			)

			go func(inputElements []testStruct) {
				defer close(input)
				for _, elem := range inputElements {
					input <- elem
				}
			}(tt.elements)

			endRes := make([]testStruct, 0)
			for res := range out.OrDone().Out() {
				endRes = append(endRes, res)
			}
			close(errCh) // Close the error channel to simulate no more errors

			// The ElementsMatch assertion is perfect here since the order of
			// window emission (even vs. odd) is not guaranteed.
			assert.ElementsMatch(t, tt.expected, endRes)
			assert.Equal(t, len(errCh), tt.errLen, "unexpected number of errors")
		})
	}
}
