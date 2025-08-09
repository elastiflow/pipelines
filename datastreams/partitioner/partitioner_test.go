package partitioner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

type mockPartition[T any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
}

func (m *mockPartition[T]) Push(item T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, item)
}

func (m *mockPartition[T]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

type mockTimeMarker struct {
	mock.Mock
}

func (m *mockTimeMarker) Now() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

type mockWaterMarker[T any] struct {
	mock.Mock
}

func (w *mockWaterMarker[T]) OnEvent(event T, eventTime time.Time) {
	w.Called(event, event)
}

func (w *mockWaterMarker[T]) GetWatermark() time.Time {
	args := w.Called()
	return args.Get(0).(time.Time)
}

func TestPartitioner_Partition(t *testing.T) {
	type input struct {
		key    string
		values []int
	}

	output := make(pipes.Pipes[[]int], 1)
	output.Initialize(10)

	tm := &mockTimeMarker{}
	wm := &mockWaterMarker[int]{}

	tests := []struct {
		name         string
		inputs       []input
		watermarker  WatermarkGenerator[int]
		timeMarker   TimeMarker
		expectedKeys []string
		expectedLen  int
		configMocks  func(*mockTimeMarker, *mockWaterMarker[int])
	}{
		{
			name: "Single key, multiple values",
			inputs: []input{
				{key: "1", values: []int{1, 1, 1}},
			},
			expectedKeys: []string{"1"},
			expectedLen:  3,
		},
		{
			name: "Multiple keys",
			inputs: []input{
				{key: "1", values: []int{1}},
				{key: "2", values: []int{2}},
			},
			expectedKeys: []string{"1", "2"},
			expectedLen:  1,
		},
		{
			name: "With time marker and watermark generator",
			inputs: []input{
				{key: "", values: []int{9, 8}},
			},
			watermarker:  wm,
			timeMarker:   tm,
			expectedKeys: []string{"9", "8"},
			expectedLen:  2,
			configMocks: func(tm *mockTimeMarker, wm *mockWaterMarker[int]) {
				tm.On("Now").Return(time.Now())
				wm.On("OnEvent", mock.Anything, mock.Anything).Return()
				wm.On("GetWatermark").Return(time.Now())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := New[int, string](
				func(ctx context.Context, out pipes.Senders[[]int], errs chan<- error) Partition[int] {
					return &mockPartition[int]{}
				},
			).WithWatermarkGenerator(tt.watermarker).
				WithTimeMarker(tt.timeMarker).
				WithContext(context.Background()).
				WithSenders(output.Senders()).
				Build()

			if tt.configMocks != nil {
				tt.configMocks(tm, wm)
			}

			inputStreams := make([]<-chan int, len(tt.inputs))
			for i, vals := range tt.inputs {
				ch := make(chan int, len(vals.values))
				for _, val := range tt.inputs[i].values {
					ch <- val
				}
				close(ch)
				inputStreams[i] = ch
			}

			mgr.Partition(func(i int) string {
				return fmt.Sprintf("%d", i)
			}, inputStreams)

			assert.Eventually(t, func() bool {
				return assert.ElementsMatch(t, mgr.Keys(), tt.expectedKeys)
			}, 100*time.Millisecond, 10*time.Millisecond)

		})
	}
}
