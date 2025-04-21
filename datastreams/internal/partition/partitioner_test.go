package partition

import (
	"context"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

type mockPartition[T any, R any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
}

func (m *mockPartition[T, R]) Push(item T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, item)
}

func (m *mockPartition[T, R]) Close() {
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

	errs := make(chan error, 10)
	output := make(pipes.Pipes[string], 1)
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
				{key: "a", values: []int{1, 2, 3}},
			},
			expectedKeys: []string{"a"},
			expectedLen:  3,
		},
		{
			name: "Multiple keys",
			inputs: []input{
				{key: "a", values: []int{1}},
				{key: "b", values: []int{2}},
			},
			expectedKeys: []string{"a", "b"},
			expectedLen:  1,
		},
		{
			name: "With time marker and watermark generator",
			inputs: []input{
				{key: "c", values: []int{9, 8}},
			},
			watermarker:  wm,
			timeMarker:   tm,
			expectedKeys: []string{"c"},
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
			// fresh store each test
			mgr := NewPartitioner[int, string, string](
				context.Background(),
				output.Senders(),
				errs,
				func(ctx context.Context, out pipes.Senders[string], errs chan<- error) Partition[int, string] {
					return &mockPartition[int, string]{}
				},
				func() TimeMarker {
					if tt.timeMarker != nil {
						return tt.timeMarker
					}
					return nil
				}(),
				func() WatermarkGenerator[int] {
					if tt.watermarker != nil {
						return tt.watermarker
					}
					return nil
				}(),
			)

			if tt.configMocks != nil {
				tt.configMocks(tm, wm)
			}

			for _, input := range tt.inputs {
				for _, val := range input.values {
					mgr.Partition(input.key, val)
				}
			}

			assert.ElementsMatch(t, mgr.Keys(), tt.expectedKeys)
		})
	}
}
