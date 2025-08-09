package windower

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/partitioner"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestNewSlidingFactory(t *testing.T) {
	testcases := []struct {
		name           string
		windowDuration time.Duration
		slideInterval  time.Duration
		assertErr      func(t *testing.T, err error)
		assert         func(t *testing.T, p partitioner.Factory[int])
	}{
		{
			name:           "valid parameters",
			windowDuration: 200 * time.Millisecond,
			slideInterval:  50 * time.Millisecond,
			assertErr: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
			assert: func(t *testing.T, p partitioner.Factory[int]) {
				assert.NotNil(t, p)
			},
		},
		{
			name:           "invalid window duration",
			windowDuration: 0 * time.Millisecond,
			slideInterval:  50 * time.Millisecond,
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.Equal(t, "window duration and slide interval must be greater than 0", err.Error())
			},
			assert: func(t *testing.T, p partitioner.Factory[int]) {
				assert.Nil(t, p)
			},
		},
		{
			name:           "invalid slide interval",
			windowDuration: 200 * time.Millisecond,
			slideInterval:  0 * time.Millisecond,
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.Equal(t, "window duration and slide interval must be greater than 0", err.Error())
			},
			assert: func(t *testing.T, p partitioner.Factory[int]) {
				assert.Nil(t, p)
			},
		},
		{
			name:           "invalid window duration and slide interval",
			windowDuration: 0 * time.Millisecond,
			slideInterval:  0 * time.Millisecond,
			assertErr: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.Equal(t, "window duration and slide interval must be greater than 0", err.Error())
			},
			assert: func(t *testing.T, p partitioner.Factory[int]) {
				assert.Nil(t, p)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSlidingFactory[int](tc.windowDuration, tc.slideInterval)
			tc.assertErr(t, err)
			tc.assert(t, s)
		})
	}
}

func TestSlidingBatch_next(t *testing.T) {
	type testCase[T any] struct {
		name           string
		initialRecords []record[T]
		windowDuration time.Duration
		now            time.Time
		final          bool
		want           []T
	}

	now := time.Now()

	tests := []testCase[int]{
		{
			name:           "no records",
			initialRecords: nil,
			windowDuration: time.Second,
			now:            now,
			final:          false,
			want:           []int{},
		},
		{
			name: "all records within window",
			initialRecords: []record[int]{
				{val: 1, ts: now.Add(-500 * time.Millisecond)},
				{val: 2, ts: now.Add(-200 * time.Millisecond)},
			},
			windowDuration: time.Second,
			now:            now,
			final:          false,
			want:           []int{1, 2},
		},
		{
			name: "some records outside window",
			initialRecords: []record[int]{
				{val: 1, ts: now.Add(-2 * time.Second)},
				{val: 2, ts: now.Add(-500 * time.Millisecond)},
				{val: 3, ts: now.Add(-100 * time.Millisecond)},
			},
			windowDuration: time.Second,
			now:            now,
			final:          false,
			want:           []int{2, 3},
		},
		{
			name: "all records outside window",
			initialRecords: []record[int]{
				{val: 1, ts: now.Add(-3 * time.Second)},
				{val: 2, ts: now.Add(-2 * time.Second)},
			},
			windowDuration: time.Second,
			now:            now,
			final:          false,
			want:           []int{},
		},
		{
			name: "final flush",
			initialRecords: []record[int]{
				{val: 1, ts: now.Add(-500 * time.Millisecond)},
				{val: 2, ts: now},
			},
			windowDuration: time.Second,
			now:            now,
			final:          true,
			want:           []int{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &slidingBatch[int]{
				items: tt.initialRecords, // copy to avoid mutation across cases
				mu:    sync.RWMutex{},
			}
			got := sb.next(tt.windowDuration, tt.now, tt.final)

			assert.Len(t, got, len(tt.want))
			assert.ElementsMatch(t, got, tt.want, "unexpected result")
		})
	}
}

func Test_newSliding(t *testing.T) {
	testcases := []struct {
		name           string
		windowDuration time.Duration
		slideInterval  time.Duration
		shouldPanic    bool
		assert         func(t *testing.T, p partitioner.Partition[int])
	}{
		{
			name:           "valid parameters",
			windowDuration: 200 * time.Millisecond,
			slideInterval:  50 * time.Millisecond,
			shouldPanic:    false,
			assert: func(t *testing.T, p partitioner.Partition[int]) {
				assert.NotNil(t, p)
				assert.IsType(t, &sliding[int]{}, p)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			errs := make(chan error, 10)
			out := make(pipes.Pipes[[]int], 1)
			out.Initialize(10)
			defer out.Close()

			s := newSliding[int](ctx, out.Senders(), errs, tc.windowDuration, tc.slideInterval)
			tc.assert(t, s)
		})
	}
}

func BenchmarkSlidingWindow(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[[]int], 3)
	out.Initialize(128)

	w := newSliding[int](ctx, out.Senders(), errs, 100*time.Millisecond, 50*time.Millisecond)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w.Push(1)
				w.Push(2)
				w.Push(3)
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Push(4)
		w.Push(5)
		w.Push(6)
	}
	b.StopTimer()
}
