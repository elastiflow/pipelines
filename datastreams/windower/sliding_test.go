package windower

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
			sb := &SlidingBatch[int]{
				items: tt.initialRecords, // copy to avoid mutation across cases
				mu:    sync.RWMutex{},
			}
			got := sb.Next(tt.windowDuration, tt.now, tt.final)

			assert.Len(t, got, len(tt.want))
			assert.ElementsMatch(t, got, tt.want, "unexpected result")
		})
	}
}

func TestSliding(t *testing.T) {
	testcases := []struct {
		name         string
		interval     time.Duration
		slide        time.Duration
		pushInterval time.Duration
		pushCount    int
		windowCount  int
	}{
		{
			name:         "interval: exactly two windows",
			interval:     500 * time.Millisecond,
			slide:        500 * time.Millisecond,
			pushInterval: 100 * time.Millisecond,
			pushCount:    8,
			windowCount:  2,
		},
		{
			name:         "No windows",
			interval:     200 * time.Millisecond,
			slide:        500 * time.Millisecond,
			pushInterval: 0 * time.Millisecond,
			pushCount:    0,
			windowCount:  0,
		},
		{
			name:         "Overlapping windows",
			interval:     500 * time.Millisecond,
			slide:        200 * time.Millisecond,
			pushInterval: 100 * time.Millisecond,
			pushCount:    8,
			windowCount:  5,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := make(chan []int, 10)
			s := NewSliding[int, string](tc.interval, tc.slide)
			go func() {
				defer close(out)
				p := s.Create(context.Background(), out)
				for i := 1; i <= tc.pushCount; i++ {
					p.Push(newTestElement(i))
					time.Sleep(tc.pushInterval)
				}
				s.Close()
			}()

			var results [][]int
			for w := range out {
				results = append(results, w)
			}
			assert.Equal(t, tc.windowCount, len(results), "unexpected number of windows")
		})
	}
}
func TestNewSliding(t *testing.T) {
	t.Parallel()
	s := NewSliding[int, string](200*time.Millisecond, 50*time.Millisecond)
	assert.NotNil(t, s)
}
func TestNewInterval(t *testing.T) {
	out := make(chan []int, 10)
	i := NewInterval[int, string](200*time.Millisecond).Create(context.Background(), out)
	assert.NotNil(t, i)
}
func BenchmarkSlidingWindow(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan []int, 50)
	w := NewSliding[int, string](100*time.Millisecond, 50*time.Millisecond).Create(ctx, out)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				w.Push(newTestElement(1))
				w.Push(newTestElement(2))
				w.Push(newTestElement(3))
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Push(newTestElement(4))
		w.Push(newTestElement(5))
		w.Push(newTestElement(6))
	}
	b.StopTimer()
	b.ReportAllocs()
}
