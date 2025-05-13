package windower

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestNewTumbling(t *testing.T) {
	testcases := []struct {
		name           string
		windowDuration time.Duration
		pushInterval   time.Duration
		pushCount      int
		expected       [][]int
		assertPanics   bool
	}{
		{
			name:           "Invalid window duration",
			windowDuration: 0 * time.Millisecond,
			pushInterval:   500 * time.Millisecond,
			pushCount:      8,
			expected:       [][]int{{1, 2, 3, 4}, {5, 6, 7, 8}},
			assertPanics:   true,
		},
		{
			name:           "Valid window duration",
			windowDuration: 200 * time.Millisecond,
			pushInterval:   500 * time.Millisecond,
			pushCount:      0,
			expected:       [][]int{},
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

			if tc.assertPanics {
				assert.Panics(t, func() {
					newTumbling[int](ctx, out.Senders(), errs, tc.windowDuration)
				}, "Expected panic when interval is less than or equal to 0")
				return
			}

			w := newTumbling[int](ctx, out.Senders(), errs, tc.windowDuration)
			assert.NotNil(t, w)

		})
	}
}

func TestTumblingWindow_Flush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[[]int], 1)
	out.Initialize(10)
	defer out.Close()

	// interval = 200ms
	w := newTumbling[int](ctx, out.Senders(), errs, 200*time.Millisecond)

	// push 8 items, one every 50ms → exactly two windows of 4 items each
	go func() {
		for i := 1; i <= 4; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}

		// ensure we are out the way of the first window
		time.Sleep(100 * time.Millisecond)

		for i := 5; i <= 8; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}

		time.Sleep(100 * time.Millisecond)
		// No more items will be pushed
	}()

	var results [][]int
	expected := [][]int{{1, 2, 3, 4}, {5, 6, 7, 8}}
	for {
		select {
		case v := <-out[0]:
			results = append(results, v)
		case <-ctx.Done():
			for i, res := range results {
				assert.ElementsMatch(t, expected[i], res)
				assert.Empty(t, errs)
			}
			return
		}
	}
}

func BenchmarkTumbling(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 10)
	out := make(pipes.Pipes[[]int], 1)
	out.Initialize(10)

	w := newTumbling[int](ctx, out.Senders(), errs, 200*time.Millisecond)
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
