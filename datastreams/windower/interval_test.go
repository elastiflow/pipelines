package windower

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestNewInterval(t *testing.T) {
	testcases := []struct {
		name        string
		interval    time.Duration
		shouldPanic bool
	}{
		{
			name:        "valid parameters",
			interval:    200 * time.Millisecond,
			shouldPanic: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			errs := make(chan error, 10)
			out := make(pipes.Pipes[[]int], 1)
			out.Initialize(10)
			defer out.Close()

			i := newInterval[int](ctx, out.Senders(), errs, tc.interval)
			assert.NotNil(t, i)
		})
	}
}

func TestInterval_Flush(t *testing.T) {
	testcases := []struct {
		name         string
		interval     time.Duration
		pushInterval time.Duration
		pushCount    int
		expected     [][]int
	}{
		{
			name:         "exactly two windows",
			interval:     500 * time.Millisecond,
			pushInterval: 100 * time.Millisecond,
			pushCount:    8,
			expected:     [][]int{{1, 2, 3, 4, 5}, {6, 7, 8}},
		},
		{
			name:         "No windows",
			interval:     200 * time.Millisecond,
			pushInterval: 500 * time.Millisecond,
			pushCount:    0,
			expected:     [][]int{},
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

			w := newInterval[int](ctx, out.Senders(), errs, tc.interval)

			go func() {
				for i := 1; i <= tc.pushCount; i++ {
					w.Push(i)
					time.Sleep(tc.pushInterval)
				}
			}()

			var results [][]int
			for {
				select {
				case v := <-out[0]:
					results = append(results, v)
				case <-ctx.Done():
					for i, res := range results {
						assert.ElementsMatch(t, tc.expected[i], res)
						assert.Empty(t, errs)
					}
					return
				}
			}
		})
	}
}
