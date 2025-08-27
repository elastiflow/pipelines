package windower

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func newTestElement(key int) datastreams.TimedKeyableElement[int, string] {
	ke := datastreams.NewKeyedElement[int, string](fmt.Sprintf("key-%d", key), key)
	return datastreams.NewTimedKeyedElement(ke, time.Now())
}

func TestNewTumbling(t *testing.T) {
	t.Parallel()
	w := NewTumbling[int, string](200 * time.Millisecond)
	assert.NotNil(t, w)
}

func TestTumbling(t *testing.T) {
	testcases := []struct {
		name             string
		interval         time.Duration
		pushInterval     time.Duration
		pushCount        int
		windowCount      int
		gapDur           time.Duration
		postGapPushCount int
	}{
		{
			name:         "exactly two windows with no gaps",
			interval:     500 * time.Millisecond,
			pushInterval: 100 * time.Millisecond,
			pushCount:    8,
			windowCount:  2,
		},
		{
			name:         "exactly 2 windows two batches with two gaps",
			interval:     200 * time.Millisecond,
			pushInterval: 100 * time.Millisecond,
			pushCount:    4,
			gapDur:       400 * time.Millisecond,
			windowCount:  2,
		},
		{
			name:             "exactly 3 windows two batches with two gaps and one post gap push",
			interval:         200 * time.Millisecond,
			pushInterval:     100 * time.Millisecond,
			pushCount:        4,
			gapDur:           400 * time.Millisecond,
			windowCount:      3,
			postGapPushCount: 1,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			errs := make(chan error, 10)
			partitioner := NewTumbling[int, string](tc.interval)
			out := make(chan []int, 10)
			go func() {
				defer close(out)
				p := partitioner.Create(context.Background(), out)
				for i := 1; i <= tc.pushCount; i++ {
					p.Push(newTestElement(i))
					time.Sleep(tc.pushInterval)
				}
				time.Sleep(tc.gapDur)
				if tc.postGapPushCount > 0 {
					for i := 1; i <= tc.postGapPushCount; i++ {
						p.Push(newTestElement(tc.pushCount + i))
						time.Sleep(tc.pushInterval)
					}
				}
				partitioner.Close()

			}()

			var results [][]int
			for elem := range out {
				results = append(results, elem)
			}

			require.Len(t, results, tc.windowCount, "number of windows mismatch")
			assert.Empty(t, errs)
		})
	}
}

func BenchmarkTumbling(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan []int, 10)
	w := NewTumbling[int, string](200 * time.Millisecond)
	p := w.Create(ctx, out)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				p.Push(newTestElement(1))
				p.Push(newTestElement(2))
				p.Push(newTestElement(3))
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Push(newTestElement(4))
		p.Push(newTestElement(5))
		p.Push(newTestElement(6))
	}
	b.StopTimer()
}
