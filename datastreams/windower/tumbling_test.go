package windower

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestTumbling_NoDataRace_Stress(t *testing.T) {
	runtime.GOMAXPROCS(2)

	const window = 80 * time.Millisecond
	w := NewTumbling[int, string](window)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan []int, 2048)
	p := w.Create(ctx, out)

	const producers = 12
	const perProd = 4000

	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perProd; j++ {
				ke := datastreams.NewKeyedElement[int, string]("k", 1)
				p.Push(datastreams.NewTimedKeyedElement[int, string](ke, time.Now()))
				// small jitter to vary arrival order
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	var flushes int
	var total int
	deadline := time.NewTimer(1200 * time.Millisecond)
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for {
			select {
			case batch := <-out:
				flushes++
				total += len(batch)
			case <-deadline.C:
				return
			}
		}
	}()

	wg.Wait()
	w.Close() // allow final flush
	<-collectDone

	if flushes == 0 {
		t.Fatalf("expected at least one flush; got %d", flushes)
	}
	if total == 0 {
		t.Fatalf("expected at least one item across flushes; got %d", total)
	}
}

func TestTumbling_NoDataRace_FlushCadence(t *testing.T) {
	runtime.GOMAXPROCS(2)

	const window = 100 * time.Millisecond
	w := NewTumbling[int, string](window)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan []int, 4096)
	p := w.Create(ctx, out)

	// Producers run for a while to generate multiple windows.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			end := time.Now().Add(900 * time.Millisecond)
			for time.Now().Before(end) {
				ke := datastreams.NewKeyedElement[int, string]("k", 1)
				p.Push(datastreams.NewTimedKeyedElement[int, string](ke, time.Now()))
				// keep it hot but not overwhelming
				time.Sleep(50 * time.Microsecond)
			}
		}(i)
	}

	// Collect flush timestamps
	var ts []time.Time
	collectDone := make(chan struct{})
	deadline := time.NewTimer(1200 * time.Millisecond)
	go func() {
		defer close(collectDone)
		for {
			select {
			case b := <-out:
				if len(b) > 0 {
					ts = append(ts, time.Now())
				}
			case <-deadline.C:
				return
			}
		}
	}()

	wg.Wait()
	w.Close()
	<-collectDone

	if len(ts) < 3 {
		t.Fatalf("expected multiple flushes; got %d", len(ts))
	}

	// Compute inter-flush deltas and check they hover around `window` within a loose tolerance.
	var deltas []time.Duration
	for i := 1; i < len(ts); i++ {
		deltas = append(deltas, ts[i].Sub(ts[i-1]))
	}
	// Accept broad tolerance to avoid flakes under CI load.
	min := window / 2
	max := window * 2
	bad := 0
	for _, d := range deltas {
		if d < min || d > max {
			bad++
		}
	}
	// Allow a few outliers but the majority should be near cadence.
	if bad > len(deltas)/3 {
		t.Fatalf("too many flush intervals outside [%v, %v]; bad=%d total=%d", min, max, bad, len(deltas))
	}
}
