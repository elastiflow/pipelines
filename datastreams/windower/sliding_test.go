package windower

import (
	"context"
	"errors"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSlidingWindow_Publish(t *testing.T) {
	// aggregator sums the window slice
	aggregator := func(items []int) (int, error) {
		sum := 0
		for _, x := range items {
			sum += x
		}
		return sum, nil
	}

	// windowDuration = 100ms, slideInterval = 50ms
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[int], 1)
	out.Initialize(10)

	w := NewSliding[int, int](ctx, out, aggregator, errs, 100*time.Millisecond, 50*time.Millisecond)

	// push items quickly (no delay) so each window sees all three
	go func() {
		defer w.Close()
		w.Push(1)
		w.Push(2)
		w.Push(3)
		time.Sleep(50 * time.Millisecond)
		w.Push(4)
		w.Push(5)
		w.Push(6)
		time.Sleep(200 * time.Millisecond)
	}()

	var results []int
	for v := range out[0] {
		results = append(results, v)
	}

	// We should get 4 windows at ~50,100,150,200, 250ms, each summing to 6 then 15.
	assert.Len(t, results, 3)
	assert.ElementsMatch(t, results, []int{6, 15, 15})

	assert.Empty(t, errs)
}

func TestSlidingWindow_ProcError(t *testing.T) {
	// procFunc always errors
	wantErr := errors.New("bad")
	proc := func([]int) (int, error) { return 0, wantErr }

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[int], 1)
	out.Initialize(10)

	w := NewSliding[int, int](ctx, out, proc, errs, 100*time.Millisecond, 50*time.Millisecond)

	go func() {
		defer out.Close()
		w.Push(42)
		time.Sleep(120 * time.Millisecond) // allow at least one flush
	}()

	// drain output (should be none)
	go func() {
		for range out[0] {
		}
	}()

	var gotErrs []error
	for {
		select {
		case e := <-errs:
			gotErrs = append(gotErrs, e)
		case <-ctx.Done():
			assert.NotEmpty(t, gotErrs, "expected at least one error")
			for _, e := range gotErrs {
				assert.Equal(t, wantErr, e)
			}
			return
		}
	}
}

func BenchmarkSlidingWindow(b *testing.B) {
	// aggregator sums the window slice
	aggregator := func(items []int) (int, error) {
		sum := 0
		for _, x := range items {
			sum += x
		}
		return sum, nil
	}
	// 1. One‑time setup:
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[int], 3)
	out.Initialize(128)

	w := NewSliding[int, int](ctx, out, aggregator, errs,
		100*time.Millisecond, 50*time.Millisecond)

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

	// 2. Reset the timer so setup time isn’t counted:
	b.ResetTimer()

	// 3. Measured loop:
	for i := 0; i < b.N; i++ {
		// e.g. push one more batch of items:
		w.Push(4)
		w.Push(5)
		w.Push(6)
	}

	// 4. (Optional) stop timer, then wait for goroutines to finish:
	b.StopTimer()
	w.Close()
}
