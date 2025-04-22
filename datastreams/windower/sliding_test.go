package windower

import (
	"context"
	"errors"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSlidingWindow_ProcError(t *testing.T) {
	wantErr := errors.New("bad")
	proc := func([]int) (int, error) { return 0, wantErr }

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[int], 1)
	out.Initialize(10)

	w := newSliding[int, int](ctx, out[0], proc, errs, 100*time.Millisecond, 50*time.Millisecond)

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[int], 3)
	out.Initialize(128)

	w := newSliding[int, int](ctx, out[0], aggregator, errs,
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Push(4)
		w.Push(5)
		w.Push(6)
	}
	b.StopTimer()
}
