package windower

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestTumblingWindow_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[[]int], 1)
	out.Initialize(10)
	defer out.Close()

	// windowDuration = 200ms
	w := newTumbling[int](ctx, out.Senders(), errs, 200*time.Millisecond)

	// push 8 items, one every 50ms → exactly two windows of 4 items each
	go func() {
		for i := 1; i <= 8; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var results [][]int
	for {
		select {
		case v := <-out[0]:
			results = append(results, v)
		case <-ctx.Done():
			assert.ElementsMatch(t, []int{10, 26}, results)
			assert.Empty(t, errs)
			return
		}
	}
}

func TestTumblingWindow_ProcError(t *testing.T) {
	// procFunc always errors
	wantErr := errors.New("bad")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[[]int], 1)
	out.Initialize(10)

	w := newTumbling[int, int](ctx, out.Senders(), errs, 200*time.Millisecond)
	// push 8 items, one every 50ms → exactly two windows of 4 items each
	go func() {
		defer out.Close()
		for i := 1; i <= 8; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var results [][]int
	for v := range out[0] {
		results = append(results, v)
	}

	assert.Empty(t, results)
	assert.Len(t, errs, 2)
	assert.Equal(t, wantErr, <-errs)
}

func BenchmarkTumblingWindow(b *testing.B) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 10)
	out := make(pipes.Pipes[[]int], 1)
	out.Initialize(10)

	w := newTumbling[int, int](ctx, out.Senders(), errs, 200*time.Millisecond)

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
