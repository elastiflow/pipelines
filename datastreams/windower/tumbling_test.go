// file: windower/window_test.go
package windower

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestTumblingWindow_Publish(t *testing.T) {
	// aggregatorFunc will transform incoming items []int into a single sum (int).
	aggregatorFunc := func(items []int) (int, error) {
		sum := 0
		for _, i := range items {
			sum += i
		}
		return sum, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errs := make(chan error, 10)
	out := make(pipes.Pipes[int], 1)
	out.Initialize(10)

	// windowDuration = 200ms
	w := NewTumbling[int, int](ctx, out, aggregatorFunc, errs, 200*time.Millisecond)

	// push 8 items, one every 50ms → exactly two windows of 4 items each
	go func() {
		defer out.Close()
		for i := 1; i <= 8; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var results []int
	for v := range out[0] {
		results = append(results, v)
	}

	// Expect two windows: 1+2+3+4=10, 5+6+7+8=26
	assert.ElementsMatch(t, []int{10, 26}, results)
	assert.Empty(t, errs)
}
