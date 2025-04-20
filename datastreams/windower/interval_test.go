package windower

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTumblingTime_Publish(t *testing.T) {
	// aggregatorFunc will transform incoming items []int into a single sum (int).
	aggregatorFunc := func(items []int) (int, error) {
		var sum int
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
	w := NewInterval[int, int](ctx, out, aggregatorFunc, errs, 200*time.Millisecond)

	go func() {
		defer out.Close()
		for i := 1; i <= 10; i++ {
			w.Push(i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var results []int
	for res := range out[0] {
		results = append(results, res)
	}

	// The expected results are the sums of the intervals:
	// 1+2+3+4 = 10, 5+6+7+8 = 26
	assert.ElementsMatch(t, []int{10, 26}, results)
}
