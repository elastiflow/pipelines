package windower

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTumblingInterval_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// aggregatorFunc will transform incoming items []int into a single sum (int).
	aggregatorFunc := func(items []int) (int, error) {
		var sum int
		for _, i := range items {
			sum += i
		}
		return sum, nil
	}

	errs := make(chan error, 10)
	w := NewTumbling[int, int](ctx, aggregatorFunc, errs, 200*time.Millisecond)
	out := w.Initialize()

	go func() {
		defer w.Close()
		for i := 1; i <= 7; i++ {
			w.Push(i, time.Now())
			time.Sleep(50 * time.Millisecond)
		}
	}()

	var results []int
	for r := range out {
		results = append(results, r)
	}

	// The expected results are the sums of the intervals:
	// 1+2+3+4 = 10, 5+6+7+8 = 26
	assert.ElementsMatch(t, []int{10}, results)
}
