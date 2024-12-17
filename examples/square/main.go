package main

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"log/slog"

	"github.com/elastiflow/pipelines"
)

type IntConsumer struct {
	num int
	out chan int
}

func (c *IntConsumer) Consume(ctx context.Context, errs chan<- errors.Error) {
	defer close(c.out)
	for i := 0; i < c.num; i++ {
		c.out <- i
	}
}

func (c *IntConsumer) Out() <-chan int {
	return c.out
}

func squareOdds(v int) (int, error) {
	if v%2 == 0 {
		return v, fmt.Errorf("even number error: %v", v)
	}
	return v * v, nil
}

func exProcess(p pipe.DataStream[int]) pipe.DataStream[int] {
	return p.OrDone().FanOut(
		pipe.Params{Num: 2},
	).Run(
		squareOdds,
	)
}

func main() {
	inChan := make(chan int) // Setup channels and cleanup
	errChan := make(chan errors.Error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	pl := pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		&IntConsumer{num: 10, out: make(chan int, 10)},
		errChan,
	).Connect(exProcess)
	go func(errReceiver <-chan errors.Error) { // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.Errors())
	for out := range pl.Out() { // Read Pipeline output
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}

	// Output:
	// {"out":0}
	// {"out":1}
	// {"out":4}
	// {"out":9}
	// {"out":16}
	// {"out":25}
	// {"out":36}
	// {"out":49}
	// {"out":64}
	// {"out":81}
}
