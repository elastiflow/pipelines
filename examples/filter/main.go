package main

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"log/slog"
	"time"
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

func filter(p int) (bool, error) {
	return p%2 == 0, nil
}

func mapFunc(p int) (string, error) {
	return fmt.Sprintf("Im an even number: %d", p), nil
}

func main() {
	errChan := make(chan errors.Error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer func() {
		close(errChan)
		cancel()
	}()

	connector := func(p pipe.DataStream[int]) pipe.DataStream[int] {
		return p.FanOut(pipe.Params{Num: 3}).Filter(filter)
	}

	pl := pipelines.FromSource[int, string](
		ctx,
		&IntConsumer{num: 10, out: make(chan int, 10)},
		errChan,
	).Connect(connector).Map(mapFunc)

	for val := range pl.Out() {
		slog.Info("received simple pipeline output", slog.String("out", val))
	}

	// Output:
	// {"out":"I'm an even number: 0"}
	// {"out":"I'm an even number: 2"}
	// {"out":"I'm an even number: 4"}
	// {"out":"I'm an even number: 6"}
	// {"out":"I'm an even number: 8"}
}
