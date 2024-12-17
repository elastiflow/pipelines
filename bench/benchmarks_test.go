package bench

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
)

func BenchmarkPipelineOpen(b *testing.B) {
	benchmarks := []struct {
		name    string
		process func(v pipe.DataStream[int]) pipe.DataStream[int]
	}{
		{
			name: "fast pipeline",
			process: func(p pipe.DataStream[int]) pipe.DataStream[int] {
				return p.Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
					pipe.Params{},
				)
			},
		},
		{
			name: "fast pipeline fanOut-5",
			process: func(p pipe.DataStream[int]) pipe.DataStream[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
		},
		{
			name: "slow pipeline",
			process: func(p pipe.DataStream[int]) pipe.DataStream[int] {
				return p.Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
				)
			},
		},
		{
			name: "slow pipeline fanOut-5",
			process: func(p pipe.DataStream[int]) pipe.DataStream[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
				)
			},
		},
		{
			name: "slow pipeline fanOut-5 buffered-5",
			process: func(p pipe.DataStream[int]) pipe.DataStream[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
				).FanIn(
					pipe.Params{BufferSize: 5},
				)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			errChan := make(chan errors.Error, 1)
			defer close(errChan)
			pipeline := pipelines.FromSource[int, int](
				context.Background(),
				NewBenchmarkConsumer(b.N),
				errChan,
			).With(bm.process)
			for range pipeline.Out() {
			}
		})
	}
}

type BenchmarkConsumer struct {
	num int
	out chan int
}

func NewBenchmarkConsumer(num int) *BenchmarkConsumer {
	return &BenchmarkConsumer{
		num: num,
		out: make(chan int, num),
	}
}

func (c *BenchmarkConsumer) Consume(ctx context.Context, errs chan<- errors.Error) {
	defer close(c.out)
	for i := 0; i < c.num; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			c.out <- i
		}
	}
}

func (c *BenchmarkConsumer) Out() <-chan int {
	return c.out
}
