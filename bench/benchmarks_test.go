package bench

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
)

func BenchmarkPipelineOpen(b *testing.B) {
	benchmarks := []struct {
		name    string
		process func(v datastreams.DataStream[int]) datastreams.DataStream[int]
	}{
		{
			name: "fast pipeline",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
					datastreams.Params{},
				)
			},
		},
		{
			name: "fast pipeline fanOut-5",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.FanOut(
					datastreams.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
		},
		{
			name: "slow pipeline",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
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
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.FanOut(
					datastreams.Params{Num: 5},
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
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.FanOut(
					datastreams.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
				).FanIn(
					datastreams.Params{BufferSize: 5},
				)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			errChan := make(chan error, 1)
			defer close(errChan)
			pipeline := pipelines.New[int, int](
				context.Background(),
				NewBenchmarkConsumer(b.N),
				errChan,
			)
			for range pipeline.Stream(bm.process).Out() {
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

// Source reads the payload from the HTTP endpoint and sends it to the output channel
func (c *BenchmarkConsumer) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[int] {
	outChan := make(chan int, c.num)
	ds := datastreams.New[int](ctx, outChan, errSender)
	go func(outSender chan<- int) {
		defer close(outSender)
		for i := 0; i < c.num; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				outSender <- i
			}
		}
	}(outChan)
	return ds
}
