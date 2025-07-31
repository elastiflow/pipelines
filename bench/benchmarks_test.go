package bench

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
)

func BenchmarkDataStream(b *testing.B) {
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

func BenchmarkKeyedDataStream(b *testing.B) {
	keyFunc := func(v int) int { return v % 10 }
	benchmarks := []struct {
		name    string
		process func(v datastreams.DataStream[int]) datastreams.DataStream[int]
	}{
		{
			name: "fast keyed pipeline",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				)
				return kds.Run(func(v int) (int, error) { return v * 2, nil })
			},
		},
		{
			name: "fast keyed pipeline watermark/time",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				).WithWatermarkGenerator(nil).WithTimeMarker(nil)
				return kds.Run(func(v int) (int, error) { return v * 2, nil })
			},
		},
		{
			name: "fast keyed pipeline fanOut-5",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				)
				return kds.FanOut(
					datastreams.Params{Num: 5},
				).Run(func(v int) (int, error) { return v * 2, nil })
			},
		},
		{
			name: "slow keyed pipeline",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				)
				return kds.Run(func(v int) (int, error) {
					time.Sleep(2 * time.Millisecond)
					return v * 2, nil
				})
			},
		},
		{
			name: "slow keyed pipeline fanOut-5",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				)
				return kds.FanOut(
					datastreams.Params{Num: 5},
				).Run(func(v int) (int, error) {
					time.Sleep(2 * time.Millisecond)
					return v * 2, nil
				})
			},
		},
		{
			name: "slow keyed pipeline fanOut-5 buffered-5",
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				kds := datastreams.KeyBy(
					p,
					keyFunc,
					datastreams.Params{},
				)
				return kds.FanOut(
					datastreams.Params{Num: 5},
				).Run(func(v int) (int, error) {
					time.Sleep(2 * time.Millisecond)
					return v * 2, nil
				}).FanIn(
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

func BenchmarkPipeline(b *testing.B) {
	benchmarks := []struct {
		name    string
		process func(p *pipelines.Pipeline[int, int]) <-chan int
	}{
		{
			name: "Map",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				ds := p.Map(func(v int) (int, error) { return v * 2, nil })
				return ds.Out()
			},
		},
		{
			name: "Expand",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				ds := p.Expand(func(v int) ([]int, error) { return []int{v, v + 1}, nil })
				return ds.Out()
			},
		},
		{
			name: "Stream/Out",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				p.Stream(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return datastreams.Map(ds, func(v int) (int, error) { return v * 2, nil })
				})
				return p.Out()
			},
		},
		{
			name: "Start chain",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				p.Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return datastreams.Map(ds, func(v int) (int, error) { return v * 2, nil })
				}).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return datastreams.Map(ds, func(v int) (int, error) { return v + 1, nil })
				})
				return p.Out()
			},
		},
		{
			name: "Process",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				np := p.Process(func(v int) (int, error) { return v * 3, nil })
				return np.In()
			},
		},
		{
			name: "ToSource cascade",
			process: func(p *pipelines.Pipeline[int, int]) <-chan int {
				p.Stream(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return datastreams.Map(ds, func(v int) (int, error) { return v * 2, nil })
				})
				nextPl := pipelines.New[int, int](context.Background(), p.ToSource(), make(chan error, 1))
				nextPl.Stream(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return datastreams.Map(ds, func(v int) (int, error) { return v + 1, nil })
				})
				return nextPl.Out()
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			errChan := make(chan error, 1)
			pl := pipelines.New[int, int](
				context.Background(),
				NewBenchmarkConsumer(b.N),
				errChan,
			)
			go func() {
				for range pl.Errors() {
				}
			}()
			out := bm.process(pl)
			for range out {
			}
			pl.Close()
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
