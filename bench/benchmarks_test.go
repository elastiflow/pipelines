package bench

import (
	"testing"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

func BenchmarkPipelineOpen(b *testing.B) {
	benchmarks := []struct {
		name    string
		process pipelines.ProcessFunc[int]
	}{
		{
			name: "fast pipeline",
			process: func(p pipe.Pipe[int]) pipe.Pipe[int] {
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
			process: func(p pipe.Pipe[int]) pipe.Pipe[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
					pipe.DefaultParams(),
				)
			},
		},
		{
			name: "slow pipeline",
			process: func(p pipe.Pipe[int]) pipe.Pipe[int] {
				return p.Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
					pipe.DefaultParams(),
				)
			},
		},
		{
			name: "slow pipeline fanOut-5",
			process: func(p pipe.Pipe[int]) pipe.Pipe[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
					pipe.DefaultParams(),
				)
			},
		},
		{
			name: "slow pipeline fanOut-5 buffered-5",
			process: func(p pipe.Pipe[int]) pipe.Pipe[int] {
				return p.FanOut(
					pipe.Params{Num: 5},
				).Run(
					func(v int) (int, error) {
						time.Sleep(2 * time.Millisecond)
						return v * 2, nil
					},
					pipe.DefaultParams(),
				).FanIn(
					pipe.Params{BufferSize: 5},
				)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			inputChan := make(chan int)
			defer close(inputChan)
			errChan := make(chan error, 1)
			defer close(errChan)
			props := pipelines.NewProps[int]( // Create new Pipeline properties
				inputChan,
				errChan,
			)
			pipeline := pipelines.New(
				props,
				bm.process,
			)
			go func(pl pipelines.Pipeline[int]) {
				for range pl.Open() {
				}
			}(*pipeline)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				inputChan <- i
			}
		})
	}
}
