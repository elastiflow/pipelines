package bench

import (
	"testing"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

func BenchmarkPipelineOpen(b *testing.B) {
	benchmarks := []struct {
		name        string
		pipeProcess pipe.ProcessFunc[int]
		process     pipelines.ProcessFunc[int]
		fanNum      int
	}{
		{
			name: "fast pipeline",
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
			process: func(p pipe.Pipe[int], params *pipe.Params) pipe.Pipe[int] {
				return p.Run("testProcess", params)
			},
			fanNum: 1,
		},
		{
			name: "fast pipeline fanOut-5",
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
			process: func(p pipe.Pipe[int], params *pipe.Params) pipe.Pipe[int] {
				return p.FanOut(params).Run("testProcess", params)
			},
			fanNum: 5,
		},
		{
			name: "slow pipeline",
			pipeProcess: func(v int) (int, error) {
				time.Sleep(2 * time.Millisecond)
				return v * 2, nil
			},
			process: func(p pipe.Pipe[int], params *pipe.Params) pipe.Pipe[int] {
				return p.FanOut(params).Run("testProcess", params)
			},
			fanNum: 1,
		},
		{
			name: "slow pipeline fanOut-5",
			pipeProcess: func(v int) (int, error) {
				time.Sleep(2 * time.Millisecond)
				return v * 2, nil
			},
			process: func(p pipe.Pipe[int], params *pipe.Params) pipe.Pipe[int] {
				return p.FanOut(params).Run("testProcess", params)
			},
			fanNum: 5,
		},
		{
			name: "slow pipeline fanOut-5 buffered-5",
			pipeProcess: func(v int) (int, error) {
				time.Sleep(2 * time.Millisecond)
				return v * 2, nil
			},
			process: func(p pipe.Pipe[int], params *pipe.Params) pipe.Pipe[int] {
				return p.FanOut(params).Run("testProcess", params).FanIn(&pipe.Params{BufferSize: 5})
			},
			fanNum: 5,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			inputChan := make(chan int)
			defer close(inputChan)
			errChan := make(chan error, 1)
			defer close(errChan)
			props := pipelines.NewProps[int]( // Create new Pipeline properties
				pipe.ProcessRegistry[int]{
					"testProcess": bm.pipeProcess,
				},
				inputChan,
				errChan,
				bm.fanNum,
			)
			pipeline := pipelines.New(
				props,
				bm.process,
			)
			go func(pl pipelines.Pipeline[int]) {
				for range pl.Open(nil) {
				}
			}(*pipeline)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				inputChan <- i
			}
		})
	}
}
