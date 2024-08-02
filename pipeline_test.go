package pipelines

import (
	"fmt"
	"testing"

	"github.com/elastiflow/pipelines/pipe"
	"github.com/stretchr/testify/assert"
)

func exProcess[T any](p pipe.Pipe[T], params *pipe.Params) pipe.Pipe[T] {
	return p.OrDone(nil).FanOut(params).Run("testProcess", nil)
}

func TestIntegrationPipelineOpen(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     ProcessFunc[int]
		pipeProcess pipe.ProcessFunc[int]
		fanNum      int
		wantOutput  []int
	}{
		{
			name:    "simple process",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
			fanNum:     1,
			wantOutput: []int{2, 4, 6, 8, 10},
		},
		{
			name:    "process with error",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				if v%2 == 0 {
					return 0, fmt.Errorf("even number error")
				}
				return v, nil
			},
			fanNum:     1,
			wantOutput: []int{1, 0, 3, 0, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputChan := make(chan int, len(tt.input))
			for _, v := range tt.input {
				inputChan <- v
			}
			close(inputChan)
			errChan := make(chan error, len(tt.input))
			defer close(errChan)
			props := NewProps[int]( // Create new Pipeline properties
				pipe.ProcessRegistry[int]{
					"testProcess": tt.pipeProcess,
				},
				inputChan,
				errChan,
				tt.fanNum,
			)
			pipeline := New(
				props,
				tt.process,
			)
			var gotOutput []int
			for v := range pipeline.Open(&pipe.Params{Num: tt.fanNum}) {
				gotOutput = append(gotOutput, v)
			}
			assert.ElementsMatch(t, tt.wantOutput, gotOutput)
		})
	}
}

func TestIntegrationPipelineTee(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     ProcessFunc[int]
		pipeProcess pipe.ProcessFunc[int]
		fanNum      int
		wantOutput  []int
	}{
		{
			name:    "simple process",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
			fanNum:     2,
			wantOutput: []int{2, 4, 6, 8, 10},
		},
		{
			name:    "process with error",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				if v%2 == 0 {
					return 0, fmt.Errorf("even number error")
				}
				return v, nil
			},
			fanNum:     2,
			wantOutput: []int{1, 0, 3, 0, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputChan := make(chan int, len(tt.input)*3)
			for _, v := range tt.input {
				inputChan <- v
			}
			errChan := make(chan error, len(tt.input)*3)
			close(inputChan)
			defer close(errChan)
			props := NewProps[int]( // Create new Pipeline properties
				pipe.ProcessRegistry[int]{
					"testProcess": tt.pipeProcess,
				},
				inputChan,
				errChan,
				tt.fanNum,
			)
			pipeline := New(
				props,
				tt.process,
			)
			out1, out2 := pipeline.Tee(&pipe.Params{Num: tt.fanNum, BufferSize: len(tt.input) + 1})
			var gotOutput1, gotOutput2 []int
			for v := range out1 {
				gotOutput1 = append(gotOutput1, v)
			}
			for v := range out2 {
				gotOutput2 = append(gotOutput2, v)
			}
			assert.ElementsMatch(t, tt.wantOutput, gotOutput1)
			assert.ElementsMatch(t, tt.wantOutput, gotOutput2)
		})
	}
}
