package pipelines

import (
	"fmt"
	"testing"

	"github.com/elastiflow/pipelines/pipe"
	"github.com/stretchr/testify/assert"
)

func exProcess[T any](p pipe.Pipe[T]) pipe.Pipe[T] {
	return p.OrDone(
		pipe.DefaultParams(),
	).FanOut(
		pipe.Params{
			Num: 2,
		},
	).Run(
		"testProcess",
		pipe.DefaultParams(),
	)
}

func TestIntegrationPipelineOpen(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     ProcessFunc[int]
		pipeProcess pipe.ProcessFunc[int]
		wantOutput  []int
	}{
		{
			name:    "simple process",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
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
			)
			pipeline := New(
				props,
				tt.process,
			)
			var gotOutput []int
			for v := range pipeline.Open() {
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
		wantOutput  []int
	}{
		{
			name:    "simple process",
			input:   []int{1, 2, 3, 4, 5},
			process: exProcess[int],
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
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
			)
			pipeline := New(
				props,
				tt.process,
			)
			out1, out2 := pipeline.Tee(pipe.Params{BufferSize: len(tt.input) + 1})
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
