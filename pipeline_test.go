package pipelines

import (
	"fmt"
	"testing"

	"github.com/elastiflow/pipelines/pipe"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationPipeline_Open(t *testing.T) {
	tests := []struct {
		name       string
		input      []int
		process    ProcessFunc[int]
		wantOutput []int
	}{
		{
			name:  "simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
				return p.OrDone().FanOut(
					pipe.Params{
						Num: 2,
					},
				).Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
			wantOutput: []int{2, 4, 6, 8, 10},
		},
		{
			name:  "process with error",
			input: []int{1, 2, 3, 4, 5},
			process: func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
				return p.OrDone().FanOut(
					pipe.Params{
						Num: 2,
					},
				).Run(
					func(v int) (int, error) {
						if v%2 == 0 {
							return 0, fmt.Errorf("even number error")
						}
						return v, nil
					},
				)
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
			pipeline := New(
				inputChan,
				errChan,
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

func TestIntegrationPipeline_Tee(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     ProcessFunc[int]
		pipeProcess pipe.ProcessorFunc[int]
		wantOutput  []int
	}{
		{
			name:  "simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
				return p.OrDone().FanOut(
					pipe.Params{
						Num: 2,
					},
				).Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
			},
			wantOutput: []int{2, 4, 6, 8, 10},
		},
		{
			name:  "process with error",
			input: []int{1, 2, 3, 4, 5},
			process: func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
				return p.OrDone().FanOut(
					pipe.Params{
						Num: 2,
					},
				).Run(
					func(v int) (int, error) {
						if v%2 == 0 {
							return 0, fmt.Errorf("even number error")
						}
						return v, nil
					},
				)
			},
			wantOutput: []int{1, 0, 3, 0, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inputChan := make(chan int, len(tt.input)*3)
			for _, v := range tt.input {
				inputChan <- v
			}
			errChan := make(chan error, len(tt.input)*3)
			close(inputChan)
			defer close(errChan)
			pipeline := New(
				inputChan,
				errChan,
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

// ExamplePipeline_Open demonstrates how to create and use a simple pipeline
// that processes integer inputs by doubling their values.
func ExamplePipeline_Open() {
	inChan := make(chan int)
	errChan := make(chan error)
	defer func() {
		close(inChan)
		close(errChan)
	}()

	// Seed the input channel
	go func() {
		for i := 1; i <= 5; i++ {
			inChan <- i
		}
	}()

	// Define a simple process function that doubles the input values
	process := func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
		return p.Run(func(v int) (int, error) {
			return v * 2, nil
		})
	}

	// Create and open the pipeline
	pipeline := New(inChan, errChan, process)
	c := 0
	for out := range pipeline.Open() {
		fmt.Println(out)
		c++
		if c == 5 {
			break
		}
	}

	// Output:
	// 2
	// 4
	// 6
	// 8
	// 10
}

// ExamplePipeline_Tee demonstrates how to create and use a simple pipeline
// that processes integer inputs by doubling their values and then tees the output.
func ExamplePipeline_Tee() {
	inChan := make(chan int)
	errChan := make(chan error)
	defer func() {
		close(inChan)
		close(errChan)
	}()

	// Seed the input channel
	go func() {
		for i := 1; i <= 5; i++ {
			inChan <- i
		}
	}()

	// Define a simple process function that doubles the input values
	process := func(p pipe.Pipe[int, int]) pipe.Pipe[int, int] {
		return p.Run(func(v int) (int, error) {
			return v * 2, nil
		})
	}

	// Create and open the pipeline
	pipeline := New(inChan, errChan, process)
	out1, out2 := pipeline.Tee(pipe.Params{BufferSize: 5})

	// Collect and print the results from both outputs
	c := 0
	for out := range out1 {
		fmt.Println("out1:", out)
		c++
		if c == 5 {
			break
		}
	}
	x := 0
	for out := range out2 {
		fmt.Println("out2:", out)
		x++
		if x == 5 {
			break
		}
	}

	// Output:
	// out1: 2
	// out1: 4
	// out1: 6
	// out1: 8
	// out1: 10
	// out2: 2
	// out2: 4
	// out2: 6
	// out2: 8
	// out2: 10
}
