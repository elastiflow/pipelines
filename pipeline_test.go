package pipelines

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestIntegrationPipeline_Map(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		preprocess  []datastreams.ProcessFunc[int]
		mapFunc     datastreams.TransformFunc[int, string]
		postprocess []datastreams.ProcessFunc[string]
		wantStrings []string
	}{
		{
			name:  "simple map",
			input: []int{1, 2, 3, 4, 5},
			mapFunc: func(p int) (string, error) {
				return fmt.Sprintf("I'm a string %d", p), nil
			},
			wantStrings: []string{"I'm a string 1", "I'm a string 2", "I'm a string 3", "I'm a string 4", "I'm a string 5"},
		},
		{
			name:  "map with a preprocess ",
			input: []int{1, 2, 3, 4, 5},
			preprocess: []datastreams.ProcessFunc[int]{func(p int) (int, error) {
				return p * 2, nil
			}},
			mapFunc: func(p int) (string, error) {
				return fmt.Sprintf("I'm a string %d", p), nil
			},
			wantStrings: []string{"I'm a string 2", "I'm a string 4", "I'm a string 6", "I'm a string 8", "I'm a string 10"},
		},
		{
			name:  "map with a preprocess and postprocess",
			input: []int{1, 2, 3, 4, 5},
			preprocess: []datastreams.ProcessFunc[int]{func(p int) (int, error) {
				return p * 2, nil
			}},
			mapFunc: func(p int) (string, error) {
				return fmt.Sprintf("I'm a string %d", p), nil
			},
			postprocess: []datastreams.ProcessFunc[string]{func(p string) (string, error) {
				return p + "!", nil
			}},
			wantStrings: []string{"I'm a string 2!", "I'm a string 4!", "I'm a string 6!", "I'm a string 8!", "I'm a string 10!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer := NewMockSource(tt.input)
			errChan := make(chan error, len(tt.input))
			defer close(errChan)
			pipeline := New[int, string](
				context.Background(),
				consumer,
				errChan,
			)

			for _, p := range tt.preprocess {
				pipeline = pipeline.Process(p)
			}

			transformed := pipeline.Map(tt.mapFunc)
			for _, p := range tt.postprocess {
				transformed = transformed.Run(p)
			}

			var gotStrings []string
			for v := range transformed.Out() {
				gotStrings = append(gotStrings, v)
			}

			assert.ElementsMatch(t, tt.wantStrings, gotStrings)
		})
	}
}

func TestIntegrationPipeline_Out(t *testing.T) {
	tests := []struct {
		name       string
		input      []int
		process    datastreams.ProcessFunc[int]
		wantOutput []int
	}{
		{
			name:  "simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(p int) (int, error) {
				return p * 2, nil
			},
			wantOutput: []int{2, 4, 6, 8, 10},
		},
		{
			name:  "process with error",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
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
			consumer := NewMockSource(tt.input)
			errChan := make(chan error, len(tt.input))
			defer close(errChan)
			pipeline := New[int, int](
				context.Background(),
				consumer,
				errChan,
			).Process(tt.process)

			var gotOutput []int
			for v := range pipeline.In() {
				gotOutput = append(gotOutput, v)
			}
			assert.ElementsMatch(t, tt.wantOutput, gotOutput)
		})
	}
}

func TestIntegrationPipeline_Sink(t *testing.T) {
	type testCase struct {
		name            string
		sourceData      []int
		mockSenderSetup func(sender *mockSender[int])
		assertions      func(sender *mockSender[int], errs <-chan error)
	}

	testCases := []testCase{
		{
			name:       "should sink messages from a standard source",
			sourceData: []int{1, 2, 3},
			mockSenderSetup: func(sender *mockSender[int]) {
				sender.On("send", mock.Anything).Return(nil)
			},
			assertions: func(sender *mockSender[int], errs <-chan error) {
				sender.AssertCalled(t, "send", mock.Anything)
			},
		},
		{
			name:       "should sink messages from a source with errors",
			sourceData: []int{1, 2, 3},
			mockSenderSetup: func(sender *mockSender[int]) {
				sender.On("send", mock.Anything).Return(nil).Once()
				sender.On("send", mock.Anything).Return(errors.New("error on 2")).Once()
				sender.On("send", mock.Anything).Return(nil).Once()
			},
			assertions: func(sender *mockSender[int], errs <-chan error) {
				assert.Len(t, errs, 0)
				sender.AssertCalled(t, "send", 1)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			source := NewMockSource(tc.sourceData)
			sender := newMockSender[int]()
			tc.mockSenderSetup(sender)

			errorsCh := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			pipeline := New[int, int](ctx, source, errorsCh).Start(
				func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
					return p.Run(
						func(v int) (int, error) {
							return v, nil
						},
					)
				},
			)
			_ = pipeline.Sink(newMockSinker[int](sender))
			tc.assertions(sender, pipeline.Errors())
		})
	}
}

func TestIntegrationPipeline_Tee(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     StreamFunc[int, int]
		pipeProcess StreamFunc[int, int]
		wantOutput  []int
	}{
		{
			name:  "simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
			pipeProcess: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.Run(
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
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.Run(
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
			errs := make(chan error, len(tt.input)*3)
			defer close(errs)
			consumer := NewMockSource(tt.input)
			out1, out2 := New[int, int](
				context.Background(),
				consumer,
				errs,
			).Start(
				tt.process,
			).Tee(datastreams.Params{BufferSize: len(tt.input) + 1})
			var gotOutput1, gotOutput2 []int
			for v := range out1.Out() {
				gotOutput1 = append(gotOutput1, v)
			}
			for v := range out2.Out() {
				gotOutput2 = append(gotOutput2, v)
			}
			assert.ElementsMatch(t, tt.wantOutput, gotOutput1)
			assert.ElementsMatch(t, tt.wantOutput, gotOutput2)
		})
	}
}

func TestPipeline_ToSource(t *testing.T) {
	t.Run("should create a source from all the streams", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		consumer := NewMockSource([]int{1, 2, 3})

		errs := make(chan error, 1)
		defer close(errs)

		streamProc := func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
			return datastreams.Map(
				p,
				func(i int) (string, error) { return string(rune('A' + i)), nil },
			).Run(
				func(s string) (string, error) {
					return "processed " + s, nil
				},
			)
		}
		source := New[int, string](ctx, consumer, errs).Start(
			streamProc,
		).ToSource()
		var consumedValues []string
		for v := range source.Source(ctx, errs).Out() {
			consumedValues = append(consumedValues, v)
		}
		assert.ElementsMatch(t, consumedValues, []string{"processed B", "processed C", "processed D"})

	})
}

// ExamplePipeline_Stream demonstrates how to create and use a simple Pipeline
// that processes integer inputs by doubling their values and then returns an output datastreams.DataStream.
func ExamplePipeline_Stream() {
	errChan := make(chan error, 3)
	defer close(errChan)
	inChan := make(chan int, 5)

	// Create and open the pipeline
	ds := New[int, int](
		context.Background(),
		sources.FromChannel(inChan, sources.Params{BufferSize: 5}),
		errChan,
	).Stream(
		func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
			return p.Run(
				func(v int) (int, error) {
					return v * 2, nil
				},
				datastreams.Params{BufferSize: 5},
			)
		},
	)

	// Send values to the pipeline's source channel
	go func() {
		arr := []int{1, 2, 3, 4, 5}
		for _, val := range arr {
			inChan <- val
		}
		close(inChan)
	}()

	for out := range ds.Out() {
		fmt.Println("out:", out)
	}

	// Output:
	// out: 2
	// out: 4
	// out: 6
	// out: 8
	// out: 10
}

// ExamplePipeline_Start demonstrates how to create and use a simple Pipeline
// that processes integer inputs by doubling their values and then returns an output Pipeline.
func ExamplePipeline_Start() {
	errChan := make(chan error, 3)
	defer close(errChan)
	inChan := make(chan int, 5)

	// Create and open the pipeline
	pl := New[int, int](
		context.Background(),
		sources.FromChannel(inChan, sources.Params{BufferSize: 5}),
		errChan,
	).Start(
		func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
			return p.Run(
				func(v int) (int, error) {
					return v * 2, nil
				},
				datastreams.Params{BufferSize: 5},
			)
		},
	)

	// Send values to the pipeline's source channel
	go func() {
		arr := []int{1, 2, 3, 4, 5}
		for _, val := range arr {
			inChan <- val
		}
		close(inChan)
	}()

	for out := range pl.Out() {
		fmt.Println("out:", out)
	}

	// Output:
	// out: 2
	// out: 4
	// out: 6
	// out: 8
	// out: 10
}

// ExamplePipeline_Tee demonstrates how to create and use a simple Pipeline
// that processes integer inputs by doubling their values and then tees the output.
func ExamplePipeline_Tee() {
	errChan := make(chan error)
	defer close(errChan)

	// Create and open the pipeline
	out1, out2 := New[int, int](
		context.Background(),
		sources.FromArray([]int{1, 2, 3, 4, 5}),
		errChan,
	).Start(
		func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
			return p.Run(func(v int) (int, error) {
				return v * 2, nil
			})
		},
	).Tee(datastreams.Params{BufferSize: 5})

	// Collect and print the results from both outputs
	for out := range out1.Out() {
		fmt.Println("out1:", out)
	}
	for out := range out2.Out() {
		fmt.Println("out2:", out)
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

// ExamplePipeline_Map demonstrates how to create and use a simple Pipeline
// that maps one type to another.
func ExamplePipeline_Map() {
	errChan := make(chan error)
	defer close(errChan)

	pl := New[int, string]( // Create a new Pipeline
		context.Background(),
		sources.FromArray([]int{1, 2, 3, 4, 5}),
		errChan,
	).Map(
		func(p int) (string, error) {
			return fmt.Sprintf("Im a string now: %d", p), nil
		},
	)

	for out := range pl.Out() { // Read Pipeline output
		fmt.Println("out:", out)
	}

	// Output:
	// out: Im a string now: 1
	// out: Im a string now: 2
	// out: Im a string now: 3
	// out: Im a string now: 4
	// out: Im a string now: 5
}

// ExamplePipeline_Sink demonstrates how to create and use a simple Pipeline that
// sinks the output to a sinks.ToChannel
func ExamplePipeline_Sink() {
	errChan := make(chan error)
	outChan := make(chan string)
	go func() {
		for out := range outChan { // Read Pipeline output
			fmt.Println("published:", out)
		}
	}()

	if err := New[int, string]( // Create a new Pipeline
		context.Background(),
		sources.FromArray([]int{1, 2, 3, 4, 5}),
		errChan,
	).Start(
		func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
			return datastreams.Map(
				p,
				func(p int) (string, error) { return fmt.Sprintf("Im a string now: %d", p), nil },
			)
		},
	).Sink(
		sinks.ToChannel(outChan),
	); err != nil {
		fmt.Println("error sinking:", err)
	}

	// Output:
	// published: Im a string now: 1
	// published: Im a string now: 2
	// published: Im a string now: 3
	// published: Im a string now: 4
	// published: Im a string now: 5
}

// ExamplePipeline_ToSource demonstrates how to turn a Pipeline into a source
// to be used in another Pipeline.
func ExamplePipeline_ToSource() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	errChan := make(chan error, 1)
	defer close(errChan)
	source := New[int, string](
		ctx,
		sources.FromArray([]int{1, 2, 3}),
		errChan,
	).Start(
		func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
			return datastreams.Map(
				p,
				func(i int) (string, error) { return string(rune('A' + i)), nil },
			).Run(
				func(s string) (string, error) {
					return "processed " + s, nil
				},
			)
		},
	).ToSource()
	for v := range source.Source(ctx, errChan).Out() {
		fmt.Println(v)
	}

	// Output:
	// processed B
	// processed C
	// processed D
}
