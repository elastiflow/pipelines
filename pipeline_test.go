package pipelines

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pipelineErrors "github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type consoleSender struct{}

func (p *consoleSender) send(v string) error {
	fmt.Println("published:", v)
	return nil
}

func TestIntegrationPipeline_Map(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		preprocess  []pipe.Processor[int]
		mapFunc     pipe.Transformer[int, string]
		postprocess []pipe.Processor[string]
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
			preprocess: []pipe.Processor[int]{func(p int) (int, error) {
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
			preprocess: []pipe.Processor[int]{func(p int) (int, error) {
				return p * 2, nil
			}},
			mapFunc: func(p int) (string, error) {
				return fmt.Sprintf("I'm a string %d", p), nil
			},
			postprocess: []pipe.Processor[string]{func(p string) (string, error) {
				return p + "!", nil
			}},
			wantStrings: []string{"I'm a string 2!", "I'm a string 4!", "I'm a string 6!", "I'm a string 8!", "I'm a string 10!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer := NewMockConsumer(tt.input)
			errChan := make(chan pipelineErrors.Error, len(tt.input))
			defer close(errChan)
			pipeline := FromSource[int, string](
				context.Background(),
				consumer,
				errChan,
			)

			for _, p := range tt.preprocess {
				pipeline = pipeline.Process(p)
			}

			transformed := pipeline.Map(tt.mapFunc)
			for _, p := range tt.postprocess {
				transformed = transformed.Process(p)
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
		process    pipe.Processor[int]
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
			consumer := NewMockConsumer(tt.input)
			errChan := make(chan pipelineErrors.Error, len(tt.input))
			defer close(errChan)
			pipeline := FromSource[int, int](
				context.Background(),
				consumer,
				errChan,
			).Process(tt.process)

			var gotOutput []int
			for v := range pipeline.Out() {
				gotOutput = append(gotOutput, v)
			}
			assert.ElementsMatch(t, tt.wantOutput, gotOutput)
		})
	}
}

func TestPipeline_Sink(t *testing.T) {
	type testCase struct {
		name            string
		sourceData      []int
		mockSenderSetup func(sender *mockSender[int])
		assertions      func(sender *mockSender[int], errs <-chan pipelineErrors.Error)
	}

	testCases := []testCase{
		{
			name:       "should sink messages from a standard source",
			sourceData: []int{1, 2, 3},
			mockSenderSetup: func(sender *mockSender[int]) {
				sender.On("send", mock.Anything).Return(nil)
			},
			assertions: func(sender *mockSender[int], errs <-chan pipelineErrors.Error) {
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
			assertions: func(sender *mockSender[int], errs <-chan pipelineErrors.Error) {
				assert.Len(t, errs, 1)
				sender.AssertCalled(t, "send", 3)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := NewMockConsumer(tc.sourceData)
			sender := newMockSender[int]()
			tc.mockSenderSetup(sender)

			errorsCh := make(chan pipelineErrors.Error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			pipeline := FromSource[int, string](ctx, consumer, errorsCh)
			pipeline.Sink(newMockPublisher[int](sender))
			tc.assertions(sender, pipeline.Errors())
		})
	}
}

func TestIntegrationPipeline_Tee(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     pipe.Processor[int]
		pipeProcess pipe.Processor[int]
		wantOutput  []int
	}{
		{
			name:  "simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			pipeProcess: func(v int) (int, error) {
				return v * 2, nil
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
			t.Parallel()
			errs := make(chan pipelineErrors.Error, len(tt.input)*3)
			defer close(errs)

			consumer := NewMockConsumer(tt.input)

			out1, out2 := FromSource[int, int](
				context.Background(),
				consumer,
				errs,
			).Process(tt.process).
				Tee(pipe.Params{BufferSize: len(tt.input) + 1})

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

		consumer := NewMockConsumer([]int{1, 2, 3})

		errors_ := make(chan pipelineErrors.Error, 1)
		defer close(errors_)

		source := FromSource[int, string](ctx, consumer, errors_).
			Map(func(i int) (string, error) {
				return string(rune('A' + i)), nil
			}).
			Process(func(s string) (string, error) {
				return "processed " + s, nil
			}).
			ToSource()

		go source.Consume(ctx, errors_)

		var consumedValues []string
		for v := range source.Out() {
			consumedValues = append(consumedValues, v)
		}
		assert.ElementsMatch(t, consumedValues, []string{"processed B", "processed C", "processed D"})

	})
}

// ExamplePipeline_Tee demonstrates how to create and use a simple pipeline
// that processes integer inputs by doubling their values and then tees the output.
func ExamplePipeline_Tee() {
	source := NewMockConsumer([]int{1, 2, 3, 4, 5})
	errChan := make(chan pipelineErrors.Error)
	defer close(errChan)

	process := func(p pipe.DataStream[int]) pipe.DataStream[int] {
		return p.Run(func(v int) (int, error) {
			return v * 2, nil
		})
	}

	// Create and open the pipeline
	pipeline := FromSource[int, int](context.Background(), source, errChan).
		With(process)

	out1, out2 := pipeline.Tee(pipe.Params{BufferSize: 5})
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

// ExamplePipeline_Map demonstrates how to create and use a simple pipeline
// that maps one type to another.
func ExamplePipeline_Map() {
	errChan := make(chan pipelineErrors.Error)
	defer close(errChan)

	source := NewMockConsumer([]int{1, 2, 3, 4, 5})
	mapFunc := func(p int) (string, error) {
		return fmt.Sprintf("Im a string now: %d", p), nil
	}

	pl := FromSource[int, string]( // Create a new Pipeline
		context.Background(),
		source,
		errChan,
	).Map(mapFunc)

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

// ExamplePipeline_Sink demonstrates how to create and use a simple pipeline that
// sinks the output of a source to a publisher
func ExamplePipeline_Sink() {
	errChan := make(chan pipelineErrors.Error)
	defer close(errChan)

	source := NewMockConsumer([]int{1, 2, 3, 4, 5})
	mapFunc := func(p int) (string, error) {
		return fmt.Sprintf("Im a string now: %d", p), nil
	}

	FromSource[int, string]( // Create a new Pipeline
		context.Background(),
		source,
		errChan,
	).Map(mapFunc).
		Sink(newMockPublisher[string](&consoleSender{}))

	// Output:
	// published: Im a string now: 1
	// published: Im a string now: 2
	// published: Im a string now: 3
	// published: Im a string now: 4
	// published: Im a string now: 5
}

// ExamplePipeline_ToSource demonstrates how to turn a pipeline into a source
// to be used in another pipeline.
func ExamplePipeline_ToSource() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	consumer := NewMockConsumer([]int{1, 2, 3})

	errors_ := make(chan pipelineErrors.Error, 1)
	defer close(errors_)

	source := FromSource[int, string](ctx, consumer, errors_).
		Map(func(i int) (string, error) {
			return string(rune('A' + i)), nil
		}).
		Process(func(s string) (string, error) {
			return "processed " + s, nil
		}).
		ToSource()

	go source.Consume(ctx, errors_)

	for v := range source.Out() {
		fmt.Println(v)
	}

	// Output:
	// processed B
	// processed C
	// processed D
}
