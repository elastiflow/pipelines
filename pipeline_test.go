package pipelines

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestIntegrationPipeline_Map tests using Pipeline.Map with optional pre/post processing.
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

			// Apply any pre-processing steps
			for _, p := range tt.preprocess {
				pipeline = pipeline.Process(p)
			}

			// Map transform
			transformed := pipeline.Map(tt.mapFunc)

			// Post-processing on the DataStream
			for _, p := range tt.postprocess {
				transformed = transformed.Run(p)
			}

			// Gather the results
			var gotStrings []string
			for v := range transformed.Out() {
				gotStrings = append(gotStrings, v)
			}

			assert.ElementsMatch(t, tt.wantStrings, gotStrings)
		})
	}
}

// TestIntegrationPipeline_Expand tests using Pipeline.Expand with optional pre/post processing.
func TestIntegrationPipeline_Expand(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		preprocess  []datastreams.ProcessFunc[int]
		expandFunc  datastreams.ExpandFunc[int, string]
		postprocess []datastreams.ProcessFunc[string]
		wantStrings []string
	}{
		{
			name:  "simple expansion",
			input: []int{1, 2, 3},
			expandFunc: func(p int) ([]string, error) {
				return []string{
					fmt.Sprintf("%d00", p),
					fmt.Sprintf("%d01", p),
					fmt.Sprintf("%d02", p),
				}, nil
			},
			wantStrings: []string{"100", "101", "102", "200", "201", "202", "300", "301", "302"},
		},
		{
			name:  "expansion with a preprocess",
			input: []int{1, 2, 3},
			preprocess: []datastreams.ProcessFunc[int]{func(p int) (int, error) {
				return p * 2, nil
			}},
			expandFunc: func(p int) ([]string, error) {
				return []string{
					fmt.Sprintf("%d00", p),
					fmt.Sprintf("%d01", p),
					fmt.Sprintf("%d02", p),
				}, nil
			},
			wantStrings: []string{"200", "201", "202", "400", "401", "402", "600", "601", "602"},
		},
		{
			name:  "expansion with a preprocess and postprocess",
			input: []int{1, 2, 3},
			preprocess: []datastreams.ProcessFunc[int]{func(p int) (int, error) {
				return p * 2, nil
			}},
			expandFunc: func(p int) ([]string, error) {
				return []string{
					fmt.Sprintf("%d01", p),
					fmt.Sprintf("%d02", p),
				}, nil
			},
			postprocess: []datastreams.ProcessFunc[string]{func(p string) (string, error) {
				return "Coding " + p, nil
			}},
			wantStrings: []string{"Coding 201", "Coding 202", "Coding 401", "Coding 402", "Coding 601", "Coding 602"},
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

			// Apply any pre-processing steps
			for _, p := range tt.preprocess {
				pipeline = pipeline.Process(p)
			}

			// Expand inputs into multiple outputs
			transformed := pipeline.Expand(tt.expandFunc)

			// Post-processing on the DataStream
			for _, p := range tt.postprocess {
				transformed = transformed.Run(p)
			}

			// Gather the results
			var gotStrings []string
			for v := range transformed.Out() {
				gotStrings = append(gotStrings, v)
			}

			assert.ElementsMatch(t, tt.wantStrings, gotStrings)
		})
	}
}

// TestIntegrationPipeline_Out demonstrates usage of pipeline.Out to read processed data.
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

// TestIntegrationPipeline_Sink checks using a Sinker to consume pipeline outputs.
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
				assert.Len(t, errs, 0) // no errors left in channel after sink
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

// TestIntegrationPipeline_Tee checks splitting the pipeline output with Tee().
func TestIntegrationPipeline_Tee(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		process     StreamFunc[int, int]
		pipeProcess StreamFunc[int, int] // Not used in the test, but left for clarity
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

// TestIntegrationPipeline_Copy checks copying the pipeline output to multiple streams.
func TestIntegrationPipeline_Copy(t *testing.T) {
	tests := []struct {
		name       string
		input      []int
		process    StreamFunc[int, int]
		wantOutput []int
		copies     int
	}{
		{
			name:  "simple process is applied to all copies",
			input: []int{1, 2, 3, 4, 5},
			process: func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
				return p.Run(
					func(v int) (int, error) {
						return v * 2, nil
					},
				)
			},
			wantOutput: []int{2, 4, 6, 8, 10},
			copies:     3,
		},
		{
			name:  "returns single output with error handling if no copies are needed",
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
			copies:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			errs := make(chan error, len(tt.input)*3)
			defer close(errs)
			pls := New[int, int](
				context.Background(),
				sources.FromArray(tt.input),
				errs,
			).Copy(tt.copies).
				Start(tt.process)

			collectOut := func(got []int, out <-chan int) []int {
				for v := range out {
					got = append(got, v)
				}
				return got
			}

			wg := sync.WaitGroup{}
			wg.Add(pls.Count())
			results := make([][]int, tt.copies)
			for i := 0; i < pls.Count(); i++ {
				results[i] = make([]int, 0, len(tt.input))
				go func() {
					defer wg.Done()
					results[i] = collectOut(results[i], pls[i].Out())
				}()
			}

			wg.Wait()
			for i := 0; i < tt.copies; i++ {
				assert.ElementsMatch(t, tt.wantOutput, results[i])
			}
		})
	}
}

// TestPipeline_ToSource verifies converting a pipeline's sink to a sourcer for another pipeline.
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
		assert.ElementsMatch(t, []string{"processed B", "processed C", "processed D"}, consumedValues)
	})
}

// ExampleNew demonstrates a minimal pipeline creation and usage.
func ExampleNew() {
	errChan := make(chan error, 3)
	defer close(errChan)
	inChan := make(chan int, 5)

	// Create a new Pipeline with int -> int
	pl := New[int, int](
		context.Background(),
		sources.FromChannel(inChan),
		errChan,
	)

	// Provide data on inChan
	go func() {
		for i := 1; i <= 5; i++ {
			inChan <- i
		}
		close(inChan)
	}()

	// Process the data: multiply by 2
	pl.Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
		return ds.Run(func(v int) (int, error) {
			return v * 2, nil
		})
	})

	// Read pipeline output until closed
	for val := range pl.Out() {
		fmt.Println("Result:", val)
	}
	// Output:
	// Result: 2
	// Result: 4
	// Result: 6
	// Result: 8
	// Result: 10
}

// ExamplePipeline_Stream demonstrates how to create and use a simple Pipeline
// that processes integer inputs by doubling their values and then returns an output DataStream.
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
		for _, val := range []int{1, 2, 3, 4, 5} {
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
		for _, val := range []int{1, 2, 3, 4, 5} {
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
// that processes integer inputs by doubling their values and then tees the output
// into two distinct streams.
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
// that maps one type (int) to another (string).
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
// sinks the output to a sinks.ToChannel.
func ExamplePipeline_Sink() {
	errChan := make(chan error)
	outChan := make(chan string)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		for out := range outChan { // Read Pipeline output
			fmt.Println("out:", out)
			wg.Done()
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
	wg.Wait()

	// Output:
	// out: Im a string now: 1
	// out: Im a string now: 2
	// out: Im a string now: 3
	// out: Im a string now: 4
	// out: Im a string now: 5
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

// TestIntegrationPipeline_Close verifies that closing the pipeline cancels everything quickly.
func TestIntegrationPipeline_Close(t *testing.T) {
	in := make(chan int)
	errChan := make(chan error, 10)
	p := New[int, int](context.Background(), sources.FromChannel(in), errChan).
		Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
			return ds.Run(func(v int) (int, error) {
				return v * 2, nil
			})
		})

	p.Close() // Cancel the pipeline immediately

	// Attempt to read from out
	select {
	case _, ok := <-p.Out():
		_ = ok // doesn't matter, but we want to see if channel closed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Pipeline output did not close after pipeline.Close() call")
	}
}

func TestIntegrationPipeline_Wait(t *testing.T) {
	tests := []struct {
		name            string
		input           []int
		cancelContext   bool
		wantOutput      []int
		expectWaitToEnd bool
	}{
		{
			name:            "normal completion",
			input:           []int{1, 2, 3, 4, 5},
			cancelContext:   false,
			wantOutput:      []int{2, 4, 6, 8, 10},
			expectWaitToEnd: true,
		},
		{
			name:            "context canceled before consuming all input",
			input:           []int{1, 2, 3, 4, 5},
			cancelContext:   true,
			wantOutput:      []int{}, // We won't reliably get output because context is canceled
			expectWaitToEnd: true,
		},
		{
			name:            "no input (empty source)",
			input:           []int{},
			cancelContext:   false,
			wantOutput:      []int{},
			expectWaitToEnd: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFn := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFn()

			errChan := make(chan error, len(tt.input)*2)

			// Create the pipeline. It processes each input by multiplying by 2.
			pipeline := New[int, int](ctx, NewMockSource(tt.input), errChan).
				Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
					return ds.Run(func(v int) (int, error) {
						return v * 2, nil
					})
				})

			// Optionally cancel the context to simulate an abrupt stop.
			if tt.cancelContext {
				cancelFn()
			}

			// We'll collect the final outputs from pipeline.Out()
			var gotOutput []int
			doneReading := make(chan struct{})

			go func() {
				defer close(doneReading)
				for val := range pipeline.Out() {
					gotOutput = append(gotOutput, val)
				}
			}()

			// Now we call Wait() in the main goroutine to block until
			// all pipeline goroutines are finished.
			waitDone := make(chan struct{})
			go func() {
				pipeline.Wait()
				close(waitDone)
			}()

			// Check if Wait returns by the deadline
			select {
			case <-waitDone:
				if !tt.expectWaitToEnd {
					t.Fatal("Wait ended unexpectedly; test expected it to block")
				}
			case <-time.After(10 * time.Second):
				if tt.expectWaitToEnd {
					t.Fatal("Wait did not return in time")
				}
			}

			<-doneReading // ensure we finish reading pipeline outputs

			// Verify final pipeline output matches expectation if not canceled
			if !tt.cancelContext {
				assert.ElementsMatch(t, tt.wantOutput, gotOutput)
			}

			// Verify the pipeline's error channel has been closed
			_, open := <-errChan
			assert.False(t, open, "errorChan should be closed after Wait returns")
		})
	}
}

// TestPipelines_Start verifies that Pipelines.Start applies the supplied StreamFunc
// to every pipeline in the collection.
func TestPipelines_Start(t *testing.T) {
	tests := []struct {
		name       string
		input      []int
		numPipes   int
		streamFunc StreamFunc[int, int]
		want       [][]int // expected output per pipeline (same length as numPipes)
	}{
		{
			name:     "double numbers on all pipelines",
			input:    []int{1, 2, 3, 4, 5},
			numPipes: 3,
			streamFunc: func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds.Run(func(v int) (int, error) { return v * 2, nil })
			},
			want: [][]int{
				{2, 4, 6, 8, 10},
				{2, 4, 6, 8, 10},
				{2, 4, 6, 8, 10},
			},
		},
		{
			name:     "identity transform",
			input:    []int{9, 8, 7},
			numPipes: 2,
			streamFunc: func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds // no‑op
			},
			want: [][]int{
				{9, 8, 7},
				{9, 8, 7},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := make(chan error, len(tt.input)*tt.numPipes)
			defer close(errs)
			root := New[int, int](context.Background(), NewMockSource(tt.input), errs)
			coll := root.Copy(tt.numPipes).Start(tt.streamFunc)
			require.Len(t, coll, tt.numPipes)
			var wg sync.WaitGroup
			wg.Add(tt.numPipes)
			for i, pl := range coll {
				i, pl := i, pl // capture
				go func() {
					defer wg.Done()
					var got []int
					for v := range pl.Out() {
						got = append(got, v)
					}
					require.ElementsMatch(t, tt.want[i], got)
				}()
			}
			wg.Wait()
		})
	}
}

// TestPipelines_Process verifies that Pipelines.Process mutates every pipeline
// and returns a new Pipelines slice with the transformations applied.
func TestPipelines_Process(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		numPipes    int
		process     datastreams.ProcessFunc[int]
		finalStream StreamFunc[int, int]
		wantPerPipe [][]int
	}{
		{
			name:     "multiply by 10 then leave unchanged",
			input:    []int{1, 2, 3},
			numPipes: 2,
			process: func(v int) (int, error) {
				return v * 10, nil
			},
			finalStream: func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds // passthrough
			},
			wantPerPipe: [][]int{
				{10, 20, 30},
				{10, 20, 30},
			},
		},
		{
			name:     "process with filtering",
			input:    []int{1, 2, 3, 4},
			numPipes: 3,
			process: func(v int) (int, error) {
				if v%2 == 0 { // zero‑out even numbers
					return 0, nil
				}
				return v, nil
			},
			finalStream: func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds.Run(func(v int) (int, error) { return v + 1, nil }) // add one
			},
			wantPerPipe: [][]int{
				{2, 1, 4, 1},
				{2, 1, 4, 1},
				{2, 1, 4, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := make(chan error, len(tt.input)*tt.numPipes)
			defer close(errs)
			root := New[int, int](context.Background(), NewMockSource(tt.input), errs)
			coll := root.Copy(tt.numPipes).Process(tt.process).Start(tt.finalStream)
			require.Len(t, coll, tt.numPipes)
			var wg sync.WaitGroup
			wg.Add(tt.numPipes)
			for i, pl := range coll {
				i, pl := i, pl
				go func() {
					defer wg.Done()
					var got []int
					for v := range pl.Out() {
						got = append(got, v)
					}
					require.ElementsMatch(t, tt.wantPerPipe[i], got)
				}()
			}
			wg.Wait()
		})
	}
}

// TestPipelines_Get exercises the Pipelines.Get helper, ensuring it returns the
// correct pipeline (or error) for a variety of boundary conditions.
func TestPipelines_Get(t *testing.T) {
	type testCase struct {
		name        string
		setup       func() Pipelines[int, int]
		index       int
		wantNil     bool
		expectError bool
	}
	tests := []testCase{
		{
			name: "valid first element",
			setup: func() Pipelines[int, int] {
				errs := make(chan error)
				root := New[int, int](
					context.Background(),
					NewMockSource([]int{1}),
					errs,
				)
				return root.Copy(3)
			},
			index:       0,
			wantNil:     false,
			expectError: false,
		},
		{
			name: "valid last element",
			setup: func() Pipelines[int, int] {
				errs := make(chan error)
				root := New[int, int](
					context.Background(),
					NewMockSource([]int{1}),
					errs,
				)
				return root.Copy(3)
			},
			index:       2,
			wantNil:     false,
			expectError: false,
		},
		{
			name: "negative index",
			setup: func() Pipelines[int, int] {
				return Pipelines[int, int]{}
			},
			index:       -1,
			wantNil:     true,
			expectError: true,
		},
		{
			name: "index == len(collection)",
			setup: func() Pipelines[int, int] {
				errs := make(chan error)
				root := New[int, int](
					context.Background(),
					NewMockSource([]int{1}),
					errs,
				)
				return root.Copy(2)
			},
			index:       2,
			wantNil:     true,
			expectError: true,
		},
		{
			name:        "empty collection",
			setup:       func() Pipelines[int, int] { return nil },
			index:       0,
			wantNil:     true,
			expectError: true,
		},
		{
			name: "valid but underlying pipeline is nil",
			setup: func() Pipelines[int, int] {
				return Pipelines[int, int]{nil}
			},
			index:       0,
			wantNil:     true,
			expectError: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			collection := tc.setup()
			got, err := collection.Get(tc.index)
			if tc.expectError {
				require.Error(t, err, "expected an error but got nil")
				require.Nil(t, got, "expected returned pipeline to be nil on error")
				return
			}
			require.NoError(t, err, "unexpected error from Get")
			if tc.wantNil {
				require.Nil(t, got, "expected nil pipeline pointer")
			} else {
				require.Same(t, collection[tc.index], got,
					"returned pointer must match element stored in slice")
			}
		})
	}
}

func TestPipelines_Wait(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		numPipes    int
		cancelEarly bool
	}{
		{
			name:     "complete normally",
			input:    []int{5, 6, 7},
			numPipes: 2,
		},
		{
			name:        "context canceled early",
			input:       []int{1, 2, 3, 4, 5},
			numPipes:    3,
			cancelEarly: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errs := make(chan error, len(tt.input)*tt.numPipes)
			defer close(errs)
			root := New[int, int](ctx, NewMockSource(tt.input), errs)
			coll := root.Copy(tt.numPipes).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds
			})
			if tt.cancelEarly {
				cancel()
			}
			var drainWG sync.WaitGroup
			for _, pl := range coll {
				drainWG.Add(1)
				go func(p *Pipeline[int, int]) {
					defer drainWG.Done()
					for range p.Out() {
					}
				}(pl)
			}
			waitDone := make(chan struct{})
			go func() {
				coll.Wait()
				close(waitDone)
			}()
			assert.Eventually(t, func() bool {
				select {
				case <-waitDone:
					return true
				default:
					return false
				}
			}, 120*time.Second, 20*time.Millisecond, "Pipelines.Wait() never returned")
			for _, pl := range coll {
				assert.Eventually(t, func() bool {
					select {
					case _, open := <-pl.Out():
						return !open
					default:
						return false
					}
				}, 120*time.Second, 10*time.Millisecond, "pipeline Out channel not closed")
			}
			drainWG.Wait()
		})
	}
}

func TestPipelines_Close(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		numPipes int
	}{
		{
			name:     "close stops all pipelines",
			input:    []int{1, 2, 3, 4},
			numPipes: 2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			errs := make(chan error, len(tt.input)*tt.numPipes)
			defer close(errs)
			root := New[int, int](
				context.Background(),
				NewMockSource(tt.input),
				errs,
			)
			coll := root.Copy(tt.numPipes).Start(func(ds datastreams.DataStream[int]) datastreams.DataStream[int] {
				return ds.Run(func(v int) (int, error) { return v * 2, nil })
			})
			var drainWG sync.WaitGroup
			for _, pl := range coll {
				drainWG.Add(1)
				go func(p *Pipeline[int, int]) {
					defer drainWG.Done()
					for range p.Out() {
					}
				}(pl)
			}
			coll.Close()
			waitDone := make(chan struct{})
			go func() {
				coll.Wait()
				close(waitDone)
			}()
			assert.Eventually(t, func() bool {
				select {
				case <-waitDone:
					return true
				default:
					return false
				}
			}, 120*time.Second, 20*time.Millisecond, "Wait() did not return after Close()")
			for _, pl := range coll {
				assert.Eventually(t, func() bool {
					select {
					case _, open := <-pl.Out():
						return !open
					default:
						return false
					}
				}, 120*time.Second, 10*time.Millisecond, "pipeline Out channel not closed after Close")
			}
			drainWG.Wait()
		})
	}
}

type MockSource[T any] struct {
	out      chan T
	messages []T
}

func NewMockSource[T any](messages []T) *MockSource[T] {
	out := make(chan T, len(messages))
	return &MockSource[T]{out: out, messages: messages}
}

func (m *MockSource[T]) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[T] {
	defer close(m.out)
	for _, msg := range m.messages {
		m.out <- msg
	}
	return datastreams.New[T](ctx, m.out, errSender)
}

type sender[T any] interface {
	send(input T) error
}

type mockSender[T any] struct {
	mock.Mock
}

func (m *mockSender[T]) send(input T) error {
	args := m.Called(input)
	return args.Error(0)
}

func newMockSender[T any]() *mockSender[T] {
	return &mockSender[T]{}
}

type sinker[T any] struct {
	sender sender[T]
}

func newMockSinker[T any](sender sender[T]) datastreams.Sinker[T] {
	return &sinker[T]{
		sender: sender,
	}
}

func (m *sinker[T]) Sink(_ context.Context, ds datastreams.DataStream[T]) error {
	for input := range ds.Out() {
		if err := m.sender.send(input); err != nil {
			return fmt.Errorf("publisher error: %w", err)
		}
	}
	return nil
}
