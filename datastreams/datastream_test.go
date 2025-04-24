package datastreams

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataStream_Take(t *testing.T) {
	type testCase struct {
		name          string
		input         [][]int
		num           int
		want          []int
		withWaitGroup bool
	}

	tests := []testCase{
		{
			name: "take from single stream",
			input: [][]int{
				{1, 2, 3, 4, 5},
			},
			num:  3,
			want: []int{1, 2, 3},
		},
		{
			name: "take from multiple streams",
			input: [][]int{
				{1, 2},
				{3, 4, 5},
			},
			num:  2,
			want: []int{1, 2, 3, 4},
		},
		{
			name: "take more than available",
			input: [][]int{
				{1, 2},
			},
			num:  5,
			want: []int{1, 2},
		},
		// New test withWaitGroup
		{
			name: "take with wait group",
			input: [][]int{
				{1, 2, 3, 4, 5},
			},
			num:           3,
			want:          []int{1, 2, 3},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var inputStreams []<-chan int
			for _, stream := range tt.input {
				eventStream := make(chan int, len(stream))
				for _, event := range stream {
					eventStream <- event
				}
				close(eventStream)
				inputStreams = append(inputStreams, eventStream)
			}
			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}

			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}

			params := Params{Num: tt.num}
			outputPipe := pipe.Take(params)
			var got []int
			for _, stream := range outputPipe.inStreams {
				for event := range stream {
					got = append(got, event)
				}
			}
			if tt.withWaitGroup {
				wg.Wait() // Ensure all goroutines complete
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_FanOut(t *testing.T) {
	type testCase struct {
		name          string
		input         []int
		num           int
		want          []int
		withWaitGroup bool
	}
	tests := []testCase{
		{
			name:  "fan out to multiple streams",
			input: []int{1, 2, 3, 4, 5},
			num:   3,
			want:  []int{1, 2, 3, 4, 5},
		},
		{
			name:  "fan out to single stream",
			input: []int{1, 2, 3, 4, 5},
			num:   1,
			want:  []int{1, 2, 3, 4, 5},
		},
		{
			name:  "fan out with empty input",
			input: []int{},
			num:   3,
			want:  []int{},
		},
		{
			name:          "fan out with wait group",
			input:         []int{10, 20, 30},
			num:           2,
			want:          []int{10, 20, 30},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			inputStream := make(chan int, len(tt.input))
			for _, event := range tt.input {
				inputStream <- event
			}
			close(inputStream)

			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
			}
			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}

			params := Params{Num: tt.num, BufferSize: len(tt.input)}
			outputPipe := pipe.FanOut(params)
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			if tt.withWaitGroup {
				wg.Wait()
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_FanIn(t *testing.T) {
	type testCase struct {
		name          string
		input         [][]int
		want          []int
		withWaitGroup bool
	}
	tests := []testCase{
		{
			name: "fan in from multiple streams",
			input: [][]int{
				{1, 2},
				{3, 4, 5},
			},
			want: []int{1, 2, 3, 4, 5},
		},
		{
			name: "fan in from single stream",
			input: [][]int{
				{1, 2, 3, 4, 5},
			},
			want: []int{1, 2, 3, 4, 5},
		},
		{
			name: "fan in with empty input",
			input: [][]int{
				{},
				{},
			},
			want: []int{},
		},
		{
			name: "fan in from multiple streams + wait group",
			input: [][]int{
				{10, 20},
				{30, 40},
			},
			want:          []int{10, 20, 30, 40},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var inputStreams []<-chan int
			for _, stream := range tt.input {
				eventStream := make(chan int, len(stream))
				for _, event := range stream {
					eventStream <- event
				}
				close(eventStream)
				inputStreams = append(inputStreams, eventStream)
			}
			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}
			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}
			params := Params{BufferSize: len(tt.input)}
			outputPipe := pipe.FanIn(params)
			var got []int
			for event := range outputPipe.inStreams[0] {
				got = append(got, event)
			}
			if tt.withWaitGroup {
				wg.Wait()
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_OrDone(t *testing.T) {
	type testCase struct {
		name          string
		input         [][]int
		want          []int
		withWaitGroup bool
	}
	tests := []testCase{
		{
			name: "orDone with multiple streams",
			input: [][]int{
				{1, 2},
				{3, 4, 5},
			},
			want: []int{1, 2, 3, 4, 5},
		},
		{
			name: "orDone with single stream",
			input: [][]int{
				{1, 2, 3, 4, 5},
			},
			want: []int{1, 2, 3, 4, 5},
		},
		{
			name: "orDone with empty input",
			input: [][]int{
				{},
				{},
			},
			want: []int{},
		},
		{
			name: "orDone with wait group",
			input: [][]int{
				{10, 11, 12},
			},
			want:          []int{10, 11, 12},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var inputStreams []<-chan int
			for _, stream := range tt.input {
				eventStream := make(chan int, len(stream))
				for _, event := range stream {
					eventStream <- event
				}
				close(eventStream)
				inputStreams = append(inputStreams, eventStream)
			}
			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}
			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}
			outputPipe := pipe.OrDone()
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			if tt.withWaitGroup {
				wg.Wait()
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_Broadcast(t *testing.T) {
	type testCase struct {
		name          string
		input         []int
		num           int
		want          []int
		withWaitGroup bool
	}
	tests := []testCase{
		{
			name:  "broadcast to multiple streams",
			input: []int{1, 2, 3, 4, 5},
			num:   2,
			want:  []int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
		},
		{
			name:  "broadcast to single stream",
			input: []int{1, 2, 3, 4, 5},
			num:   1,
			want:  []int{1, 2, 3, 4, 5},
		},
		{
			name:  "broadcast with empty input",
			input: []int{},
			num:   2,
			want:  []int{},
		},
		{
			name:          "broadcast to multiple streams with waitgroup",
			input:         []int{10, 20},
			num:           2,
			want:          []int{10, 20, 10, 20},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inputStream := make(chan int, len(tt.input))
			for _, event := range tt.input {
				inputStream <- event
			}
			close(inputStream)
			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
			}
			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}
			params := Params{Num: tt.num, BufferSize: len(tt.input)*tt.num + 1}
			outputPipe := pipe.Broadcast(params)
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			if tt.withWaitGroup {
				wg.Wait()
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_Tee(t *testing.T) {
	type testCase struct {
		name          string
		input         []int
		want          [][]int
		withWaitGroup bool
	}

	tests := []testCase{
		{
			name:  "tee to multiple streams",
			input: []int{1, 2, 3, 4, 5},
			want: [][]int{
				{1, 2, 3, 4, 5},
				{1, 2, 3, 4, 5},
			},
		},
		{
			name:  "tee with empty input",
			input: []int{},
			want:  [][]int{},
		},
		{
			name:          "tee to multiple streams + wait group",
			input:         []int{10, 20},
			want:          [][]int{{10, 20}, {10, 20}},
			withWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg *sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inputStream := make(chan int, len(tt.input))
			for _, event := range tt.input {
				inputStream <- event
			}
			close(inputStream)
			pipe := DataStream[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
			}
			if tt.withWaitGroup {
				wg = &sync.WaitGroup{}
				pipe = pipe.WithWaitGroup(wg)
			}
			params := Params{BufferSize: len(tt.input)}
			outputPipe1, outputPipe2 := pipe.Tee(params)
			var got [][]int
			var got1, got2 []int
			for _, outPipe := range outputPipe1.inStreams {
				for event := range outPipe {
					got1 = append(got1, event)
				}
			}
			for _, outPipe := range outputPipe2.inStreams {
				for event := range outPipe {
					got2 = append(got2, event)
				}
			}
			if len(got1) > 0 {
				got = append(got, got1)
			}
			if len(got2) > 0 {
				got = append(got, got2)
			}

			if tt.withWaitGroup {
				wg.Wait()
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_Run(t *testing.T) {
	tests := []struct {
		name         string
		input        []int
		process      ProcessFunc[int]
		params       Params
		want         []int
		useWaitGroup bool
	}{
		{
			name:  "run with simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			params: Params{},
			want:   []int{2, 4, 6, 8, 10},
		},
		{
			name:  "run with nil params",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			params: Params{},
			want:   []int{2, 4, 6, 8, 10},
		},
		{
			name:  "run with error process",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				if v%2 == 0 {
					return 0, fmt.Errorf("even number error")
				}
				return v, nil
			},
			params: Params{},
			want:   []int{1, 0, 3, 0, 5},
		},
		{
			name:  "run with error process skip",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				if v%2 == 0 {
					return 0, fmt.Errorf("even number error")
				}
				return v, nil
			},
			params: Params{SkipError: true},
			want:   []int{1, 3, 5},
		},
		{
			name:  "run with empty input",
			input: []int{},
			process: func(v int) (int, error) {
				return v, nil
			},
			params: Params{},
			want:   []int{},
		},
		{
			name:  "run with waitgroup",
			input: []int{10, 20},
			process: func(v int) (int, error) {
				return v + 1, nil
			},
			want:         []int{11, 21},
			useWaitGroup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inputStream := make(chan int, len(tt.input))
			for _, event := range tt.input {
				inputStream <- event
			}
			close(inputStream)
			errStream := make(chan error, len(tt.input)+5)
			ds := DataStream[int]{
				ctx:       ctx,
				errStream: errStream,
				inStreams: []<-chan int{inputStream},
			}
			var wg *sync.WaitGroup
			if tt.useWaitGroup {
				wg = &sync.WaitGroup{}
				ds = ds.WithWaitGroup(wg)
			}
			outputPipe := ds.Run(tt.process, tt.params)
			var got []int
			for event := range outputPipe.inStreams[0] {
				got = append(got, event)
			}
			assert.ElementsMatch(t, tt.want, got)
			if tt.useWaitGroup && wg != nil {
				wg.Wait()
			}
		})
	}
}

func TestDataStream_Filter(t *testing.T) {
	tests := []struct {
		name   string
		input  []int
		filter FilterFunc[int]
		params Params
		want   []int
	}{
		{
			name:  "run with simple process",
			input: []int{1, 2, 3, 4, 5},
			filter: func(v int) (bool, error) {
				return v%2 == 0, nil
			},
			params: Params{},
			want:   []int{2, 4},
		},
		{
			name:  "run with errors",
			input: []int{1, 2, 3, 4, 5},
			filter: func(v int) (bool, error) {
				return true, errors.New("error")
			},
			params: Params{},
			want:   []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inputStream := make(chan int, len(tt.input))
			for _, event := range tt.input {
				inputStream <- event
			}
			close(inputStream)
			errStream := make(chan error, len(tt.input))
			pipe := DataStream[int]{
				ctx:       ctx,
				errStream: errStream,
				inStreams: []<-chan int{inputStream},
			}
			outputPipe := pipe.Filter(tt.filter, tt.params)
			var got []int
			for event := range outputPipe.inStreams[0] {
				got = append(got, event)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDataStream_Map(t *testing.T) {
	tests := []struct {
		name             string
		processes        []ProcessFunc[int]
		transformers     []TransformFunc[int, string]
		postprocessor    []ProcessFunc[string]
		expectedElements []string
	}{
		{
			name: "process then transform the data",
			processes: []ProcessFunc[int]{
				func(v int) (int, error) {
					return v * 2, nil
				},
			},
			transformers: []TransformFunc[int, string]{
				func(v int) (string, error) {
					return fmt.Sprintf("dollars: %v", v), nil
				},
			},
			expectedElements: []string{
				"dollars: 0",
				"dollars: 2",
				"dollars: 4",
				"dollars: 6",
				"dollars: 8",
			},
		},
		{
			name: "map then post process the results",
			transformers: []TransformFunc[int, string]{
				func(v int) (string, error) {
					return fmt.Sprintf("dollars: %v", v), nil
				},
			},
			postprocessor: []ProcessFunc[string]{
				func(v string) (string, error) {
					return fmt.Sprintf("I was not multiplied, %v", v), nil
				},
			},
			expectedElements: []string{
				"I was not multiplied, dollars: 0",
				"I was not multiplied, dollars: 1",
				"I was not multiplied, dollars: 2",
				"I was not multiplied, dollars: 3",
				"I was not multiplied, dollars: 4",
			},
		},
		{
			name: "multi process to fan out map the process output",
			processes: []ProcessFunc[int]{
				func(v int) (int, error) {
					return v * 2, nil
				},
				func(v int) (int, error) {
					return v * v, nil
				},
				func(v int) (int, error) {
					return v % 3, nil
				},
			},
			transformers: []TransformFunc[int, string]{
				func(v int) (string, error) {
					return fmt.Sprintf("modulo: %v", v), nil
				},
			},
			postprocessor: []ProcessFunc[string]{
				func(v string) (string, error) {
					return fmt.Sprintf("I'm divisible by 3, %v", v), nil
				},
			},
			expectedElements: []string{
				"I'm divisible by 3, modulo: 0",
				"I'm divisible by 3, modulo: 1",
				"I'm divisible by 3, modulo: 1",
				"I'm divisible by 3, modulo: 0",
				"I'm divisible by 3, modulo: 1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputChan := make(chan int)
			errChan := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer func() {
				close(errChan)
				cancel()
			}()

			go func() {
				for num := range 5 {
					inputChan <- num
				}
				close(inputChan)
			}()

			pipe := New[int](ctx, inputChan, errChan)
			for _, process := range tt.processes {
				pipe = pipe.Run(process)
			}

			var next_ DataStream[string]
			for _, transformer := range tt.transformers {
				next_ = Map(pipe, transformer)
			}

			for _, postprocessor := range tt.postprocessor {
				next_ = next_.Run(postprocessor)
			}

			var results []string
			for out := range next_.OrDone().Out() {
				results = append(results, out)
			}

			assert.ElementsMatch(t, tt.expectedElements, results)
		})
	}
}

func TestDataStream_Expand(t *testing.T) {
	tests := []struct {
		name             string
		input            []int
		processes        []ProcessFunc[int]
		expandFunc       ExpandFunc[int, string]
		postprocessor    []ProcessFunc[string]
		expectedElements []string
		expectedErr      string
	}{
		{
			name:  "process then expand the data into both dollars and pesos",
			input: []int{0, 1, 2},
			processes: []ProcessFunc[int]{
				func(v int) (int, error) {
					return v * 2, nil
				},
			},
			expandFunc: func(v int) ([]string, error) {
				return []string{
					fmt.Sprintf("dollars: %v", v),
					fmt.Sprintf("pesos: %v", v*20),
				}, nil
			},
			expectedElements: []string{
				"dollars: 0",
				"pesos: 0",
				"dollars: 2",
				"pesos: 40",
				"dollars: 4",
				"pesos: 80",
			},
		},
		{
			name:  "expand then post process the results",
			input: []int{2, 3, 4},
			expandFunc: func(v int) ([]string, error) {
				return []string{
					fmt.Sprintf("dollars: %v", v),
					fmt.Sprintf("pesos: %v", v*20),
				}, nil
			},
			postprocessor: []ProcessFunc[string]{
				func(v string) (string, error) {
					return fmt.Sprintf("Post processed - %v", v), nil
				},
			},
			expectedElements: []string{
				"Post processed - dollars: 2",
				"Post processed - pesos: 40",
				"Post processed - dollars: 3",
				"Post processed - pesos: 60",
				"Post processed - dollars: 4",
				"Post processed - pesos: 80",
			},
		},
		{
			name:  "should transform one input into several different outputs",
			input: []int{5},
			expandFunc: func(v int) ([]string, error) {
				return []string{
					fmt.Sprintf("value: %v", v),
					fmt.Sprintf("multiplied: %v", v*2),
					fmt.Sprintf("multiplied by 3: %v", v*3),
					fmt.Sprintf("modulo: %v", v%3),
				}, nil
			},
			expectedElements: []string{
				"value: 5",
				"multiplied: 10",
				"multiplied by 3: 15",
				"modulo: 2",
			},
		},
		{
			name:  "should error when the expandFunc returns an error",
			input: []int{5},
			expandFunc: func(v int) ([]string, error) {
				return nil, fmt.Errorf("error while expanding")
			},
			expectedErr: "error while expanding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputChan := make(chan int)
			errChan := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer func() {
				close(errChan)
				cancel()
			}()

			go func() {
				for _, num := range tt.input {
					inputChan <- num
				}
				close(inputChan)
			}()

			pipe := New[int](ctx, inputChan, errChan)
			for _, process := range tt.processes {
				pipe = pipe.Run(process)
			}

			next_ := Expand(pipe, tt.expandFunc)

			for _, postprocessor := range tt.postprocessor {
				next_ = next_.Run(postprocessor)
			}

			var results []string
			for out := range next_.OrDone().Out() {
				results = append(results, out)
			}

			select {
			case err := <-errChan:
				if err != nil && tt.expectedErr == "" {
					t.Errorf("unexpected error: %v", err)
				} else {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
			default:
				if tt.expectedErr != "" {
					t.Errorf("expected error but got none")
				}
			}

			assert.ElementsMatch(t, tt.expectedElements, results)
		})
	}
}

func TestDataStream_Sink(t *testing.T) {
	type testCase struct {
		name              string
		input             []int
		sinkErrorValue    int
		expectedCollected []int
		expectedErrors    int
	}

	tests := []testCase{
		{
			name:              "error on 3",
			input:             []int{1, 2, 3, 4, 5},
			sinkErrorValue:    3,
			expectedCollected: []int{1, 2},
			expectedErrors:    1,
		},
		{
			name:              "no errors returned",
			input:             []int{10, 20, 30},
			sinkErrorValue:    -1,
			expectedCollected: []int{10, 20, 30},
			expectedErrors:    0,
		},
		{
			name:              "error on first item",
			input:             []int{99, 100},
			sinkErrorValue:    99,
			expectedCollected: []int{},
			expectedErrors:    1,
		},
		{
			name:              "empty input",
			input:             []int{},
			sinkErrorValue:    -1,
			expectedCollected: []int{},
			expectedErrors:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			in := make(chan int, len(tt.input))
			for _, val := range tt.input {
				in <- val
			}
			close(in)
			errChan := make(chan error, len(tt.input)+2)
			ds := New[int](ctx, in, errChan)
			collected := []int{}
			var wg sync.WaitGroup
			wg.Add(1)
			sink := MockSinkerFunc[int](func(ctx context.Context, ds DataStream[int]) error {
				defer wg.Done()
				for val := range ds.Out() {
					if val == tt.sinkErrorValue && tt.sinkErrorValue != -1 {
						return errors.New("sink error triggered on value")
					}
					collected = append(collected, val)
				}
				return nil
			})
			ds.Sink(sink)
			wg.Wait()
			gotErrors := make([]error, 0, tt.expectedErrors)
			for i := 0; i < tt.expectedErrors; i++ {
				select {
				case e := <-errChan:
					gotErrors = append(gotErrors, e)
				case <-time.After(200 * time.Millisecond):
					require.Fail(t, "expected error but none arrived")
				}
			}
			if tt.expectedErrors == 0 {
				select {
				case e := <-errChan:
					require.Failf(t, "unexpected error", "got: %v", e)
				default:
					// no errors == good
				}
			}
			assert.Equal(t, tt.expectedCollected, collected, "collected items mismatch")
			require.Len(t, gotErrors, tt.expectedErrors, "number of errors mismatch")
			if tt.expectedErrors > 0 {
				assert.Contains(t, gotErrors[0].Error(), "sink error triggered on value")
			}
		})
	}
}

func TestDataStream_WithWaitGroup(t *testing.T) {
	tests := []struct {
		name    string
		input   []int
		process ProcessFunc[int]
		cancel  bool
		want    []int
	}{
		{
			name:  "no cancel, simple doubling",
			input: []int{1, 2, 3},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			want: []int{2, 4, 6},
		},
		{
			name:  "context canceled before reading all",
			input: []int{10, 20, 30},
			process: func(v int) (int, error) {
				return v, nil
			},
			cancel: true,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			defer cancelFn()
			in := make(chan int, len(tt.input))
			for _, val := range tt.input {
				in <- val
			}
			close(in)
			wg := &sync.WaitGroup{}
			errCh := make(chan error, len(tt.input)*2)
			ds := DataStream[int]{
				ctx:       ctx,
				errStream: errCh,
				inStreams: []<-chan int{in},
			}.WithWaitGroup(wg)
			outDS := ds.Run(tt.process)
			if tt.cancel {
				cancelFn()
			}
			var got []int
			for val := range outDS.Out() {
				got = append(got, val)
			}
			wg.Wait()
			if !tt.cancel {
				assert.ElementsMatch(t, tt.want, got)
			} else {
				t.Logf("Canceled scenario: got %v before context was canceled", got)
			}
		})
	}
}

// MockSinkerFunc is a helper type that implements the Sinker[T] interface via a function.
type MockSinkerFunc[T any] func(ctx context.Context, ds DataStream[T]) error

func (f MockSinkerFunc[T]) Sink(ctx context.Context, ds DataStream[T]) error {
	return f(ctx, ds)
}
