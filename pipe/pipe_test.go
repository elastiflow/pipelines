package pipe

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipeTake(t *testing.T) {
	tests := []struct {
		name  string
		input [][]int
		num   int
		want  []int
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}
			params := Params{Num: tt.num}
			outputPipe := pipe.Take(params)
			var got []int
			for _, stream := range outputPipe.inStreams {
				for event := range stream {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeFanOut(t *testing.T) {
	tests := []struct {
		name  string
		input []int
		num   int
		want  []int
	}{
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
			want: []int{
				1, 2, 3, 4, 5,
			},
		},
		{
			name:  "fan out with empty input",
			input: []int{},
			num:   3,
			want:  []int{},
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
			}
			params := Params{Num: tt.num, BufferSize: len(tt.input)}
			outputPipe := pipe.FanOut(params)
			assert.Equal(t, tt.num, len(outputPipe.inStreams))
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeFanIn(t *testing.T) {
	tests := []struct {
		name  string
		input [][]int
		want  []int
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}
			params := Params{BufferSize: len(tt.input)}
			outputPipe := pipe.FanIn(params)
			var got []int
			for event := range outputPipe.inStreams[0] {
				got = append(got, event)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeOrDone(t *testing.T) {
	tests := []struct {
		name  string
		input [][]int
		want  []int
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: inputStreams,
			}
			outputPipe := pipe.OrDone(DefaultParams())
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeBroadcast(t *testing.T) {
	tests := []struct {
		name  string
		input []int
		num   int
		want  []int
	}{
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
			}
			params := Params{Num: tt.num, BufferSize: len(tt.input)*tt.num + 1}
			outputPipe := pipe.Broadcast(params)
			assert.Equal(t, tt.num, len(outputPipe.inStreams))
			var got []int
			for _, outStream := range outputPipe.inStreams {
				for event := range outStream {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeTee(t *testing.T) {
	tests := []struct {
		name  string
		input []int
		want  [][]int
	}{
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
			pipe := Pipe[int]{
				ctx:       ctx,
				inStreams: []<-chan int{inputStream},
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
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestPipeRun(t *testing.T) {
	tests := []struct {
		name    string
		input   []int
		process ProcessFunc[int]
		params  Params
		want    []int
	}{
		{
			name:  "run with simple process",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			params: DefaultParams(),
			want:   []int{2, 4, 6, 8, 10},
		},
		{
			name:  "run with nil params",
			input: []int{1, 2, 3, 4, 5},
			process: func(v int) (int, error) {
				return v * 2, nil
			},
			params: DefaultParams(),
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
			params: DefaultParams(),
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
			params: DefaultParams(),
			want:   []int{},
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
			errStream := make(chan error, len(tt.input))
			pipe := Pipe[int]{
				ctx:       ctx,
				errStream: errStream,
				inStreams: []<-chan int{inputStream},
			}
			outputPipe := pipe.Run(tt.process, tt.params)
			var got []int
			for event := range outputPipe.inStreams[0] {
				got = append(got, event)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
