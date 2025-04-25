package sinks

import (
	"context"
	"testing"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToChannel(t *testing.T) {
	type testCase[T any] struct {
		name   string
		sender chan T
		params []Params
		want   datastreams.Sinker[T]
	}

	out := make(chan int, 128)
	tests := []testCase[int]{
		{
			name:   "given valid params, should return channelSink",
			sender: out,
			params: []Params{{BufferSize: 128}},
			want: &channelSink[int]{
				sender: out,
				params: Params{BufferSize: 128},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := ToChannel(tt.sender, tt.params...)
			require.NotNil(t, sink)
			assert.Equal(t, tt.want, sink)
		})
	}
}

func TestChannelSink_Sink(t *testing.T) {
	type testCase[T any] struct {
		name   string
		sender chan T
		params Params
		input  []T
		want   []T
	}

	tests := []testCase[int]{
		{
			name:   "given valid params, should send data",
			sender: make(chan int, 128),
			params: Params{BufferSize: 128},
			input:  []int{1, 2},
			want:   []int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sinkChan := make(chan int, 128)
			inChan := make(chan int, 128)
			errChan := make(chan error, 128)
			sourceDS := datastreams.New[int](ctx, inChan, errChan)
			go func() {
				for _, val := range tt.input {
					inChan <- val
				}
				close(inChan)
			}()
			sink := ToChannel(sinkChan, tt.params)
			go func() {
				_ = sink.Sink(ctx, sourceDS)
			}()

			var got []int
			for val := range sinkChan {
				got = append(got, val)
				if len(got) == len(tt.input) {
					break
				}
			}
			close(sinkChan)
			require.Equal(t, tt.want, got)
			assert.Empty(t, errChan)
		})
	}
}

func TestDataStream_Sink(t *testing.T) {
	type testCase[T any] struct {
		name            string
		input           []T
		params          Params
		buildDataStream func(ctx context.Context, inChan chan int, errChan chan error) datastreams.DataStream[T]
		want            []T
	}

	tests := []testCase[int]{
		{
			name:   "sink with valid data",
			input:  []int{1, 2, 3, 4, 5},
			params: Params{BufferSize: 128},
			buildDataStream: func(ctx context.Context, inChan chan int, errChan chan error) datastreams.DataStream[int] {
				return datastreams.New(ctx, inChan, errChan)
			},
			want: []int{1, 2, 3, 4, 5},
		},
		{
			name:   "sink a fanned out data stream with valid data",
			input:  []int{1, 2, 3, 4, 5},
			params: Params{BufferSize: 128},
			buildDataStream: func(ctx context.Context, inChan chan int, errChan chan error) datastreams.DataStream[int] {
				return datastreams.New(ctx, inChan, errChan).FanOut(datastreams.Params{Num: 2})
			},
			want: []int{1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			inChan := make(chan int, len(tt.input))
			errChan := make(chan error, 128)
			sourceDS := tt.buildDataStream(ctx, inChan, errChan)
			outChan := make(chan int, len(tt.input))
			sink := ToChannel(outChan, tt.params)
			sourceDS.Sink(sink)
			go func() {
				for _, val := range tt.input {
					inChan <- val
				}
				close(inChan)
			}()

			var got []int
			for val := range outChan {
				got = append(got, val)
				if len(got) == len(tt.input) {
					break
				}
			}
			close(outChan)
			require.ElementsMatch(t, tt.want, got)
			assert.Empty(t, errChan)
		})
	}
}
