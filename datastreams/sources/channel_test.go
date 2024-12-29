package sources

import (
	"context"
	"testing"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromChannel(t *testing.T) {
	type testCase[T any] struct {
		name   string
		rec    <-chan T
		params []Params
		want   datastreams.Sourcer[T]
	}

	in := make(chan int, 128)
	tests := []testCase[int]{
		{
			name:   "given valid params, should return channelSource",
			rec:    in,
			params: []Params{{BufferSize: 128}},
			want: &channelSource[int]{
				receiver: in,
				params:   Params{BufferSize: 128},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := FromChannel(tt.rec, tt.params...)
			require.NotNil(t, source)
			assert.Equal(t, tt.want, source)
		})
	}
}

func TestChannelSource_Source(t *testing.T) {
	type testCase[T any] struct {
		name   string
		rec    chan T
		params Params
		input  []T
		want   []T
	}

	tests := []testCase[int]{
		{
			name:   "given valid params, should consume data",
			rec:    make(chan int, 128),
			params: Params{BufferSize: 128},
			input:  []int{1, 2},
			want:   []int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for _, val := range tt.input {
					tt.rec <- val
				}
				close(tt.rec)
			}()

			source := FromChannel(tt.rec, tt.params)
			ds := source.Source(ctx, make(chan error, 128))

			var got []int
			for val := range ds.Out() {
				got = append(got, val)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
