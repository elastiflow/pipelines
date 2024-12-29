package sources

import (
	"context"
	"testing"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromDataStream(t *testing.T) {
	type testCase[T any] struct {
		name string
		ds   datastreams.DataStream[T]
		want *datastream[T]
	}

	in := make(chan int, 128)
	errs := make(chan error, 128)
	tests := []testCase[int]{
		{
			name: "given valid params, should return pipe consumer",
			ds:   datastreams.New[int](context.Background(), in, errs),
			want: &datastream[int]{
				out:     make(chan int, 128),
				inputDS: datastreams.New[int](context.Background(), in, errs),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := FromDataStream(tt.ds)
			require.NotNil(t, source.Source(context.Background(), errs))
		})
	}
}

func TestDataStreamSource_Source(t *testing.T) {
	type testCase[T any] struct {
		name string
		ds   datastreams.DataStream[T]
	}

	in := make(chan int, 128)
	errs := make(chan error, 128)
	tests := []testCase[int]{
		{
			name: "given valid params, should consume data",
			ds:   datastreams.New[int](context.Background(), in, errs),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := FromDataStream(tt.ds)
			go func() {
				in <- 1
				in <- 2
				close(in)
			}()
			elements := []int{}
			for val := range source.Source(context.Background(), errs).Out() {
				elements = append(elements, val)
			}

			assert.Equal(t, []int{1, 2}, elements)
		})
	}
}
