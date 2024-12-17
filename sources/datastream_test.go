package sources

import (
	"context"
	"testing"

	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"github.com/stretchr/testify/assert"
)

func TestFromDataStream(t *testing.T) {
	type testCase[T any] struct {
		name string
		ds   pipe.DataStream[T]
		want *Pipe[T]
	}

	in := make(chan int, 128)
	errs := make(chan errors.Error, 128)
	tests := []testCase[int]{
		{
			name: "given valid params, should return pipe consumer",
			ds:   pipe.NewDataStream[int](context.Background(), in, errs),
			want: &Pipe[int]{
				out: make(chan int, 128),
				ds:  pipe.NewDataStream[int](context.Background(), in, errs),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := FromDataStream(tt.ds)
			assert.Equal(t, tt.want.ds, source.ds)
			assert.NotNil(t, source.out)
		})
	}
}

func TestPipe_Consume(t *testing.T) {
	type testCase[T any] struct {
		name string
		ds   pipe.DataStream[T]
	}

	in := make(chan int, 128)
	errs := make(chan errors.Error, 128)
	tests := []testCase[int]{
		{
			name: "given valid params, should consume data",
			ds:   pipe.NewDataStream[int](context.Background(), in, errs),
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
			go source.Consume(context.Background(), errs)
			elements := []int{}
			for val := range source.Out() {
				elements = append(elements, val)
			}

			assert.Equal(t, []int{1, 2}, elements)
		})
	}
}
