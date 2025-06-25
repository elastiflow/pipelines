package sinks

import (
	"context"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockEventProducer[T any] struct {
	mock.Mock
}

func (m *mockEventProducer[T]) Produce(msg T) {
	m.Called(msg)
}

func TestToEventProducer(t *testing.T) {
	producer := &mockEventProducer[int]{}
	sinker := ToEventProducer[int](producer)
	assert.NotNil(t, sinker)
}

func TestEventProducer_Sink(t *testing.T) {
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
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			ep := &mockEventProducer[int]{}
			ep.On("Produce", mock.Anything).Return().Times(len(tt.input))
			inChan := make(chan int, 128)
			errChan := make(chan error, 128)
			sourceDS := datastreams.New[int](ctx, inChan, errChan)
			go func() {
				for _, val := range tt.input {
					inChan <- val
				}
				close(inChan)
			}()
			sink := ToEventProducer[int](ep, tt.params)
			err := sink.Sink(ctx, sourceDS)
			assert.NoError(t, err)
			ep.AssertNumberOfCalls(t, "Produce", len(tt.input))
		})
	}
}
