package pipelines

import (
	"context"
	"fmt"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/stretchr/testify/mock"
)

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

func (m *sinker[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	for input := range ds.Out() {
		if err := m.sender.send(input); err != nil {
			return fmt.Errorf("publisher error: %wr", err)
		}
	}
	return nil
}
