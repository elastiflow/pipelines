package pipelines

import (
	"context"

	pipelineErrors "github.com/elastiflow/pipelines/errors"
	"github.com/stretchr/testify/mock"
)

type MockConsumer[T any] struct {
	out      chan T
	messages []T
}

func NewMockConsumer[T any](messages []T) *MockConsumer[T] {
	out := make(chan T, len(messages))
	return &MockConsumer[T]{out: out, messages: messages}
}

func (m *MockConsumer[T]) Consume(ctx context.Context, errs chan<- pipelineErrors.Error) {
	defer close(m.out)
	for _, msg := range m.messages {
		m.out <- msg
	}
}
func (m *MockConsumer[T]) Out() <-chan T {
	return m.out
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

type publisher[T any] struct {
	sender sender[T]
}

func newMockPublisher[T any](sender sender[T]) *publisher[T] {
	return &publisher[T]{
		sender: sender,
	}
}

func (m *publisher[T]) Publish(ctx context.Context, in <-chan T, errs chan<- pipelineErrors.Error) {
	for input := range in {
		if err := m.sender.send(input); err != nil {
			errs <- pipelineErrors.NewSegment("publisher", "", err)
		}
	}
}
