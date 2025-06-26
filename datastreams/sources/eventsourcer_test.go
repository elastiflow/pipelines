package sources

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockConsumer[T any] struct {
	mock.Mock
}

func (m *MockConsumer[T]) MarkSuccess(msg T) {
	m.Called(msg)
}

func (m *MockConsumer[T]) Messages() <-chan T {
	args := m.Called()
	return args.Get(0).(<-chan T)
}

func (m *MockConsumer[T]) MarkError(msg T, err error) {
	m.Called(msg, err)
}

func (m *MockConsumer[T]) Run(ctx context.Context) {
	m.Called(ctx)
}

func TestNewEventSourcer(t *testing.T) {
	mockConsumer := &MockConsumer[int]{}

	sourcer := NewEventSourcer[int](1, mockConsumer)
	assert.NotNil(t, sourcer)
}

func TestEventSourcer_Source(t *testing.T) {
	t.Parallel()
	messageConsumer := &MockConsumer[int]{}
	messages := make(chan int, 64)
	messages <- 56
	close(messages)
	messageConsumer.On("Messages").Return((<-chan int)(messages))
	messageConsumer.On("Run", mock.Anything).Return()

	sourcer := NewEventSourcer[int](
		5,
		messageConsumer,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sourcer.Source(ctx, make(chan<- error, 1))
	<-ctx.Done()
	messageConsumer.AssertExpectations(t)
}

func TestEventSourcer_MarkSuccess(t *testing.T) {
	t.Parallel()
	messageConsumer := &MockConsumer[int]{}
	messageConsumer.On("MarkSuccess", 56).Return()
	sourcer := NewEventSourcer[int](
		5,
		messageConsumer,
	)

	sourcer.MarkSuccess(56)
	messageConsumer.AssertExpectations(t)
}

func TestEventSourcer_MarkError(t *testing.T) {
	t.Parallel()
	messageConsumer := &MockConsumer[int]{}
	messageConsumer.On("MarkError", 0, errors.New("some error")).Return()
	sourcer := NewEventSourcer[int](
		5,
		messageConsumer,
	)

	sourcer.MarkError(0, errors.New("some error"))
	messageConsumer.AssertExpectations(t)
}
