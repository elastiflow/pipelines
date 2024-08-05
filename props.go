package pipelines

import "github.com/elastiflow/pipelines/pipe"

// Props defines the properties of a New Pipeline
type Props[T any] struct {
	pipeRegister pipe.ProcessRegistry[T] // ProcessRegistry is a map of user defined functions to extend pipe.Pipe with
	inputChan    <-chan T
	errChan      chan<- error
}

// NewProps creates a new Props
func NewProps[T any](
	pipeRegister pipe.ProcessRegistry[T],
	inChan <-chan T,
	errChan chan<- error,
) *Props[T] {
	return &Props[T]{
		pipeRegister: pipeRegister,
		inputChan:    inChan,
		errChan:      errChan,
	}
}
