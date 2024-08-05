package pipelines

// Props defines the properties of a New Pipeline
type Props[T any] struct {
	inputChan <-chan T
	errChan   chan<- error
}

// NewProps creates a new Props
func NewProps[T any](
	inChan <-chan T,
	errChan chan<- error,
) *Props[T] {
	return &Props[T]{
		inputChan: inChan,
		errChan:   errChan,
	}
}
