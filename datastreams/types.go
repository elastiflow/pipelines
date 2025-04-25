package datastreams

// ProcessFunc is a user defined function type used in a given DataStream stage
type ProcessFunc[T any] func(T) (T, error)

// TransformFunc is a user defined function type used in a given DataStream stage
// This function type is used to transform a given input type to a given output type
type TransformFunc[T any, U any] func(T) (U, error)

// ExpandFunc is a user defined function type used in a given DataStream stage
// This function type is used to expand a given input into multiple outputs
type ExpandFunc[T any, U any] func(T) ([]U, error)

// ReduceFunc collapses a slice of M items into a single output.
type ReduceFunc[T any, U any] func([]T) (U, error)

// FilterFunc is a user defined function type used in a given DataStream stage
// This function type is used to filter a given input type
type FilterFunc[T any] func(T) (bool, error)

type receivers[T any] []<-chan T
type senders[T any] []chan<- T
type pipes[T any] []chan T

func (c pipes[T]) Initialize(buffer int) {
	for i := range len(c) {
		if buffer > 0 {
			c[i] = make(chan T, buffer)
			continue
		}
		c[i] = make(chan T)
	}
}

func (c pipes[T]) Close() {
	for i := range len(c) {
		close(c[i])
	}
}

func (c pipes[T]) Senders() senders[T] {
	s := make(senders[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}

func (c pipes[T]) Receivers() receivers[T] {
	s := make(receivers[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}
