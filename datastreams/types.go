package datastreams

// Processor is a user defined function type used in a given DataStream stage
type Processor[T any] func(T) (T, error)

// Transformer is a user defined function type used in a given DataStream stage
type Transformer[T any, U any] func(T) (U, error)

// Filter is a user defined function type used in a given DataStream stage
type Filter[T any] func(T) (bool, error)

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
