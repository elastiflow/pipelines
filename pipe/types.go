package pipe

// ProcessFunc is a user defined function type used in a given Pipe stage
type ProcessFunc[T any] func(T) (T, error)

// MapFunc is a user defined function type used in a given Pipe stage
type MapFunc[T any, U any] func(T) (U, error)

type receivers[T any] []<-chan T
type senders[T any] []chan<- T
type channels[T any] []chan T

func (c channels[T]) Initialize(buffer int) {
	for i := range len(c) {
		if buffer > 0 {
			c[i] = make(chan T, buffer)
			continue
		}
		c[i] = make(chan T)
	}
}

func (c channels[T]) Close() {
	for i := range len(c) {
		close(c[i])
	}
}

func (c channels[T]) Senders() senders[T] {
	s := make(senders[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}

func (c channels[T]) Receivers() receivers[T] {
	s := make(receivers[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}
