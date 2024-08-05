package pipe

// Params are used to pass args into Pipe methods.
type Params struct {
	Num        int
	BufferSize int
	SkipError  bool
}

func DefaultParams() Params {
	return Params{}
}

// ProcessFunc is a user defined function type used in a given Pipe stage
type ProcessFunc[T any] func(T) (T, error)

// ProcessRegistry enables the extension of pipe.Pipe with user defined methods
type ProcessRegistry[T any] map[string]ProcessFunc[T]

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
