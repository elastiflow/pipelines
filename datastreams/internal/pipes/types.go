package pipes

import "context"

type Receivers[T any] []<-chan T
type Senders[T any] []chan<- T

func (s Senders[T]) Send(ctx context.Context, item T) {
	for _, out := range s {
		select {
		case <-ctx.Done():
			return
		case out <- item:
		}
	}
}

type Pipes[T any] []chan T

func (c Pipes[T]) Initialize(buffer int) {
	for i := range len(c) {
		if buffer > 0 {
			c[i] = make(chan T, buffer)
			continue
		}
		c[i] = make(chan T)
	}
}

func (c Pipes[T]) Close() {
	for i := range len(c) {
		close(c[i])
	}
}

func (c Pipes[T]) Senders() Senders[T] {
	s := make(Senders[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}

func (c Pipes[T]) Receivers() Receivers[T] {
	s := make(Receivers[T], len(c))
	for i := range c {
		s[i] = c[i]
	}
	return s
}
