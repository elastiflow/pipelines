package partition

import (
	"context"
	"sync"
	"time"
)

type Partition[T any, R any] interface {
	Push(item T, eventTime time.Time)
	Initialize() chan R
	Close()
}

type Factory[T any, R any] func(
	ctx context.Context,
	errs chan<- error,
) Partition[T, R]

type Manager[T any, K comparable, R any] struct {
	store   *store[T, K, R]
	factory Factory[T, R]
	errs    chan<- error
	wg      *sync.WaitGroup
	out     chan R
}

func NewManager[T any, K comparable, R any](
	errs chan<- error,
	factory Factory[T, R],
) *Manager[T, K, R] {
	return &Manager[T, K, R]{
		store:   newStore[T, K, R](),
		factory: factory,
		errs:    errs,
		wg:      &sync.WaitGroup{},
		out:     make(chan R),
	}
}

func (m *Manager[T, K, R]) Partition(ctx context.Context, key K) Partition[T, R] {
	p, ok := m.store.get(key)
	if !ok {
		p = m.initPartition(ctx, m.out)
		m.store.set(key, p.Partition, p.out)
	}
	return p.Partition
}

func (m *Manager[T, K, R]) Close() {
	m.store.close()
}

func (m *Manager[T, K, R]) Out() <-chan R {
	return m.out
}

func (m *Manager[T, K, R]) initPartition(ctx context.Context, streamOut chan R) partition[T, R] {
	instance := m.factory(ctx, m.errs)
	pOut := instance.Initialize()
	go func(in chan R, out chan<- R) {
		defer close(pOut)
		for {
			select {
			case <-ctx.Done():
				return
			case agg, aggOk := <-in:
				if !aggOk {
					return
				}
				out <- agg
			}
		}
	}(pOut, streamOut)
	return partition[T, R]{
		Partition: instance,
		out:       pOut,
	}
}
