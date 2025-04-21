package partition

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"time"
)

type TimeMarker interface {
	Now() time.Time
}

type Partition[T any, R any] interface {
	// Push is called to send an item to the partition.
	Push(item T)
}

type WatermarkGenerator[T any] interface {
	// OnEvent is called whenever a new event arrives, allowing the generator to update its state.
	//  Event: the event that arrived
	//  EventTime: the time of the event
	OnEvent(event T, eventTime time.Time)

	// GetWatermark returns the current watermark.
	GetWatermark() time.Time
}

type Factory[T any, R any] func(
	ctx context.Context,
	out pipes.Senders[R],
	errs chan<- error,
) Partition[T, R]

type manager[T any, K comparable, R any] struct {
	ctx         context.Context
	errs        chan<- error
	factory     Factory[T, R]
	generator   WatermarkGenerator[T]
	outChannels pipes.Senders[R]
	store       *store[T, K, R]
	timeMarker  TimeMarker
}

type Partitioner[T any, K comparable, R any] interface {
	// Partition is called to send an item to the appropriate partition.
	//  Key: the key to partition by
	//  Value: the value to send
	Partition(key K, value T)
	Keys() []K
}

func NewPartitioner[T any, K comparable, R any](
	ctx context.Context,
	out pipes.Senders[R],
	errs chan<- error,
	factory Factory[T, R],
	timeMarker TimeMarker,
	generator WatermarkGenerator[T],
) Partitioner[T, K, R] {
	return &manager[T, K, R]{
		ctx:         ctx,
		errs:        errs,
		factory:     factory,
		generator:   generator,
		outChannels: out,
		store:       newStore[T, K, R](),
		timeMarker:  timeMarker,
	}
}

func (m *manager[T, K, R]) Partition(key K, value T) {
	p, ok := m.store.get(key)
	if !ok {
		p = m.factory(m.ctx, m.outChannels, m.errs)
		m.store.set(key, p)
	}

	eventTime := time.Now()
	if m.timeMarker != nil {
		eventTime = m.timeMarker.Now()
	}

	if m.generator != nil {
		m.generator.OnEvent(value, eventTime)
		eventTime = m.generator.GetWatermark()
	}

	p.Push(value)
}

func (m *manager[T, K, R]) Keys() []K {
	m.store.mu.RLock()
	defer m.store.mu.RUnlock()

	keys := make([]K, 0, len(m.store.partitions))
	for k := range m.store.partitions {
		keys = append(keys, k)
	}
	return keys
}
