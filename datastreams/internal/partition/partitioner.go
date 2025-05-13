package partition

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

type TimeMarker interface {
	Now() time.Time
}

type Partition[T any] interface {
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

type Factory[T any] func(
	ctx context.Context,
	senders pipes.Senders[[]T],
	errs chan<- error,
) Partition[T]

type manager[T any, K comparable] struct {
	ctx        context.Context
	errs       chan<- error
	factory    Factory[T]
	generator  WatermarkGenerator[T]
	senders    pipes.Senders[[]T]
	store      *store[T, K]
	timeMarker TimeMarker
}

type Partitioner[T any, K comparable] interface {
	// Partition is called to send an item to the appropriate partition.
	//  Key: the key to partition by
	//  Value: the value to send
	Partition(key K, value T)
	// Keys returns the keys of the partitions.
	Keys() []K
}

func NewPartitioner[T any, K comparable](
	ctx context.Context,
	senders pipes.Senders[[]T],
	factory Factory[T],
	timeMarker TimeMarker,
	generator WatermarkGenerator[T],
) Partitioner[T, K] {
	return &manager[T, K]{
		ctx:        ctx,
		factory:    factory,
		generator:  generator,
		store:      newStore[T, K](),
		timeMarker: timeMarker,
		senders:    senders,
	}
}

func (m *manager[T, K]) Partition(key K, value T) {
	p, ok := m.store.get(key)
	if !ok {
		p = m.factory(m.ctx, m.senders, m.errs)
		m.store.set(key, p)
	}

	eventTime := time.Now()
	if m.timeMarker != nil {
		eventTime = m.timeMarker.Now()
	}

	if m.generator != nil {
		eventTime = m.generator.GetWatermark()
		m.generator.OnEvent(value, eventTime)
	}

	p.Push(value)
}

func (m *manager[T, K]) Keys() []K {
	m.store.mu.RLock()
	defer m.store.mu.RUnlock()

	keys := make([]K, 0, len(m.store.partitions))
	for k := range m.store.partitions {
		keys = append(keys, k)
	}
	return keys
}
