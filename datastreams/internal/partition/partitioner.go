package partition

import (
	"context"
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
	out chan R,
	errs chan<- error,
) Partition[T, R]

type manager[T any, K comparable, R any] struct {
	ctx        context.Context
	errs       chan<- error
	factory    Factory[T, R]
	generator  WatermarkGenerator[T]
	out        chan R
	store      *store[T, K, R]
	timeMarker TimeMarker
}

type Partitioner[T any, K comparable, R any] interface {
	// Partition is called to send an item to the appropriate partition.
	//  Key: the key to partition by
	//  Value: the value to send
	Partition(key K, value T)
	// Keys returns the keys of the partitions.
	Keys() []K
	// Out returns the output channel for the partitioner.
	Out() chan R
}

func NewPartitioner[T any, K comparable, R any](
	ctx context.Context,
	factory Factory[T, R],
	errs chan<- error,
	timeMarker TimeMarker,
	generator WatermarkGenerator[T],
) Partitioner[T, K, R] {
	out := make(chan R)
	return &manager[T, K, R]{
		ctx:        ctx,
		errs:       errs,
		factory:    factory,
		generator:  generator,
		store:      newStore[T, K, R](),
		timeMarker: timeMarker,
		out:        out,
	}
}

// WithExporterChan sets the output channels for the partitioner.
func (m *manager[T, K, R]) WithExporterChan(outChannels chan R) {
	m.out = outChannels
}

func (m *manager[T, K, R]) Partition(key K, value T) {
	p, ok := m.store.get(key)
	if !ok {
		p = m.factory(m.ctx, m.Out(), m.errs)
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

func (m *manager[T, K, R]) Out() chan R {
	return m.out
}
