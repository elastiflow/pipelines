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

type Manager[T any, K comparable] struct {
	ctx     context.Context
	errs    chan<- error
	senders pipes.Senders[[]T]
	store   *ShardedStore[T, K]
	ManagerOpts[T, K]
}

type ManagerOpts[T any, K comparable] struct {
	TimeMarker   TimeMarker
	WatermarkGen WatermarkGenerator[T]
	Factory      Factory[T]
	ShardOpts    *ShardedStoreOpts[K]
}

func NewPartitionManager[T any, K comparable](
	ctx context.Context,
	senders pipes.Senders[[]T],
	opts ManagerOpts[T, K],
) *Manager[T, K] {
	return &Manager[T, K]{
		ctx:         ctx,
		ManagerOpts: opts,
		store:       NewShardedStore[T, K](opts.ShardOpts),
		senders:     senders,
	}
}

func (m *Manager[T, K]) Partition(key K, value T) {
	p, ok := m.store.Get(key)
	if !ok {
		p = m.Factory(m.ctx, m.senders, m.errs)
		m.store.Set(key, p)
	}

	eventTime := time.Now()
	if m.TimeMarker != nil {
		eventTime = m.TimeMarker.Now()
	}

	if m.WatermarkGen != nil {
		m.WatermarkGen.OnEvent(value, eventTime)
	}

	p.Push(value)
}

func (m *Manager[T, K]) Keys() []K {
	return m.store.Keys()
}
