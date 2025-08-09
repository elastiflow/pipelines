package partitioner

import (
	"context"
	"sync"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

type TimeMarker interface {
	// Now returns the current time according to the marker.
	Now() time.Time
}

type WatermarkGenerator[T any] interface {
	// OnEvent is called whenever a new event arrives, allowing the generator to update its state.
	//  Event: the event that arrived
	//  EventTime: the time of the event
	OnEvent(event T, eventTime time.Time)

	// GetWatermark returns the current watermark.
	GetWatermark() time.Time
}

type Partition[T any] interface {
	// Push is called to send an item to the partition.
	Push(item T)
}

type Factory[T any] func(
	ctx context.Context,
	senders pipes.Senders[[]T],
	errs chan<- error,
) Partition[T]

type Partitioner[T any, K comparable] interface {
	// Keys returns the keys of the partitions.
	Keys() []K

	// Partition starts partitioning the input streams using the provided key function.
	Partition(keyFunc func(T) K, inStreams []<-chan T) Partitioner[T, K]
}

type Builder[T any, K comparable] struct {
	partitioner *manager[T, K]
}

func (pb *Builder[T, K]) WithContext(ctx context.Context) *Builder[T, K] {
	pb.partitioner.ctx = ctx
	return pb
}

func (pb *Builder[T, K]) WithWaitGroup(wg *sync.WaitGroup) *Builder[T, K] {
	pb.partitioner.wg = wg
	return pb
}

func (pb *Builder[T, K]) WithErrorChannel(errs chan<- error) *Builder[T, K] {
	pb.partitioner.errs = errs
	return pb
}

func (pb *Builder[T, K]) WithWatermarkGenerator(generator WatermarkGenerator[T]) *Builder[T, K] {
	pb.partitioner.generator = generator
	return pb
}

func (pb *Builder[T, K]) WithSenders(senders pipes.Senders[[]T]) *Builder[T, K] {
	pb.partitioner.senders = senders
	return pb
}

func (pb *Builder[T, K]) WithTimeMarker(marker TimeMarker) *Builder[T, K] {
	pb.partitioner.timeMarker = marker
	return pb
}

func (pb *Builder[T, K]) Build() Partitioner[T, K] {
	return pb.partitioner
}

func New[T any, K comparable](
	factory Factory[T],
) *Builder[T, K] {
	return &Builder[T, K]{
		partitioner: &manager[T, K]{
			factory: factory,
			store:   newStore[T, K](),
		},
	}
}

type manager[T any, K comparable] struct {
	ctx        context.Context
	wg         *sync.WaitGroup
	errs       chan<- error
	factory    Factory[T]
	generator  WatermarkGenerator[T]
	senders    pipes.Senders[[]T]
	store      *store[T, K]
	timeMarker TimeMarker
	keyFunc    func(T) K
}

func (m *manager[T, K]) Partition(keyFunc func(T) K, inStreams []<-chan T) Partitioner[T, K] {
	for _, in := range inStreams {
		m.incrementWaitGroup(1)
		go func(ctx context.Context, inStream <-chan T) {
			defer m.decrementWaitGroup()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}
					key := keyFunc(item)
					m.push(key, item)
				}
			}
		}(m.ctx, in)
	}
	return m
}

func (m *manager[T, K]) push(key K, value T) {
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

func (m *manager[T, K]) incrementWaitGroup(delta int) {
	if m.wg != nil {
		m.wg.Add(delta)
	}
}

func (m *manager[T, K]) decrementWaitGroup() {
	if m.wg != nil {
		m.wg.Done()
	}
}
