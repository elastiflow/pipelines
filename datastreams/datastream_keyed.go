package datastreams

import (
	"context"
	"sync"
	"time"
)

type stream[T any] struct {
	ds DataStream[T]
	p  pipes[T]
}

func newStream[T any](ds DataStream[T], p pipes[T]) stream[T] {
	return stream[T]{
		ds: ds,
		p:  p,
	}
}

type streamStore[T any, K comparable] struct {
	store map[K]stream[T]
	mu    sync.RWMutex
}

func newStreamStore[T any, K comparable]() streamStore[T, K] {
	return streamStore[T, K]{
		store: make(map[K]stream[T]),
	}
}

func (ss *streamStore[T, K]) stream(key K) (stream[T], bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	ds, found := ss.store[key]
	if !found {
		return ds, false
	}
	return ds, true
}

func (ss *streamStore[T, K]) set(key K, stream stream[T]) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.store[key] = stream
}

// getOrCreateDataStream is used to either retrieve an existing
// DataStream[T] for the given key or create one if it doesn't exist.
func (ss *streamStore[T, K]) getOrCreateDataStream(
	key K,
	params Params,
	chanCount int,
	ctx context.Context,
	errStream chan<- error,
	wg *sync.WaitGroup,
) (DataStream[T], pipes[T]) {
	s, found := ss.stream(key)
	if found {
		return s.ds, s.p
	}

	nextDs, outChannels := next[T](
		standard,
		params,
		chanCount,
		ctx,
		errStream,
		wg,
	)
	ss.set(key, newStream[T](nextDs, outChannels))
	return nextDs, outChannels
}

type WatermarkGenerator[T any] interface {
	// OnEvent is called whenever a new event arrives,
	// allowing the generator to update its state.
	OnEvent(event T, eventTime time.Time)

	// GetWatermark returns the current watermark.
	GetWatermark() time.Time
}

type KeyedDataStream[T any, K comparable] struct {
	keyFunc     KeyFunc[T, K]
	waterMarker WatermarkGenerator[T]
	streamStore *streamStore[T, K]
	ctx         context.Context
	wg          *sync.WaitGroup
	errStream   chan<- error
	params      Params
}

func WithWatermarkGenerator[T any, K comparable](
	waterMarker WatermarkGenerator[T],
) func(ks KeyedDataStream[T, K]) KeyedDataStream[T, K] {
	return func(ks KeyedDataStream[T, K]) KeyedDataStream[T, K] {
		ks.waterMarker = waterMarker
		return ks
	}
}

// Stream returns the DataStream for a particular key, if it exists.
func (k KeyedDataStream[T, K]) Stream(key K) DataStream[T] {
	ds, _ := k.streamStore.getOrCreateDataStream(key, k.params, 1, k.ctx, k.errStream, k.wg)
	return ds
}

// WithWatermarkGenerator attaches a watermark generator to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithWatermarkGenerator(
	waterMarker WatermarkGenerator[T],
) KeyedDataStream[T, K] {
	k.waterMarker = waterMarker
	return k
}

// WithWaitGroup attaches a WaitGroup to this KeyedDataStream, returning a copy.
func (k KeyedDataStream[T, K]) WithWaitGroup(wg *sync.WaitGroup) KeyedDataStream[T, K] {
	k.wg = wg
	return k
}

func (k KeyedDataStream[T, K]) incrementWaitGroup(delta int) {
	if k.wg != nil {
		k.wg.Add(delta)
	}
}

// KeyBy partitions a DataStream into multiple channels (one channel per computed key)
// so that all items with the same key travel through the same DataStream[T].
func KeyBy[T any, K comparable](
	ds DataStream[T],
	keyFunc KeyFunc[T, K],
	param ...Params,
) KeyedDataStream[T, K] {
	ss := newStreamStore[T, K]()
	kds := KeyedDataStream[T, K]{
		ctx:         ds.ctx,
		errStream:   ds.errStream,
		streamStore: &ss,
		wg:          ds.wg,
		keyFunc:     keyFunc,
		params:      applyParams(param...),
	}

	for i := 0; i < len(ds.inStreams); i++ {
		kds.incrementWaitGroup(1)
		go func(in <-chan T) {
			if ds.wg != nil {
				defer ds.wg.Done()
			}
			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					if kds.waterMarker != nil {
						kds.waterMarker.OnEvent(v, time.Now()) // TODO use clock instead of time.Now()
					}
					key := kds.keyFunc(v)
					_, outChans := kds.streamStore.getOrCreateDataStream(key, kds.params, 1, kds.ctx, kds.errStream, kds.wg)
					for j := 0; j < len(outChans); j++ {
						go func(appCtx context.Context, v T, outChans chan T) {
							select {
							case <-appCtx.Done():
								return
							default:
								outChans <- v
							}
						}(kds.ctx, v, outChans[j])
					}
				case <-kds.ctx.Done():
					return
				}
			}
		}(ds.inStreams[i])
	}

	return kds
}
