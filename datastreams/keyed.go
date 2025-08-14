package datastreams

import (
	"sync"
	"time"
)

// KeyedDataStream represents a stream of data elements partitioned by a key of type K, derived using a key function.
type KeyedDataStream[T any, K comparable] struct {
	DataStream[KeyableElement[T, K]]
	keyFunc     KeyFunc[T, K]
	waterMarker WatermarkGenerator[T]
	timeMarker  TimeMarker
}

// WithWatermarkGenerator attaches a watermark generator to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithWatermarkGenerator(
	waterMarker WatermarkGenerator[T],
) KeyedDataStream[T, K] {
	k.waterMarker = waterMarker
	return k
}

// WithTimeMarker attaches a time marker to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithTimeMarker(
	timeMarker TimeMarker,
) KeyedDataStream[T, K] {
	k.timeMarker = timeMarker
	return k
}

// KeyBy  a DataStream into multiple channels (one channel per computed key)
// so that all items with the same key travel through the same DataStream[T].
func KeyBy[T any, K comparable](
	ds DataStream[T],
	keyFunc KeyFunc[T, K],
	params ...Params,
) KeyedDataStream[T, K] {
	param := applyParams(params...)
	nextPipe, outChannels := next[KeyableElement[T, K]](standard, param, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	kds := KeyedDataStream[T, K]{
		DataStream: nextPipe,
		keyFunc:    keyFunc,
	}

	for i, inStream := range ds.inStreams {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan T, outChannel chan<- KeyableElement[T, K]) {
			defer ds.decrementWaitGroup()
			defer close(outChannel)
			for item := range inStream {
				select {
				case outChannel <- NewKeyedElement(keyFunc(item), item):
				case <-ds.ctx.Done():
					return
				}
			}
		}(inStream, outChannels[i])
	}

	return kds
}

// Window applies a windowing function to a KeyedDataStream, producing a new KeyedDataStream with re-partitioned output.
// It uses the provided Partitioner[T], KeyFunc, and optional Params to define the windowing behavior.
func Window[T any, K comparable, R any](
	keyedDs KeyedDataStream[T, K],
	wf WindowFunc[T, R],
	partitioner Partitioner[T, K], // The new partitioner interface
	param ...Params,
) DataStream[R] {
	params := applyParams(param...)

	shardedStore := NewShardedPartitionStore[T, K](&ShardedStoreOpts[K]{
		ShardKeyFunc: ModulusHash[K],
		ShardCount:   params.ShardCount,
	})

	var dispatchWg sync.WaitGroup

	windowOut, windowOutChans := next[[]T](standard, params, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	for i, inStream := range keyedDs.inStreams {
		dispatchWg.Add(1)
		keyedDs.incrementWaitGroup(1)

		go func(in <-chan KeyableElement[T, K], windowsChan chan<- []T) {
			defer dispatchWg.Done()
			defer keyedDs.decrementWaitGroup()

			for {
				select {
				case item, ok := <-in:
					if !ok {
						return
					}
					p, exists := shardedStore.Get(item.Key())
					if !exists {
						p = partitioner.Create(keyedDs.ctx, windowsChan)
						shardedStore.Set(item.Key(), p)
					}

					p.Push(NewTimedKeyedElement(item, time.Now()))
				case <-keyedDs.ctx.Done():
					return
				}
			}
		}(inStream, windowOutChans[i])
	}

	go func() {
		dispatchWg.Wait()
		partitioner.Close()
		shardedStore.Close()
		windowOutChans.Close()
	}()

	nextPipe, outChans := next[R](standard, params, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	for i, windowIn := range windowOut.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(in <-chan []T, out chan<- R) {
			defer keyedDs.decrementWaitGroup()
			defer close(out)
			for window := range in {
				result, err := wf(window)
				if err != nil {
					keyedDs.errStream <- err
					continue
				}
				out <- result
			}
		}(windowIn, outChans[i])
	}

	return nextPipe
}

type keyedElement[T any, K comparable] struct {
	k K
	v T
}

// NewKeyedElement creates a new KeyableElement with the given key and value.
func NewKeyedElement[T any, K comparable](key K, value T) KeyableElement[T, K] {
	return &keyedElement[T, K]{k: key, v: value}
}

func (ke *keyedElement[T, K]) Key() K {
	return ke.k
}

func (ke *keyedElement[T, K]) Value() T {
	return ke.v
}

type timedKeyedElement[T any, K comparable] struct {
	KeyableElement[T, K]
	t time.Time
}

// NewTimedKeyedElement creates a new TimedKeyableElement with the given key, value, and time.
func NewTimedKeyedElement[T any, K comparable](v KeyableElement[T, K], t time.Time) TimedKeyableElement[T, K] {
	return &timedKeyedElement[T, K]{
		KeyableElement: v,
		t:              t,
	}
}

func (tke *timedKeyedElement[T, K]) Time() time.Time {
	return tke.t
}
