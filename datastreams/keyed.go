package datastreams

import (
	"github.com/elastiflow/pipelines/datastreams/partitioner"
	"time"
)

// KeyedDataStream represents a stream of data elements partitioned by a key of type K, derived using a key function.
type KeyedDataStream[T any, K comparable] struct {
	DataStream[T]
	keyFunc     KeyFunc[T, K]
	waterMarker partitioner.WatermarkGenerator[T]
	timeMarker  partitioner.TimeMarker
}

// WithWatermarkGenerator attaches a watermark generator to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithWatermarkGenerator(
	waterMarker partitioner.WatermarkGenerator[T],
) KeyedDataStream[T, K] {
	k.waterMarker = waterMarker
	return k
}

// WithTimeMarker attaches a time marker to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithTimeMarker(
	timeMarker partitioner.TimeMarker,
) KeyedDataStream[T, K] {
	k.timeMarker = timeMarker
	return k
}

// KeyBy partitions a DataStream into multiple channels (one channel per computed key)
// so that all items with the same key travel through the same DataStream[T].
func KeyBy[T any, K comparable](
	ds DataStream[T],
	keyFunc KeyFunc[T, K],
	params ...Params,
) KeyedDataStream[T, K] {
	param := applyParams(params...)
	nextPipe, outChannels := next[T](standard, param, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	kds := KeyedDataStream[T, K]{
		DataStream: nextPipe,
		keyFunc:    keyFunc,
	}

	for i, inStream := range ds.inStreams {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan T, outChannel chan<- T) {
			defer ds.decrementWaitGroup()
			defer close(outChannel)
			for {
				select {
				case <-ds.ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}
					select {
					case outChannel <- item:
					case <-ds.ctx.Done():
						return
					}
				}
			}
		}(inStream, outChannels[i])
	}

	return kds
}

// Window applies a windowing function to a KeyedDataStream, producing a new KeyedDataStream with re-partitioned output.
// It uses the provided partitioner.Factory, KeyFunc, and optional Params to define the windowing behavior.
func Window[T any, K comparable, R any](
	keyedDs KeyedDataStream[T, K],
	wf WindowFunc[T, R],
	pf partitioner.Factory[T],
	param ...Params,
) DataStream[R] {
	p := applyParams(param...)
	nextPipe, outChans := next[R](standard, p, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	windowPipe, windowOutChan := next[[]T](standard, p, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	ctx := keyedDs.ctx
	partitioner.New[T, K](pf).
		WithContext(ctx).
		WithSenders(windowOutChan.Senders()).
		WithWaitGroup(keyedDs.wg).
		WithWatermarkGenerator(keyedDs.waterMarker).
		WithTimeMarker(keyedDs.timeMarker).
		Build().
		Partition(keyedDs.keyFunc, keyedDs.inStreams)

	for i, in := range windowPipe.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(inStream <-chan []T, outStream chan<- R) {
			defer keyedDs.decrementWaitGroup()
			for {
				select {
				case <-keyedDs.ctx.Done():
					return
				case items, ok := <-inStream:
					if !ok {
						return
					}
					result, err := wf(items)
					if err != nil {
						keyedDs.errStream <- err
						continue
					}
					select {
					case outStream <- result:
					case <-keyedDs.ctx.Done():
					}

				}
			}
		}(in, outChans[i])
	}

	return nextPipe
}

func Join[T any, U any, K comparable, R any](
	left KeyedDataStream[T, K],
	right KeyedDataStream[U, K],
	wf WindowFunc[KeyableUnion[T, U, K], R],
	pf partitioner.Factory[KeyableUnion[T, U, K]],
	param ...Params,
) DataStream[R] {
	p := applyParams(param...)
	joinPipe, joinPipeOutChan := next[KeyableUnion[T, U, K]](standard, p, len(left.inStreams), left.ctx, left.errStream, left.wg)
	mergeLeft[T, U, K](left, joinPipeOutChan.Senders())
	mergeRight[T, U, K](right, joinPipeOutChan.Senders())

	kds := KeyBy[KeyableUnion[T, U, K], K](
		joinPipe,
		func(rec KeyableUnion[T, U, K]) (key K) {
			if rec.Left() != nil {
				return rec.Left().Key()
			}

			if rec.Right() != nil {
				return rec.Right().Key()
			}
			return key
		},
	)

	return Window[KeyableUnion[T, U, K], K, R](
		kds,
		wf,
		pf,
		param...,
	)

}

func mergeLeft[T any, U any, K comparable](keyedDs KeyedDataStream[T, K], outChans []chan<- KeyableUnion[T, U, K]) {
	for i, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- KeyableUnion[T, U, K]) {
			defer keyedDs.decrementWaitGroup()
			for item := range inStream {
				select {
				case <-keyedDs.ctx.Done():
					return
				default:
					outStream <- &either[T, U, K]{
						left: newKeyable(item, keyedDs.keyFunc),
						ts:   time.Now(),
					}
				}
			}
		}(in, outChans[i])
	}
}

func mergeRight[T any, U any, K comparable](keyedDs KeyedDataStream[U, K], outChans []chan<- KeyableUnion[T, U, K]) {
	for i, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(inStream <-chan U, outStream chan<- KeyableUnion[T, U, K]) {
			defer keyedDs.decrementWaitGroup()
			for event := range inStream {
				select {
				case <-keyedDs.ctx.Done():
					return
				default:
					outStream <- &either[T, U, K]{
						right: newKeyable(event, keyedDs.keyFunc),
						ts:    time.Now(),
					}
				}
			}
		}(in, outChans[i])
	}
}

type Keyable[T any, K comparable] interface {
	Key() K
	Value() T
}

type Union[T any, U any] interface {
	Left() T
	Right() U
}

type KeyableUnion[T any, U any, K comparable] interface {
	Union[Keyable[T, K], Keyable[U, K]]
}

type either[T any, U any, K comparable] struct {
	left  Keyable[T, K]
	right Keyable[U, K]
	ts    time.Time
}

func (r *either[T, U, K]) Left() Keyable[T, K] {
	return r.left
}

func (r *either[T, U, K]) Right() Keyable[U, K] {
	return r.right
}

type keyable[T any, K comparable] struct {
	value   T
	keyFunc func(T) K
}

func newKeyable[T any, K comparable](
	t T,
	keyFunc func(T) K,
) Keyable[T, K] {
	return &keyable[T, K]{
		value:   t,
		keyFunc: keyFunc,
	}

}

func (k *keyable[T, K]) Key() K {
	return k.keyFunc(k.value)
}

func (k *keyable[T, K]) Value() T {
	return k.value
}
