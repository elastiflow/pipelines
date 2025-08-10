package datastreams

import (
	"time"

	"github.com/elastiflow/pipelines/datastreams/partitioner"
)

// KeyedDataStream represents a stream of data elements partitioned by a key of type K, derived using a key function.
type KeyedDataStream[T any, K comparable] struct {
	DataStream[partitioner.Keyable[T, K]]
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
	nextPipe, outChannels := next[partitioner.Keyable[T, K]](standard, param, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	kds := KeyedDataStream[T, K]{
		DataStream: nextPipe,
	}

	for i, inStream := range ds.inStreams {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan T, outChannel chan<- partitioner.Keyable[T, K]) {
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
					case outChannel <- partitioner.NewKeyable[T, K](item, keyFunc(item)):
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
		Partition(keyedDs.inStreams)

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
	wf WindowFunc[KeyedUnion[T, U, K], R],
	pf partitioner.Factory[KeyedUnion[T, U, K]],
	param ...Params,
) DataStream[R] {
	p := applyParams(param...)
	joinPipe, joinPipeOutChan := next[KeyedUnion[T, U, K]](standard, p, len(left.inStreams), left.ctx, left.errStream, left.wg)
	mergeLeft[T, U, K](left, joinPipeOutChan.Senders())
	mergeRight[T, U, K](right, joinPipeOutChan.Senders())

	kds := KeyBy[KeyedUnion[T, U, K], K](
		joinPipe,
		func(rec KeyedUnion[T, U, K]) (key K) {
			if rec.Left() != nil {
				return rec.Left().Key()
			}

			if rec.Right() != nil {
				return rec.Right().Key()
			}
			return key
		},
	)

	return Window[KeyedUnion[T, U, K], K, R](
		kds,
		wf,
		pf,
		param...,
	)

}

func mergeLeft[T any, U any, K comparable](left KeyedDataStream[T, K], outChans []chan<- KeyedUnion[T, U, K]) {
	for i, in := range left.inStreams {
		left.incrementWaitGroup(1)
		go func(inStream <-chan partitioner.Keyable[T, K], outStream chan<- KeyedUnion[T, U, K]) {
			defer left.decrementWaitGroup()
			for event := range inStream {
				select {
				case <-left.ctx.Done():
					return
				default:
					outStream <- NewKeyedUnion[T, U, K](
						event,
						nil,
						time.Now(),
					)
				}
			}
		}(in, outChans[i])
	}
}

func mergeRight[T any, U any, K comparable](keyedDs KeyedDataStream[U, K], outChans []chan<- KeyedUnion[T, U, K]) {
	for i, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(inStream <-chan partitioner.Keyable[U, K], outStream chan<- KeyedUnion[T, U, K]) {
			defer keyedDs.decrementWaitGroup()
			for event := range inStream {
				select {
				case <-keyedDs.ctx.Done():
					return
				default:
					outStream <- NewKeyedUnion[T, U, K](
						nil,
						event,
						time.Now(),
					)
				}
			}
		}(in, outChans[i])
	}
}

type Union[T any, U any] interface {
	Left() T
	Right() U
	TS() time.Time
}

type union[T any, U any] struct {
	left  T
	right U
	ts    time.Time
}

func NewUnion[T any, U any](
	left T,
	right U,
	ts time.Time,
) Union[T, U] {
	return &union[T, U]{
		left:  left,
		right: right,
		ts:    ts,
	}
}

func (k *union[T, U]) Left() T {
	return k.left
}

func (k *union[T, U]) Right() U {
	return k.right
}

func (k *union[T, U]) TS() time.Time {
	return k.ts
}

type KeyedUnion[T any, U any, K comparable] interface {
	Union[partitioner.Keyable[T, K], partitioner.Keyable[U, K]]
}

func NewKeyedUnion[T any, U any, K comparable](
	left partitioner.Keyable[T, K],
	right partitioner.Keyable[U, K],
	ts time.Time,
) KeyedUnion[T, U, K] {
	return NewUnion[partitioner.Keyable[T, K], partitioner.Keyable[U, K]](
		left,
		right,
		ts,
	)
}
