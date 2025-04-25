package datastreams

import (
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
)

// KeyedDataStream represents a stream of data elements partitioned by a key of type K, derived using a key function.
type KeyedDataStream[T any, K comparable] struct {
	DataStream[T]
	keyFunc     KeyFunc[T, K]
	waterMarker partition.WatermarkGenerator[T]
	timeMarker  partition.TimeMarker
}

// WithWatermarkGenerator attaches a watermark generator to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithWatermarkGenerator(
	waterMarker partition.WatermarkGenerator[T],
) KeyedDataStream[T, K] {
	k.waterMarker = waterMarker
	return k
}

// WithTimeMarker attaches a time marker to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithTimeMarker(
	timeMarker partition.TimeMarker,
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
// It uses the provided partition.Factory, KeyFunc, and optional Params to define the windowing behavior.
func Window[T any, K comparable, R any](
	keyedDs KeyedDataStream[T, K],
	wf WindowFunc[T, R],
	pf partition.Factory[T],
	param ...Params,
) DataStream[R] {
	p := applyParams(param...)
	nextPipe, outChans := next[R](standard, p, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	windowPipe, windowOutChan := next[[]T](standard, p, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	pm := partition.NewPartitioner[T, K](
		keyedDs.ctx,
		windowOutChan.Senders(),
		pf,
		keyedDs.timeMarker,
		keyedDs.waterMarker,
	)

	for _, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(pm partition.Partitioner[T, K], inStream <-chan T) {
			defer keyedDs.decrementWaitGroup()
			for {
				select {
				case <-keyedDs.ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}
					key := keyedDs.keyFunc(item)
					pm.Partition(key, item)
				}
			}
		}(pm, in)
	}

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

func WindowAll[T any, K comparable, R any](
	ds DataStream[T],
	wf WindowFunc[T, R],
	pf partition.Factory[T],
	param ...Params,
) DataStream[R] {
	appliedParams := applyParams(param...)
	nextPipe, outChans := next[R](standard, appliedParams, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	windowPipe, windowOutChan := next[[]T](standard, appliedParams, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	p := pf(ds.ctx, windowOutChan.Senders(), ds.errStream)
	for _, in := range ds.inStreams {
		ds.incrementWaitGroup(1)
		go func(partition partition.Partition[T], inStream <-chan T) {
			defer ds.decrementWaitGroup()
			for {
				select {
				case <-ds.ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}

					partition.Push(item)
				}
			}
		}(p, in)
	}

	for i, in := range windowPipe.inStreams {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan []T, outStream chan<- R) {
			defer ds.decrementWaitGroup()
			for {
				select {
				case <-ds.ctx.Done():
					return
				case items, ok := <-inStream:
					if !ok {
						return
					}
					result, err := wf(items)
					if err != nil {
						ds.errStream <- err
						continue
					}
					select {
					case outStream <- result:
					case <-ds.ctx.Done():
					}

				}
			}
		}(in, outChans[i])
	}

	return nextPipe
}
