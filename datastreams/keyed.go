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
			for {
				select {
				case <-ds.ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}
					outChannel <- item
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
	wf partition.Factory[T, R],
	keyFunc KeyFunc[R, K],
	param ...Params,
) KeyedDataStream[R, K] {
	p := applyParams(param...)
	nextPipe, outChannels := next[R](standard, p, len(keyedDs.inStreams), keyedDs.ctx, keyedDs.errStream, keyedDs.wg)
	kdOut := KeyedDataStream[R, K]{
		DataStream: nextPipe,
		keyFunc:    keyFunc,
		timeMarker: keyedDs.timeMarker,
	}

	pm := partition.NewPartitioner[T, K, R](
		keyedDs.ctx,
		outChannels,
		keyedDs.errStream,
		wf,
		keyedDs.timeMarker,
		keyedDs.waterMarker,
	)

	for _, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(pm partition.Partitioner[T, K, R], inStream <-chan T) {
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

	return kdOut
}
