package datastreams

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams/internal/partition"
	"time"
)

type WatermarkGenerator[T any] interface {
	// OnEvent is called whenever a new event arrives, allowing the generator to update its state.
	//  Event: the event that arrived
	//  EventTime: the time of the event
	OnEvent(event T, eventTime time.Time)

	// GetWatermark returns the current watermark.
	GetWatermark() time.Time
}

// KeyedDataStream represents a stream of data elements partitioned by a key of type K, derived using a key function.
type KeyedDataStream[T any, K comparable] struct {
	DataStream[T]
	keyFunc     KeyFunc[T, K]
	waterMarker WatermarkGenerator[T]
}

// WithWatermarkGenerator attaches a watermark generator to the KeyedDataStream.
func (k KeyedDataStream[T, K]) WithWatermarkGenerator(
	waterMarker WatermarkGenerator[T],
) KeyedDataStream[T, K] {
	k.waterMarker = waterMarker
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
	}
	pm := partition.NewManager[T, K, R](keyedDs.errStream, wf)
	for _, in := range keyedDs.inStreams {
		keyedDs.incrementWaitGroup(1)
		go func(pm *partition.Manager[T, K, R], inStream <-chan T) {
			defer keyedDs.decrementWaitGroup()
			for {
				select {
				case <-keyedDs.ctx.Done():
					return
				case item, ok := <-inStream:
					if !ok {
						return
					}
					push[T, K, R](keyedDs, item, pm)
				}
			}
		}(pm, in)
	}

	sendOut[R](keyedDs.ctx, pm.Out(), outChannels)
	return kdOut
}

// JoinFunc is the user-provided function that takes a pair (A,B) and
// returns a single result (R) or an error.
type JoinFunc[A any, R any] func(a A, b A) (R, error)

// Join merges two KeyedDataStream[A,K] and KeyedDataStream[B,K] into
// a single KeyedDataStream[R,K], using a join function joinFn for items
// that share the same key.
//
// This example does a naive "infinite buffer" join: it stores all items
// from each side until the context is done or the channels close.
func Join[T any, K comparable, R any](
	keyFunc KeyFunc[R, K],
	leftDs KeyedDataStream[T, K],
	rightDs KeyedDataStream[T, K],
	joiner partition.Factory[T, R],
	params ...Params,
) KeyedDataStream[R, K] {
	p := applyParams(params...)
	nextPipe, outChannels := next[R](standard, p, len(leftDs.inStreams), leftDs.ctx, leftDs.errStream, leftDs.wg)
	kdOut := KeyedDataStream[R, K]{
		DataStream: nextPipe,
		keyFunc:    keyFunc,
	}
	pm := partition.NewManager[T, K, R](leftDs.errStream, joiner)
	for i := 0; i < len(leftDs.inStreams); i++ {
		leftDs.incrementWaitGroup(1)
		rightDs.incrementWaitGroup(1)
		go func(pm *partition.Manager[T, K, R], left <-chan T, right <-chan T) {
			defer leftDs.decrementWaitGroup()
			defer rightDs.decrementWaitGroup()
			for {
				select {
				case <-rightDs.ctx.Done():
					return
				case <-leftDs.ctx.Done():
					return
				case leftItem, leftOk := <-left:
					if !leftOk {
						return
					}
					push[T, K, R](leftDs, leftItem, pm)
				case rightItem, rightOk := <-right:
					if !rightOk {
						return
					}
					push[T, K, R](leftDs, rightItem, pm)
				}
			}
		}(pm, leftDs.inStreams[i], rightDs.inStreams[i])
	}

	sendOut[R](leftDs.ctx, pm.Out(), outChannels)
	return kdOut
}

func push[T any, K comparable, R any](keyedDs KeyedDataStream[T, K], item T, pm *partition.Manager[T, K, R]) {
	eventTime := time.Now()
	if keyedDs.waterMarker != nil {
		keyedDs.waterMarker.OnEvent(item, eventTime)
		eventTime = keyedDs.waterMarker.GetWatermark()
	}

	key := keyedDs.keyFunc(item)
	pm.Partition(keyedDs.ctx, key).Push(item, eventTime)
}

func sendOut[T any](ctx context.Context, source <-chan T, outChannels pipes[T]) {
	for _, out := range outChannels {
		go func(outChannel chan<- T) {
			defer close(outChannel)
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-source:
					if !ok {
						return
					}
					outChannel <- item
				}
			}
		}(out)
	}
}
