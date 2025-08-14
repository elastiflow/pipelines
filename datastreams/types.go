package datastreams

import (
	"context"
	"time"
)

// ProcessFunc is a user defined function type used in a given DataStream stage
type ProcessFunc[T any] func(T) (T, error)

// TransformFunc is a user defined function type used in a given DataStream stage
// This function type is used to transform a given input type to a given output type
type TransformFunc[T any, U any] func(T) (U, error)

// ExpandFunc is a user defined function type used in a given DataStream stage
// This function type is used to expand a given input into multiple outputs
type ExpandFunc[T any, U any] func(T) ([]U, error)

// FilterFunc is a user defined function type used in a given DataStream stage
// This function type is used to filter a given input type
type FilterFunc[T any] func(T) (bool, error)

// KeyFunc is a user defined function that takes a value of type T and returns a key of type K
// for the given value. This function is used to partition pipeline data streams by key.
type KeyFunc[T any, K comparable] func(T) K

// WindowFunc processes a given batch of data and returns a result. You can use this
// function in conjunction with the KeyedDataStream to perform windowed operations on  batches
// of data with a given key.
type WindowFunc[T any, R any] func([]T) (R, error)

// ShardKeyFunc defines the function signature for a sharding algorithm.
// It takes a key and the total number of shards and must return a deterministic
// shard index (typically in the range [0, shardCount-1]). Implementations can
// include modulus hashing, jump hashing, or other consistent hashing algorithms.
type ShardKeyFunc[K comparable] func(k K, shardCount int) uint64

type TimeMarker interface {
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

type Keyable[K comparable] interface {
	Key() K
}

type KeyableElement[T any, K comparable] interface {
	Keyable[K]
	Value() T
}

type TimedKeyableElement[T any, K comparable] interface {
	KeyableElement[T, K]
	Time() time.Time
}

type Closeable interface {
	// Close signals the component to flush any remaining data and shut down.
	// It should be called when the component is no longer needed.
	Close()
}

// Partition represents a single, active partition for a specific key.
// It is an active component responsible for its own logic.
type Partition[T any, K comparable] interface {
	// Push sends a new element into the partition for processing.
	Push(item TimedKeyableElement[T, K])
}

// Partitioner TODO: Decouple Partitioning from Windowing. ref github issue #61
// Partitioner acts as a template or factory for creating new partitions.
// A single Partitioner (e.g., a configured Sliding window) is used to create
// all active partition instances for different keys.
type Partitioner[T any, K comparable] interface {
	// Create initializes and starts a new, active Partition instance.
	// It takes a context for cancellation and an output channel where it will
	// send its results (e.g., slices of T for windows).
	Create(ctx context.Context, out chan<- []T) Partition[T, K]
	Closeable
}

type WindowedPartitioner[T any, K comparable] interface {
	// Create initializes and starts a new, active Partition instance.
	// It takes a context for cancellation and an output channel where it will
	Create(ctx context.Context, out chan<- []T) Partition[T, K]
	Closeable
}
