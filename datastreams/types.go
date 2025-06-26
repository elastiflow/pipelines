package datastreams

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

// JoinFunc merges a left + right element into a single result.
type JoinFunc[T any, U any, R any] func(T, U) (R, error)
