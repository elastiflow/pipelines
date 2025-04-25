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

type KeyFunc[T any, K comparable] func(T) K

type WindowFunc[T any, R any] func([]T) (R, error)
