package sinks

import (
	"context"

	"github.com/elastiflow/pipelines/datastreams"
)

// BatchSinker is a generic sink that collects items of type T into batches.
// When a batch reaches a configured size, it is flushed to a user-provided
// function for processing. It is designed to be used as a terminal stage in a pipeline.
type BatchSinker[T any] struct {
	onFlush func(context.Context, []T) error
	batch   []T
	errs    chan error
}

// NewBatchSinker creates and returns a new BatchSinker.
//
// **Parameters:**
//   - `onFlush`: The function to call when a batch is ready for processing.
//   - `batchSize`: The number of items to collect in a batch before flushing.
//   - `errs`: The channel where any errors from the `onFlush` function will be sent.
func NewBatchSinker[T any](onFlush func(context.Context, []T) error, batchSize int, errs chan error) *BatchSinker[T] {
	return &BatchSinker[T]{
		onFlush: onFlush,
		batch:   make([]T, 0, batchSize),
		errs:    errs,
	}
}

// Sink consumes items from a DataStream, adding them to an internal batch.
// It flushes the batch asynchronously in a new goroutine whenever the batch
// reaches its configured capacity. If the context is canceled, it performs
// a final synchronous flush of any remaining items before returning.
func (s *BatchSinker[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	for {
		select {
		case <-ctx.Done():
			// Flush any remaining items in the batch upon shutdown.
			s.flush(ctx, s.Next())
			return nil
		case val := <-ds.Out():
			s.batch = append(s.batch, val)
			if len(s.batch) == cap(s.batch) {
				go s.flush(ctx, s.Next())
			}
		}
	}
}

// flush executes the onFlush callback with the provided elements.
// If the callback returns an error, it is sent to the error channel.
// This function does nothing if the element slice is empty.
func (s *BatchSinker[T]) flush(ctx context.Context, elems []T) {
	if len(elems) == 0 {
		return
	}
	if err := s.onFlush(ctx, elems); err != nil {
		s.errs <- err
	}
}

// Next returns the current batch and efficiently resets the internal batch slice
// for the next set of items. The slice is reset by setting its length to zero,
// which avoids a new memory allocation for the underlying array.
func (s *BatchSinker[T]) Next() []T {
	next := s.batch
	s.batch = s.batch[:0]
	return next
}
