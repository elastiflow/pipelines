package sinks

import (
	"context"
	"github.com/elastiflow/pipelines/datastreams"
)

type BatchSinker[T any] struct {
	onFlush func(context.Context, []T) error
	batch   []T
	errs    chan error
}

func NewSinker[T any](onFlush func(context.Context, []T) error, batchSize int, errs chan error) *BatchSinker[T] {
	return &BatchSinker[T]{
		onFlush: onFlush,
		batch:   make([]T, 0, batchSize),
	}
}

func (s *BatchSinker[T]) Sink(ctx context.Context, ds datastreams.DataStream[T]) error {
	for {
		select {
		case <-ctx.Done():
			// Flush any remaining items in the batch
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

func (s *BatchSinker[T]) flush(ctx context.Context, elems []T) {
	if err := s.onFlush(ctx, elems); err != nil {
		s.errs <- err
	}
}

func (s *BatchSinker[T]) Next() []T {
	next := s.batch
	s.batch = s.batch[:0]
	return next
}
