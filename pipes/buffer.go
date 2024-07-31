package pipes

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// Buffer is used for creating pipeline queues, which is especially useful for a pipeline's ingestion stage.
func Buffer(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	size uint16,
) <-chan pipelines.Event {
	if size == 0 {
		size = 1
	}
	bufferedStream := make(chan pipelines.Event, size)
	go func(bufStream chan<- pipelines.Event) {
		defer close(bufStream)
		for v := range eventStream {
			select {
			case <-ctx.Done():
				return
			case bufStream <- v:
			}
		}
	}(bufferedStream)
	return bufferedStream
}
