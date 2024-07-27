package event

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// OrDone checks to ensure that an external input stream is still running
func OrDone(
	ctx context.Context,
	c <-chan pipelines.Event,
) <-chan pipelines.Event {
	valStream := make(chan pipelines.Event)
	go func(stream chan<- pipelines.Event) {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case stream <- v:
				case <-ctx.Done():
				}
			}
		}
	}(valStream)
	return valStream
}
