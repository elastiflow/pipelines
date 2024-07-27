package event

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// Take a specific number of events from the eventStream
func Take(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	num uint16,
) <-chan pipelines.Event {
	takeStream := make(chan pipelines.Event)
	go func(stream chan<- pipelines.Event) {
		defer close(stream)
		for i := uint16(0); i < num; i++ {
			select {
			case <-ctx.Done():
				return
			case stream <- <-eventStream:
			}
		}
	}(takeStream)
	return takeStream
}
