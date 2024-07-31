package pipes

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// Bridge destructures a channel of channels into a simple channel allowing
//
//	for use of the channel of channels from within a single range statement
func Bridge(
	ctx context.Context,
	eventStreams <-chan <-chan pipelines.Event,
) <-chan pipelines.Event {
	valStream := make(chan pipelines.Event)
	go func(bridge chan<- pipelines.Event) {
		defer close(bridge)
		for {
			var stream <-chan pipelines.Event
			select {
			case maybeStream, ok := <-eventStreams:
				if !ok {
					return
				}
				stream = maybeStream
			case <-ctx.Done():
				return
			}
			for val := range OrDone(ctx, stream) {
				select {
				case bridge <- val:
				case <-ctx.Done():
				}
			}
		}
	}(valStream)
	return valStream
}
