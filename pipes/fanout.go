package pipes

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// FanOut kicks off a num number of pipeline streams to process fn
func FanOut(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	fn pipelines.Pipe,
	num uint16,
) []<-chan pipelines.Event {
	streams := make([]<-chan pipelines.Event, num)
	for i := uint16(0); i < num; i++ {
		streams[i] = fn(ctx, eventStream)
	}
	return streams
}
