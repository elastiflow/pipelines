package event

import (
	"context"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipes"
)

// FanOut kicks off a num number of pipeline streams to process fn
func FanOut(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	fn pipes.Pipe,
	num uint16,
) []<-chan pipelines.Event {
	streams := make([]<-chan pipelines.Event, num)
	for i := uint16(0); i < num; i++ {
		streams[i] = fn(ctx, eventStream)
	}
	return streams
}
