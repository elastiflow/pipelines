package procs

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// FanOut kicks off a num number of pipeline streams to process fn
func FanOut(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	errorStream chan<- error,
	fn pipelines.Proc,
	props *pipelines.Props,
	num uint16,
) []<-chan pipelines.Event {
	streams := make([]<-chan pipelines.Event, num)
	for i := uint16(0); i < num; i++ {
		streams[i] = fn(ctx, eventStream, errorStream, props)
	}
	return streams
}
