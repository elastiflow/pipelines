package event

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// ToPacket converts an Event Stream into a PacketEvent Stream
func ToPacket(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
) <-chan pipelines.PacketEvent {
	packetStream := make(chan pipelines.PacketEvent)
	go func(stream chan<- pipelines.PacketEvent) {
		defer close(packetStream)
		for v := range eventStream {
			select {
			case <-ctx.Done():
				return
			case stream <- v.(pipelines.PacketEvent):
			}
		}
	}(packetStream)
	return packetStream
}
