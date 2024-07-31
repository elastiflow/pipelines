package procs

import (
	"context"
	"fmt"

	"github.com/elastiflow/pipelines"
)

// ToSNMPPacket converts an Event Stream into a PacketEvent Stream
func ToSNMPPacket(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	errorStream chan<- error,
	props *pipelines.Props,
) <-chan pipelines.Event {
	packetStream := make(chan pipelines.Event)
	go func(stream chan<- pipelines.Event) {
		defer close(packetStream)
		for v := range eventStream {
			if props.Type == pipelines.Packet {
				packet := v.(pipelines.PacketEvent)
				if err := packet.ToPacket(); err != nil { // converts underlying to snmp.Packet
					errorStream <- fmt.Errorf("proc.ToSNMPPacket error: %w", err)
				}
				select {
				case <-ctx.Done():
					return
				case stream <- packet:
				}
			}
		}
	}(packetStream)
	return packetStream
}
