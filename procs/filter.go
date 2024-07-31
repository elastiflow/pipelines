package procs

import (
	"context"
	"errors"

	"github.com/elastiflow/pipelines"
)

// Filter an event stream by defined *pipelines.Props
func Filter(
	ctx context.Context,
	eventStream <-chan pipelines.Event,
	errorStream chan<- error,
	props *pipelines.Props,
) <-chan pipelines.Event {
	outStream := make(chan pipelines.Event)
	go func(stream chan<- pipelines.Event) {
		defer close(outStream)
		for v := range eventStream {
			switch props.Type {
			case pipelines.Packet:
				if props.AddrMap == nil {
					select {
					case <-ctx.Done():
						return
					case errorStream <- errors.New("procs.Filter error: props.AddrMap should not be nil"):
					}
					continue
				}
				packet := v.(pipelines.PacketEvent)
				if _, ok := props.AddrMap[packet.Addr().String()]; !ok {
					select {
					case <-ctx.Done():
						return
					case stream <- packet:
					}
				}
			default:
				select {
				case <-ctx.Done():
					return
				case errorStream <- errors.New("procs.Filter error: props.Type should be Packet"):
				}
			}
		}
	}(outStream)
	return outStream
}
