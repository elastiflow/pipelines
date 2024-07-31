package procs

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/mocks"
	"github.com/stretchr/testify/assert"
)

func TestFilterPacket(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		want      []pipelines.Event
		props     *pipelines.Props
		cancelCtx bool
	}{
		{
			name: "filter packet event by addr",
			input: []pipelines.Event{
				mocks.NewMockPacketEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockPacketEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			want: []pipelines.Event{
				mocks.NewMockPacketEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			props: pipelines.NewPacketProps(
				map[string]bool{"192.0.2.1:0": true},
			),
			cancelCtx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			eventStream := make(chan pipelines.Event, len(tt.input))
			errStream := make(chan error)
			go func() {
				defer close(eventStream)
				defer close(errStream)
				for _, event := range tt.input {
					eventStream <- event
				}
			}()
			if tt.cancelCtx {
				cancel()
			}
			outputStream := Filter(ctx, eventStream, errStream, tt.props)
			var got []pipelines.Event
			for event := range outputStream {
				got = append(got, event)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
