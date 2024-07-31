package pipes

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/mocks"
	"github.com/stretchr/testify/assert"
)

func TestFanOut(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		numOut    int
		want      []pipelines.Event
		cancelCtx bool
	}{
		{
			name: "single output channel",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			numOut: 1,
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
		{
			name: "multiple output channels",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			numOut: 2,
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
		{
			name: "multiple output PacketEvent channels",
			input: []pipelines.Event{
				mocks.NewMockPacketEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockPacketEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			numOut: 2,
			want: []pipelines.Event{
				mocks.NewMockPacketEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockPacketEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			eventStream := make(chan pipelines.Event, len(tt.input))
			go func() {
				defer close(eventStream)
				for _, event := range tt.input {
					eventStream <- event
				}
			}()
			if tt.cancelCtx {
				cancel()
			}
			testPipe := func(ctx context.Context, eventStream <-chan pipelines.Event) <-chan pipelines.Event {
				return eventStream
			}
			outputStreams := FanOut(ctx, eventStream, testPipe, uint16(tt.numOut))
			if len(outputStreams) != tt.numOut {
				t.Errorf("expected %d output streams, got %d", tt.numOut, len(outputStreams))
				return
			}
			var got []pipelines.Event
			for _, outputStream := range outputStreams {
				for event := range outputStream {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
