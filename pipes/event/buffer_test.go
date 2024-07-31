package event

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipes/event/mocks"
	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		size      uint16
		want      []pipelines.Event
		cancelCtx bool
	}{
		{
			name: "buffer size 1",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			size: 1,
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
		{
			name: "buffer size 2",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			size: 2,
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			eventStream := make(chan pipelines.Event, len(tt.input))
			outputStream := Buffer(ctx, eventStream, tt.size)
			go func() {
				defer close(eventStream)
				for _, event := range tt.input {
					eventStream <- event
				}
			}()
			if tt.cancelCtx {
				cancel()
			}
			var got []pipelines.Event
			for event := range outputStream {
				if event != nil {
					got = append(got, event)
				}
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
