package event

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipes/event/mocks"
	"github.com/stretchr/testify/assert"
)

func TestTake(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		num       uint16
		want      []pipelines.Event
		cancelCtx bool
	}{
		{
			name: "take 1 event",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			num: 1,
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
			},
			cancelCtx: false,
		},
		{
			name: "take 2 events",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			num: 2,
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
			eventStream := make(chan pipelines.Event)
			go func(eventSender chan<- pipelines.Event) {
				defer close(eventSender)
				for _, event := range tt.input {
					eventSender <- event
				}
			}(eventStream)
			if tt.cancelCtx {
				cancel()
			}
			outputStream := Take(ctx, eventStream, tt.num)
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
