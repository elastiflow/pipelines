package event

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipes/event/mocks"
	"github.com/stretchr/testify/assert"
)

func TestOrDone(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		want      []pipelines.Event
		cancelCtx bool
	}{
		{
			name: "no context cancel",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
		{
			name: "context canceled",
			input: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
			},
			want:      []pipelines.Event{},
			cancelCtx: true,
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
			outputStream := OrDone(ctx, eventStream)
			got := []pipelines.Event{}
			for event := range outputStream {
				if event != nil {
					got = append(got, event)
				}
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
