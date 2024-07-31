package pipes

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/mocks"
	"github.com/stretchr/testify/assert"
)

func TestTee(t *testing.T) {
	tests := []struct {
		name      string
		input     []pipelines.Event
		numOut    int
		want      []pipelines.Event
		cancelCtx bool
	}{
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
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			testStream := make(chan pipelines.Event, len(tt.want))
			defer func(stream chan pipelines.Event) {
				cancel()
				close(stream)
			}(testStream)
			eventStream := make(chan pipelines.Event)
			go func() {
				defer close(eventStream)
				for _, event := range tt.input {
					eventStream <- event
				}
			}()
			if tt.cancelCtx {
				cancel()
			}
			outputStreamA, outputStreamB := Tee(ctx, eventStream)
			go func(stream chan<- pipelines.Event) {
				for event := range outputStreamA {
					stream <- event
				}
			}(testStream)
			go func(stream chan<- pipelines.Event) {
				for event := range outputStreamB {
					stream <- event
				}
			}(testStream)
			var got []pipelines.Event
			for event := range testStream {
				got = append(got, event)
				if len(got) == len(tt.want) {
					assert.ElementsMatch(t, tt.want, got)
					return
				}
			}
		})
	}
}
