package pipes

import (
	"context"
	"net"
	"testing"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/mocks"
	"github.com/stretchr/testify/assert"
)

func TestBridge(t *testing.T) {
	tests := []struct {
		name      string
		input     [][]pipelines.Event
		want      []pipelines.Event
		cancelCtx bool
	}{
		{
			name: "single stream",
			input: [][]pipelines.Event{
				{
					mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
					mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
				},
			},
			want: []pipelines.Event{
				mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
			},
			cancelCtx: false,
		},
		{
			name: "multiple streams",
			input: [][]pipelines.Event{
				{
					mocks.NewMockEvent("1", 1, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}),
				},
				{
					mocks.NewMockEvent("2", 2, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2)}),
				},
			},
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
			inputStream := make(chan (<-chan pipelines.Event))
			outputStream := Bridge(ctx, inputStream)
			go func() {
				defer close(inputStream)
				for _, stream := range tt.input {
					eventStream := make(chan pipelines.Event, len(stream))
					for _, event := range stream {
						eventStream <- event
					}
					close(eventStream)
					inputStream <- eventStream
				}
			}()
			if tt.cancelCtx {
				cancel()
			}
			got := []pipelines.Event{}
			for event := range outputStream {
				if event != nil {
					got = append(got, event)
				}
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
