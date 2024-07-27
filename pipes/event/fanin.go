package event

import (
	"context"
	"sync"

	"github.com/elastiflow/pipelines"
)

// FanIn concurrent Event into a single stream
func FanIn(
	ctx context.Context,
	channels ...<-chan pipelines.Event,
) <-chan pipelines.Event {
	var wg sync.WaitGroup
	multiplexedStream := make(chan pipelines.Event)
	multiplex := func(c <-chan pipelines.Event) {
		defer wg.Done()
		for i := range c {
			select {
			case <-ctx.Done():
				return
			case multiplexedStream <- i:
			}
		}
	}
	// Select from all the channels
	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}
	// Wait for all the reads to complete
	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()
	return multiplexedStream
}
