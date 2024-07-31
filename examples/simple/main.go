package main

import (
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/examples/simple/pipeline"
)

func main() {
	// Create IO channels and construct the pipeline
	inChan := make(chan pipelines.Event)
	errChan := make(chan error)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	pl := pipeline.New(inChan, errChan, 3)
	// Start a pipeline error handler
	go func(ch <-chan error) {
		for err := range ch {
			slog.Error("received error: ", slog.Any("error", err))
			pl.Close() // Close the pipeline on error, which breaks the pipeline loop below
		}
	}(errChan)
	// Open the pipeline and log output events
	for event := range pl.Open() {
		slog.Info("received event: ", slog.Any("event", event))
	}
}
