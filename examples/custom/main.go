package custom

import (
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/examples/custom/pipeline"
)

func main() {
	// Create IO channels and construct the pipeline
	inChan := make(chan pipelines.Event)
	errChan := make(chan error)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	addrFilter := map[string]bool{
		"127.0.0.1:65000": true,
	}
	pl := pipeline.New(inChan, errChan, addrFilter, 3)
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
