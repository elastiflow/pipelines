package pipelines_test

import (
	"context"

	"log"
	"strings"
	"sync"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

func ExampleFromArray() {
	log.Println("Starting FromSlice example pipeline...")

	// 1. Set up a context for graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 2. Define the input data slice.
	inputData := []string{"apple", "banana", "cherry"}
	sourcer := sources.FromArray(inputData)

	// 3. Create a sink that will collect results. We'll use a simple channel.
	out := make(chan string, len(inputData))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range out {
			log.Printf("Sink received: %s", msg)
		}
	}()

	// 4. Build and run the pipeline.
	err := pipelines.New[string, string](ctx, sourcer, nil). // No error channel needed for this simple case.
									Start(func(p datastreams.DataStream[string]) datastreams.DataStream[string] {
			// The processing stage converts each string to uppercase.
			return datastreams.Map(p, func(s string) (string, error) {
				return strings.ToUpper(s), nil
			})
		}).
		Sink(sinks.ToChannel(out))

	if err != nil {
		log.Fatalf("Pipeline failed with error: %v", err)
	}

	wg.Wait() // Wait for the sink to finish processing.
	log.Println("FromSlice example finished successfully.")
}
