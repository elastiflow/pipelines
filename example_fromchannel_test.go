package pipelines_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

func ExampleFromChannel() {
	log.Println("🚀 Starting FromChannel example pipeline...")

	// 1. Set up a context for graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 2. Create the source Go channel and a goroutine to populate it.
	sourceChan := make(chan int, 5)
	go func() {
		defer close(sourceChan) // Closing the channel signals the end of the source data.
		for i := 1; i <= 5; i++ {
			log.Printf("Source channel sending: %d", i)
			sourceChan <- i
		}
	}()

	// 3. Create the sourcer from the channel.
	sourcer := sources.FromChannel(sourceChan)

	// 4. Create a sink channel to receive the final results.
	out := make(chan string, 5)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range out {
			log.Printf("Sink received: %s", msg)
		}
	}()

	// 5. Build and run the pipeline.
	err := pipelines.New[int, string](ctx, sourcer, nil).
		Start(func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
			// The processing stage squares the number and converts it to a string.
			return datastreams.Map(p, func(i int) (string, error) {
				return fmt.Sprintf("The square of %d is %d", i, i*i), nil
			})
		}).
		Sink(sinks.ToChannel(out))

	if err != nil {
		log.Fatalf("Pipeline failed with error: %v", err)
	}

	wg.Wait()
	log.Println("✅ FromChannel example finished successfully.")
}
