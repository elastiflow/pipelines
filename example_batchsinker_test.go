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

// workItem represents a single piece of data to be processed.
type workItem struct {
	ID int
}

// processedItem represents the result after processing a workItem.
type processedItem struct {
	ID     int
	Result string
}

func ExampleBatchSinker() {
	log.Println("Starting complex batch processing example...")

	const (
		numItemsToProcess = 10_000
		batchSize         = 1_000
		numWorkers        = 8 // Number of parallel workers for processing.
	)

	// 1. Setup context and error handling.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		for err := range errs {
			log.Printf("Pipeline error received: %v", err)
		}
	}()

	sourcer := sources.FromArray[workItem]([]workItem{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
		{ID: 5},
	})

	// 3. Configure the BatchSinker for efficient writes.
	var wg sync.WaitGroup
	onFlush := func(ctx context.Context, batch []processedItem) error {
		defer wg.Done()
		if len(batch) == 0 {
			return nil
		}
		log.Printf("⚡️ Flushing batch of %d items (e.g., IDs %d to %d)...", len(batch), batch[0].ID, batch[len(batch)-1].ID)
		// Simulate a bulk write operation to a database or API.
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// We must add to the WaitGroup *before* starting the pipeline,
	// as flush operations run asynchronously.
	expectedFlushes := (numItemsToProcess + batchSize - 1) / batchSize
	wg.Add(expectedFlushes)
	batchSinker := sinks.NewBatchSinker(onFlush, batchSize, errs)

	// 4. Build and run the pipeline with parallel processing.
	err := pipelines.New[workItem, processedItem](ctx, sourcer, errs).
		Start(func(p datastreams.DataStream[workItem]) datastreams.DataStream[processedItem] {
			// FanOut distributes the work among multiple concurrent workers.
			fannedOut := p.FanOut(datastreams.Params{Num: numWorkers})

			// Each worker executes this Map function.
			return datastreams.Map(fannedOut, func(item workItem) (processedItem, error) {
				// Simulate a time-consuming task like an I/O call or heavy computation.
				time.Sleep(5 * time.Millisecond)
				return processedItem{
					ID:     item.ID,
					Result: fmt.Sprintf("processed_data_for_%d", item.ID),
				}, nil
			})
		}).
		// The sink will automatically collect items until a batch is full, then call our onFlush function.
		Sink(batchSinker)

	if err != nil {
		log.Fatalf("Pipeline failed with error: %v", err)
	}

	// 5. Wait for all asynchronous flush operations to complete.
	log.Println("Pipeline finished. Waiting for final batches to flush...")
	wg.Wait()

	log.Println("✅ BatchSinker example finished successfully.")
}
