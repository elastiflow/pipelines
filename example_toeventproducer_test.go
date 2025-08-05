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

// mockQueueClient simulates a client for an external message queue like Kafka or RabbitMQ.
// It implements the sources.EventProducer interface.
type mockQueueClient struct {
	wg   sync.WaitGroup
	name string
}

// Produce simulates sending a single message to the external queue.
func (c *mockQueueClient) Produce(msg string) {
	// In a real application, this would send the message over the network.
	log.Printf("[%s] Producing event: '%s'", c.name, msg)
	time.Sleep(50 * time.Millisecond) // Simulate network latency.
	c.wg.Done()                       // Signal that this message has been "sent".
}

func ExampleToEventProducer() {
	log.Println("Starting ToEventProducer example...")

	// 1. Setup context and error handling.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. Define the initial data and create a source.
	inputData := []string{"user_signed_up", "payment_processed", "user_deleted_account"}
	sourcer := sources.FromArray(inputData)

	// 3. Create an instance of our mock client that will act as the event destination.
	// We use a WaitGroup to ensure the main function doesn't exit before all async "produce" calls are done.
	mockClient := &mockQueueClient{name: "AuditLogStream"}
	mockClient.wg.Add(len(inputData))

	// 4. Create the sink using the ToEventProducer adapter.
	// This makes our mockQueueClient compatible with the pipeline.
	eventSink := sinks.ToEventProducer[string](mockClient)

	// 5. Build and run the pipeline.
	err := pipelines.New[string, string](ctx, sourcer, nil).
		Start(func(p datastreams.DataStream[string]) datastreams.DataStream[string] {
			// This stage transforms the raw event data into a structured log message.
			return datastreams.Map(p, func(event string) (string, error) {
				return fmt.Sprintf(`{"timestamp": "%s", "event": "%s"}`, time.Now().Format(time.RFC3339), event), nil
			})
		}).
		// The pipeline will call the Produce method on our mock client for each item.
		Sink(eventSink)

	if err != nil {
		log.Fatalf("Pipeline failed with error: %v", err)
	}

	// 6. Wait for the mock client to finish "sending" all its messages.
	log.Println("Pipeline finished. Waiting for producer to send all events...")
	mockClient.wg.Wait()

	log.Println("ToEventProducer example finished successfully.")
}
