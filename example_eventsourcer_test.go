package pipelines_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// mockEventConsumer is a mock implementation of the sources.EventConsumer interface.
// It simulates a message queue consumer that produces a fixed number of integer messages.
type mockEventConsumer struct {
	messages chan int
}

// newMockEventConsumer creates a new mock consumer.
func newMockEventConsumer() sources.EventConsumer[int] {
	return &mockEventConsumer{
		messages: make(chan int),
	}
}

// Messages returns the channel from which messages can be read.
func (m *mockEventConsumer) Messages() <-chan int {
	return m.messages
}

// Run starts the consumer's message production loop.
// It sends integers 0 through 4 and then closes the channel.
func (m *mockEventConsumer) Run(ctx context.Context) {
	defer close(m.messages) // Ensure the channel is closed to terminate the pipeline.
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			log.Println("Mock consumer shutting down due to context cancellation.")
			return
		case m.messages <- i:
			log.Printf("Mock consumer produced message: %d\n", i)
			time.Sleep(100 * time.Millisecond) // Simulate work
		}
	}
	log.Println("Mock consumer finished producing messages.")
}

// MarkSuccess is called when a message is successfully processed by the pipeline.
func (m *mockEventConsumer) MarkSuccess(msg int) {
	log.Printf("✅ SUCCESS: Message '%d' was successfully processed and acknowledged.\n", msg)
}

// MarkError is called when a message fails processing in the pipeline.
func (m *mockEventConsumer) MarkError(msg int, err error) {
	log.Printf("❌ ERROR: Message '%d' failed processing and was marked with error: %v\n", msg, err)
}

func ExampleNewEventSourcer() {
	log.Println("🚀 Starting EventSourcer example pipeline...")

	// 1. Set up a context for graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2. Create an error channel to receive any pipeline errors.
	errs := make(chan error, 1)
	go func() {
		for err := range errs {
			log.Printf("Pipeline error received: %v", err)
		}
	}()

	//3. Create an output channel for the pipeline.
	out := make(chan string, 10)
	go func() {
		for msg := range out {
			log.Printf("Sink received message: %s\n", msg)
			// Simulate processing the message in the sink.
			time.Sleep(50 * time.Millisecond)
		}
		log.Println("Sink has finished processing all messages.")
	}()

	// 4. Instantiate our mock EventConsumer.
	consumer := newMockEventConsumer()

	// 5. Create an EventSourcer, adapting our consumer into a pipeline source.
	// The EventSourcer also implements MarkSuccess and MarkError by delegating to the consumer.
	sourcer := sources.NewEventSourcer[int](10, consumer)

	// 6. Create a sinker to handle the output of the pipeline.
	sinker := sinks.ToChannel(out)

	// 7. Build and run the pipeline.
	err := pipelines.New[int, string](
		ctx,
		sourcer,
		errs,
	).Start(func(p datastreams.DataStream[int]) datastreams.DataStream[string] {
		return datastreams.Map[int, string](
			p.FanOut(
				datastreams.Params{Num: 3},
			),
			func(i int) (string, error) {
				if i%2 == 0 {
					return fmt.Sprintf("Processed even number: %d", i), nil
				}
				// Simulate an error for odd numbers.
				return "", fmt.Errorf("error processing odd number: %d", i)
			},
		)
	}).Sink(sinker)

	if err != nil {
		log.Fatalf("Pipeline failed with error: %v", err)
	}

	log.Println("✅ Example finished successfully.")
}
