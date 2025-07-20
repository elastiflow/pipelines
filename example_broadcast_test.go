package pipelines_test

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams/sinks"
	"log"

	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
	"golang.org/x/sync/errgroup"
)

type listenerOutput struct {
	Index   int
	Message string
}

func ExamplePipeline_Broadcast() {
	log.Println("🚀 Starting Listen example...")
	g, ctx := errgroup.WithContext(context.Background())
	stdoutChan := make(chan listenerOutput, 10)
	go func() {
		for msg := range stdoutChan {
			log.Printf("[Listener %d] Received: %s", msg.Index, msg.Message)
		}
	}()

	// 2. Create a simple source from a slice.
	sourcer := sources.FromArray([]int{100, 200, 300})

	// 3. Create the initial DataStream and attach the WaitGroup.
	// The WaitGroup will be passed to all subsequent stages.
	pls := pipelines.New[int, listenerOutput](
		ctx,
		sourcer,
		make(chan error, 1),
	).Broadcast(3)

	// 4. Start the pipeline with a listener that processes each stream.
	g.Go(func() error {
		return pls[0].Start(func(p datastreams.DataStream[int]) datastreams.DataStream[listenerOutput] {
			return datastreams.Map[int, listenerOutput](
				p.OrDone().FanOut(
					datastreams.Params{Num: 2},
				).Run(
					func(i int) (int, error) {
						return i * i, nil // Square the input
					},
				),
				func(i int) (listenerOutput, error) {
					return listenerOutput{
						Index:   0, // This is the index of the listener
						Message: fmt.Sprintf("Processed value: %d", i),
					}, nil
				},
			)

		}).Sink(sinks.ToChannel[listenerOutput](stdoutChan))
	})

	g.Go(func() error {
		return pls[1].Start(func(p datastreams.DataStream[int]) datastreams.DataStream[listenerOutput] {
			return datastreams.Map[int, listenerOutput](
				p.OrDone().FanOut(
					datastreams.Params{Num: 2},
				).Run(
					func(i int) (int, error) {
						return i + 10, nil // Add 10 to the input
					},
				),
				func(i int) (listenerOutput, error) {
					return listenerOutput{
						Index:   1, // This is the index of the listener
						Message: fmt.Sprintf("Processed value: %d", i),
					}, nil
				},
			)

		}).Sink(sinks.ToChannel[listenerOutput](stdoutChan))
	})

	g.Go(func() error {
		return pls[2].Start(func(p datastreams.DataStream[int]) datastreams.DataStream[listenerOutput] {
			return datastreams.Map[int, listenerOutput](
				p.OrDone().FanOut(
					datastreams.Params{Num: 2},
				).Run(
					func(i int) (int, error) {
						return i - 5, nil // Subtract 5 from the input
					},
				),
				func(i int) (listenerOutput, error) {
					return listenerOutput{
						Index:   2, // This is the index of the listener
						Message: fmt.Sprintf("Processed value: %d", i),
					}, nil
				},
			)

		}).Sink(sinks.ToChannel[listenerOutput](stdoutChan))
	})

	// 5. Wait for all processing to finish.
	if err := g.Wait(); err != nil {
		log.Fatalf("Error in pipeline: %v", err)
	}

	log.Println("✅ Listen example finished successfully.")
}
