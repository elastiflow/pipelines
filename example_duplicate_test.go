package pipelines_test

import (
	"context"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

// createIntArr creates a simple slice of int values from 0..num-1.
func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

// duplicateProcess demonstrates a pipeline stage that broadcasts each value to two streams
// and then fans them back in (duplicates).
func duplicateProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
	// Broadcasting by 2 then fanning in merges them back, effectively duplicating each item.
	return p.Broadcast(
		datastreams.Params{Num: 2},
	).FanIn()
}

func Example_duplicate() {
	errChan := make(chan error)
	pl := pipelines.New[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)), // Create a new Source from slice
		errChan,
	).Start(duplicateProcess)
	defer func() {
		close(errChan)
		pl.Close()
	}()

	// Handle pipeline errors in a separate goroutine
	go func(errReceiver <-chan error) {
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				return
			}
		}
	}(errChan)

	// Read pipeline output
	for out := range pl.Out() {
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}

	// Output (example):
	//   received simple pipeline output out=0
	//   received simple pipeline output out=0
	//   received simple pipeline output out=1
	//   received simple pipeline output out=1
	//   ...
	//   received simple pipeline output out=9
	//   received simple pipeline output out=9
}
