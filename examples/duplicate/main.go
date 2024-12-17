package main

import (
	"context"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/errors"
	"github.com/elastiflow/pipelines/pipe"
	"github.com/elastiflow/pipelines/sources"
)

func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

func duplicateProcess(p pipe.DataStream[int]) pipe.DataStream[int] {
	return p.Broadcast(
		pipe.Params{Num: 2},
	).FanIn() // Broadcasting by X then Fanning In will create X duplicates per T.
}

func main() {
	errChan := make(chan errors.Error)
	pl := pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)), // Create a new Source
		errChan,
	).With(duplicateProcess)

	defer pl.Close()

	go func(errReceiver <-chan errors.Error) { // Handle Pipeline errors
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				return
			}
		}
	}(errChan)
	for out := range pl.Out() { // Read Pipeline output
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}

	// Output:
	/*
	 received simple pipeline output out=0
	 received simple pipeline output out=0
	 received simple pipeline output out=1
	 received simple pipeline output out=1
	 received simple pipeline output out=2
	 received simple pipeline output out=2
	 received simple pipeline output out=3
	 received simple pipeline output out=4
	 received simple pipeline output out=3
	 received simple pipeline output out=4
	 received simple pipeline output out=5
	 received simple pipeline output out=5
	 received simple pipeline output out=6
	 received simple pipeline output out=6
	 received simple pipeline output out=7
	 received simple pipeline output out=7
	 received simple pipeline output out=8
	 received simple pipeline output out=8
	 received simple pipeline output out=9
	*/

}
