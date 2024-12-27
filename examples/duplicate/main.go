package main

import (
	"context"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
)

func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

func duplicateProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
	return p.Broadcast(
		datastreams.Params{Num: 2},
	).FanIn() // Broadcasting by X then Fanning In will create X duplicates per T.
}

func main() {
	errChan := make(chan error)
	pl := pipelines.New[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)), // Create a new Source
		errChan,
	).Start(duplicateProcess)

	defer pl.Close()

	go func(errReceiver <-chan error) { // Handle Pipeline errors
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
	 received simple pipeline output out=9
	*/

}
