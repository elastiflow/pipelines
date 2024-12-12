package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines/pipe"
)

func mapFunc(v int) (string, error) {
	return fmt.Sprintf("value: %v", v), nil
}

func postProcessFunc(p string) (string, error) {
	return fmt.Sprintf("I was multiplied, %v", p), nil
}

func preprocessFunc(p int) (int, error) {
	return p * 2, nil
}

func seedPipeline(inChan chan<- int) {
	for i := 0; i < 10; i++ {
		inChan <- i
	}
}

func main() {
	inChan := make(chan int)
	errChan := make(chan error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()

	pipe_ := pipe.NewMap[int, string](
		context.Background(),
		inChan,
		errChan,
	).FanOut(pipe.Params{Num: 3}).
		Run(preprocessFunc).
		Map(mapFunc).
		Run(postProcessFunc).
		OrDone().
		Out()

	go func(errReceiver <-chan error) {
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(errChan)
	go seedPipeline(inChan)
	var i int
	for out := range pipe_ {
		if i == 9 {
			slog.Info("received simple pipeline output", slog.String("out", out))
			return
		}
		slog.Info("received simple pipeline output", slog.String("out", out))
		i++
	}
}
