# Pipelines

The `pipelines` module is a Go library designed to facilitate the creation and management of data processing pipelines. It provides a set of tools for flow control, error handling, and pipeline processes.

## Setup

To get started with the `pipelines` module, follow these steps:

* Get the `pipelines` module:

    ```sh
    go get github.com/elastiflow/pipelines
    ```

## Documentation

1. Ensure godocs are installed:
    ```sh
    go install -v golang.org/x/tools/cmd/godoc@latest
    ```

2. Run make docs command to start godocs on port 6060 locally:
    ```sh
    make docs
    ```

3. Once running, visit [GoDocs](http://localhost:6060/pkg/github.com/elastiflow/pipelines/) to view the latest documentation locally.

## Pipeline

A pipeline is a series of data processing stages connected by channels. Each stage (pipe.DataStream) is a function that performs a specific task and passes its output to the next stage. The `pipelines` module provides a flexible way to define and manage these stages.

## DataStream

The `pipe.DataStream` struct is the core of the `pipelines` module. It manages the flow of data through the pipeline stages and handles errors according to the provided parameters.

### Key Components

- **Params**: Used to pass arguments into `DataStream` methods.
- **ProcessFunc**: A user-defined function type used in a given `DataStream` stage via the `DataStream.Run()` method.


### Examples

Below are examples of how to use the `pipelines` module to create simple pipelines.

#### Squaring Numbers

This example demonstrates how to set up a pipeline that takes a stream of integers, squares each integer, and outputs the results.

```go
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/sources"
)

func createIntArr(num int) []int {
	var arr []int
	for i := 0; i < num; i++ {
		arr = append(arr, i)
	}
	return arr
}

func squareOdds(v int) (int, error) {
	if v%2 == 0 {
		return v, fmt.Errorf("even number error: %v", v)
	}
	return v * v, nil
}

func exProcess(p datastreams.DataStream[int]) datastreams.DataStream[int] {
	return p.OrDone().FanOut(
		datastreams.Params{Num: 2},
	).Run(
		squareOdds,
	)
}

func main() {
	errChan := make(chan error, 10)
	defer close(errChan)

	pl := pipelines.FromSource[int, int]( // Create a new Pipeline
		context.Background(),
		sources.FromArray(createIntArr(10)), // Create a source to start the pipeline
		errChan,
	).With(exProcess)

	go func(errReceiver <-chan error) { // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(pl.Errors())
	for out := range pl.Out() { // Read Pipeline output
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}
}
```