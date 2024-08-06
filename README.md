# Pipelines

The `pipelines` module is a Go library designed to facilitate the creation and management of data processing pipelines. It provides a set of tools for flow control, error handling, and pipeline processes.

## Setup

To get started with the `pipelines` module, follow these steps:

* Get the `pipelines` module:

    ```sh
    go get github.com/elastiflow/pipelines
    ```

## Pipeline

A pipeline is a series of data processing stages connected by channels. Each stage (pipe.Pipe) is a function that performs a specific task and passes its output to the next stage. The `pipelines` module provides a flexible way to define and manage these stages.

## Pipe

The `pipe.Pipe` struct is the core of the `pipelines` module. It manages the flow of data through the pipeline stages and handles errors according to the provided parameters.

### Key Components

- **Params**: Used to pass arguments into `Pipe` methods.
- **ProcessFunc**: A user-defined function type used in a given `Pipe` stage via the `Pipe.Run()` method.

### Examples

Below are examples of how to use the `pipelines` module to create simple pipelines.

#### Squaring Numbers

This example demonstrates how to set up a pipeline that takes a stream of integers, squares each integer, and outputs the results.

```go
package main

import (
	"fmt"
	"log/slog"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipe"
)

// squareNumbers is a user defined pipe.Pipe function that squares an integer, will be registered and used in a pipe.Pipe.
func squareOdds(v int) (int, error) {
	if v%2 == 0 {
		return v, fmt.Errorf("even number error: %v", v)
	}
	return v * v, nil
}

// exampleProcess is a generic user defined pipelines.Pipeline function comprised of pipe.Pipe stages that will run in a pipelines.Pipeline.
func exampleProcess(p pipe.Pipe[int]) pipe.Pipe[int] {
	return p.OrDone(    // pipe.Pipe.OrDone will stop the pipeline if the input channel is closed. 
	    pipe.DefaultParams(), 
	).FanOut(   // pipe.Pipe.FanOut will run subsequent pipe.Pipe stages in parallel. 
	    pipe.Params{Num: 2},
	).Run(      // pipe.Pipe.Run will execute the pipe.Pipe process: "squareOdds". 
	    squareOdds,
            pipe.DefaultParams(),
	)           // pipe.Pipe.Out automatically FanIns to a single output channel if needed.
}

func main() {
	// Setup channels and cleanup
	inChan := make(chan int) 
	errChan := make(chan error, 10)
	defer func() {
		close(inChan)
		close(errChan)
	}()
	// Create new Pipeline properties
	props := pipelines.NewProps[int](
		inChan,
		errChan,
	)
	// Create a new Pipeline
	pl := pipelines.New[int](props, exampleProcess) 
	go func(errReceiver <-chan error) {             // Handle Pipeline errors
		defer pl.Close()
		for err := range errReceiver {
			if err != nil {
				slog.Error("demo error: " + err.Error())
				// return // if you wanted to close the pipeline during error handling.
			}
		}
	}(errChan)
	// Read Pipeline output
	for out := range pl.Open() {     // Open the Pipeline and read the output.
		slog.Info("received simple pipeline output", slog.Int("out", out))
	}
}
```