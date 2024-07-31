# Pipelines

The `pipelines` module is a Go library designed to facilitate the creation and management of data processing pipelines. It provides a set of tools for flow control, error handling, and pipeline processes.

## Directory Structure

- **pipes/**: Contains functions used for flow control purposes.
- **procs/**: Contains functions used for pipeline processes, including error handling and a `props` struct.
- **examples/**: Contains example implementations of pipelines.

## Installation

To install the `pipelines` module, use the following command:

```sh
go get github.com/elastiflow/pipelines
```

## Usage

### Flow Control (`pipes`)

The `pipes` package provides functions to control the flow of data through the pipeline. These functions help in managing the data streams and ensuring that data is processed in the correct order.

### Pipeline Processes (`procs`)

The `procs` package contains functions that define the processes within the pipeline. This includes error handling mechanisms and the `props` struct, which is used to pass properties and configurations to the processes.

### Example Implementation

The `examples` folder contains a simple example of how to implement a pipeline using the `pipelines` module. Below is a brief overview of implementation a pipeline:

1. **Implement pipelines.Event**: Create a struct that implements `pipelines.Event`.
2. **Implement pipelines.Pipeline**" Create a struct that implements `pipelines.Pipeline` with a construction `New`.
3. **Constructing the Pipeline**: Make IO channels and use the `pipeline.New` function to create a new pipeline instance.
4. **Starting the Pipeline**: Open the pipeline and start processing events.
5. **Error Handling**: Use a goroutine to handle errors and close the pipeline if an error occurs.

Here is a snippet from the example implementation:

```go
package simple

import (
 "log/slog"

 "github.com/elastiflow/pipelines"
 "github.com/elastiflow/pipelines/examples/simple/pipeline"
)

func main() {
 // Create IO channels and construct the pipeline
 inChan := make(chan pipelines.Event)
 errChan := make(chan error)
 defer func() {
  close(inChan)
  close(errChan)
 }()
 pl := pipeline.New(inChan, errChan, 3)
	// Start a pipeline error handler
 go func(ch <-chan error) {
  for err := range ch {
   slog.Error("received error: ", slog.Any("error", err))
   pl.Close() // Close the pipeline on error, which breaks the pipeline loop below
  }
 }(errChan)
 // Open the pipeline and log output events
 for event := range pl.Open() {
  slog.Info("received event: ", slog.Any("event", event))
 }
}
```
