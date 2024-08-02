package pipelines

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines/pipe"
)

// ProcessFunc is a function type used by Pipeline to define the sequence of pipe.Pipe operations
type ProcessFunc[T any] func(pipe.Pipe[T], *pipe.Params) pipe.Pipe[T]

// Pipeline is a struct that defines a generic stream process
type Pipeline[T any] struct {
	process      ProcessFunc[T]
	pipeRegistry pipe.ProcessRegistry[T]
	startTime    time.Time
	errorChan    chan<- error // Streams errors from the Pipeline to the caller
	inputChan    <-chan T     // Streams input data to the Pipeline for processing
	fanNum       int
	cancelFunc   context.CancelFunc
}

// New constructs a new Pipeline of a given type by passing in the properties and process function
func New[T any](
	props *Props[T],
	process ProcessFunc[T],
) *Pipeline[T] {
	return &Pipeline[T]{
		process:      process,
		pipeRegistry: props.pipeRegister,
		inputChan:    props.inputChan,
		errorChan:    props.errChan,
		fanNum:       props.fanNum,
	}
}

// Open a Pipeline with a given set of parameters and return the output channel
func (p *Pipeline[T]) Open(params *pipe.Params) <-chan T {
	if params == nil {
		params = &pipe.Params{Num: p.fanNum}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	return p.process(
		pipe.New[T](ctx, p.pipeRegistry, p.inputChan, p.errorChan),
		params,
	).Consume()
}

// Tee a Pipeline with a given set of parameters and return two output channels with copied data
func (p *Pipeline[T]) Tee(params *pipe.Params) (<-chan T, <-chan T) {
	if params == nil {
		params = &pipe.Params{Num: p.fanNum}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	out1, out2 := p.process(
		pipe.New[T](ctx, p.pipeRegistry, p.inputChan, p.errorChan),
		params,
	).Tee(params)
	return out1.Consume(), out2.Consume()
}

// Close a Pipeline and safely stop processing
func (p *Pipeline[T]) Close() {
	p.cancelFunc()
}
