package datastreams

import (
	"context"
	"sync"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
)

// DataStream is a struct that defines a generic stream process stage.
// It manages one or more input channels (inStreams) and a shared error stream.
type DataStream[T any] struct {
	ctx       context.Context
	errStream chan<- error
	inStreams []<-chan T
	wg        *sync.WaitGroup
}

// New constructs a new DataStream of a given type by passing in a context, an input
// channel, and an error channel. Additional channels can be introduced internally
// via transformations like FanOut.
func New[T any](
	ctx context.Context,
	inStream <-chan T,
	errStream chan<- error,
) DataStream[T] {
	return DataStream[T]{
		ctx:       ctx,
		errStream: errStream,
		inStreams: []<-chan T{inStream},
		wg:        nil,
	}
}

// WithWaitGroup attaches a WaitGroup to this DataStream, returning a copy.
func (p DataStream[T]) WithWaitGroup(wg *sync.WaitGroup) DataStream[T] {
	p.wg = wg
	return p
}

// incrementWaitGroup checks to see if a wait group is attached to the DataStream and increments it by delta.
func (p DataStream[T]) incrementWaitGroup(delta int) {
	if p.wg != nil {
		p.wg.Add(delta)
	}
}

func (p DataStream[T]) decrementWaitGroup() {
	if p.wg != nil {
		p.wg.Done()
	}
}

// Out returns the single output channel of this DataStream.
// If the DataStream has multiple input channels, it automatically FanIns them into a single output.
func (p DataStream[T]) Out() <-chan T {
	if len(p.inStreams) == 1 {
		return p.inStreams[0]
	}
	return p.FanIn().inStreams[0] // If multiple streams, FanIn to a single stream
}

// Sink outputs DataStream values to a defined Sinker in a separate goroutine.
// This allows the pipeline to continue processing asynchronously.
func (p DataStream[T]) Sink(
	sinker Sinker[T],
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	p.incrementWaitGroup(1)
	go func(ctx context.Context, sink Sinker[T], ds DataStream[T], parameters Params) {
		if p.wg != nil {
			defer p.wg.Done()
		}
		if err := sink.Sink(ctx, ds); err != nil {
			p.errStream <- newSinkError(parameters.SegmentName, err)
		}
	}(p.ctx, sinker, p, param)
	return p
}

// Run executes a user defined process function on the input stream(s).
// Each input channel is handled in its own goroutine, writing processed results
// to a newly created set of output channels. Errors can be skipped if SkipError is set.
func (p DataStream[T]) Run(
	proc ProcessFunc[T],
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		p.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- T, process ProcessFunc[T]) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			defer close(outStream)
			for {
				select {
				case <-p.ctx.Done():
					return
				case v, ok := <-inStream:
					if !ok {
						return
					}
					val, err := process(v)
					if err != nil {
						p.errStream <- newRunError(param.SegmentName, err)
						if param.SkipError {
							continue
						}
					}
					select {
					case outStream <- val:
					case <-p.ctx.Done():
						return
					}
				}
			}
		}(p.inStreams[i], outChannels[i], proc)
	}
	return nextPipe
}

// Filter applies a user defined function to each value in the input stream(s)
// and only returns values that pass the filter check (true). If an error occurs,
// the item is dropped.
func (p DataStream[T]) Filter(filter FilterFunc[T], params ...Params) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		p.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- T) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			defer close(outStream)
			for val := range inStream {
				pass, err := filter(val)
				if err != nil {
					p.errStream <- newFilterError(param.SegmentName, err)
					continue
				}
				if !pass {
					continue
				}
				select {
				case outStream <- val:
				case <-p.ctx.Done():
					return
				}
			}
		}(p.inStreams[i], outChannels[i])
	}
	return nextPipe
}

// Take returns only the first N items from the input streams. If multiple input
// streams exist, each is read up to N items, meaning total items could be N * numberOfStreams.
func (p DataStream[T]) Take(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		p.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- T) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			defer close(outStream)
			for j := 0; j < param.Num; j++ {
				select {
				case <-p.ctx.Done():
					return
				case val, ok := <-inStream:
					if ok {
						outStream <- val
					}
				}
			}
		}(p.inStreams[i], outChannels[i])
	}
	return nextPipe
}

// FanOut duplicates the number of output channels by param.Num, distributing
// incoming items in a round-robin manner across all new channels.
func (p DataStream[T]) FanOut(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(fanOut, param)
	p.incrementWaitGroup(1)
	go func(inStream <-chan T, outStreams pipes.Senders[T]) {
		if p.wg != nil {
			defer p.wg.Done()
		}
		defer outChannels.Close()
		var counter int
		for val := range inStream {
			select {
			case outStreams[counter%len(outStreams)] <- val:
				counter++
			case <-p.ctx.Done():
				return
			}
		}
	}(p.inStreams[0], outChannels.Senders())
	return nextPipe
}

// FanIn merges a slice of input channels into a single output channel.
func (p DataStream[T]) FanIn(
	params ...Params,
) DataStream[T] {
	// If there's only one input stream, then no need to fan in
	if len(p.inStreams) == 1 {
		return p
	}

	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(fanIn, param)
	outSenders := outChannels.Senders()
	var wg sync.WaitGroup
	multiplex := func(c <-chan T) {
		defer wg.Done()
		for i := range c {
			select {
			case outSenders[0] <- i:
			case <-p.ctx.Done():
				return
			}
		}
	}
	// Select from all the pipes
	wg.Add(len(p.inStreams))
	for _, c := range p.inStreams {
		go multiplex(c)
	}
	// Wait for all the reads to complete
	p.incrementWaitGroup(1)
	go func() {
		wg.Wait()
		if p.wg != nil {
			defer p.wg.Done()
		}
		outChannels.Close()
	}()
	return nextPipe
}

// OrDone terminates if the input stream is closed or context is done,
// effectively passing items through until the upstream channel signals completion.
func (p DataStream[T]) OrDone(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		p.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- T) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			defer close(outStream)
			for {
				select {
				case <-p.ctx.Done():
					return
				case v, ok := <-inStream:
					if !ok {
						return
					}
					select {
					case outStream <- v:
					case <-p.ctx.Done():
						return
					}
				}
			}
		}(p.inStreams[i], outChannels[i])
	}
	return nextPipe
}

// Broadcast sends each item to param.Num new channels, effectively duplicating
// every item across all output channels.
func (p DataStream[T]) Broadcast(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(broadcast, param)
	p.incrementWaitGroup(1)
	go func(inStream <-chan T, outStreams pipes.Senders[T]) {
		if p.wg != nil {
			defer p.wg.Done()
		}
		defer outChannels.Close()
		for val := range inStream {
			for i := 0; i < len(outStreams); i++ {
				in := val
				select {
				case outStreams[i] <- in:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}(p.inStreams[0], outChannels.Senders())
	return nextPipe
}

// Listen allows you to subscribe to a DataStream, effectively
// creating a new DataStream that listens to the same input channel.
// This allows you to broadcast the same input stream to multiple listeners.
func (p DataStream[T]) Listen(
	listenerIndex int,
	params ...Params,
) DataStream[T] {
	if listenerIndex < 0 || listenerIndex >= len(p.inStreams) {
		return DataStream[T]{
			ctx:       p.ctx,
			errStream: p.errStream,
			inStreams: []<-chan T{},
			wg:        p.wg,
		}
	}

	param := applyParams(params...)
	nextPipe, outChannels := next[T](fanIn, param, 1, p.ctx, p.errStream, p.wg)
	p.incrementWaitGroup(1)
	go func(inStream <-chan T, outStreams pipes.Senders[T]) {
		if p.wg != nil {
			defer p.wg.Done()
		}
		defer outChannels.Close()
		for val := range inStream {
			for i := 0; i < len(outStreams); i++ {
				in := val
				select {
				case outStreams[i] <- in:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}(p.inStreams[listenerIndex], outChannels.Senders())
	return nextPipe
}

// Tee splits values coming in from a single channel so that you can send them
// off into two separate DataStream outputs.
func (p DataStream[T]) Tee(
	params ...Params,
) (DataStream[T], DataStream[T]) {
	param := applyParams(params...)
	nextPipe1, outChannels1 := p.nextT(standard, param)
	nextPipe2, outChannels2 := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		p.incrementWaitGroup(1)
		go func(in <-chan T, o1 chan<- T, o2 chan<- T) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			defer close(o1)
			defer close(o2)
			for val := range orDone(p.ctx, in) {
				var ch1, ch2 = o1, o2
				for i := 0; i < 2; i++ {
					select {
					case <-p.ctx.Done():
						return
					case ch1 <- val:
						ch1 = nil
					case ch2 <- val:
						ch2 = nil
					}
				}
			}
		}(p.inStreams[i], outChannels1[i], outChannels2[i])
	}
	return nextPipe1, nextPipe2
}

func (p DataStream[T]) nextT(pipeType pipeType, params Params) (DataStream[T], pipes.Pipes[T]) {
	return next[T](pipeType, params, len(p.inStreams), p.ctx, p.errStream, p.wg)
}

// Map is a package-level function that transforms each item from T to U using a TransformFunc.
func Map[T any, U any](
	ds DataStream[T],
	transformFunc TransformFunc[T, U],
	params ...Params,
) DataStream[U] {
	param := applyParams(params...)
	nextPipe, outChannels := next[U](standard, param, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	for i := 0; i < len(ds.inStreams); i++ {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- U, transformer TransformFunc[T, U]) {
			if ds.wg != nil {
				defer ds.wg.Done()
			}
			defer close(outStream)
			for {
				select {
				case <-ds.ctx.Done():
					return
				case v, ok := <-inStream:
					if !ok {
						return
					}
					val, err := transformer(v)
					if err != nil {
						ds.errStream <- newMapError(param.SegmentName, err)
						if param.SkipError {
							continue
						}
					}
					select {
					case outStream <- val:
					case <-ds.ctx.Done():
						return
					}
				}
			}
		}(ds.inStreams[i], outChannels[i], transformFunc)
	}
	return nextPipe
}

func Expand[T any, U any](
	ds DataStream[T],
	expandFunc ExpandFunc[T, U],
	params ...Params,
) DataStream[U] {
	param := applyParams(params...)
	nextPipe, outChannels := next[U](standard, param, len(ds.inStreams), ds.ctx, ds.errStream, ds.wg)
	for i := 0; i < len(ds.inStreams); i++ {
		ds.incrementWaitGroup(1)
		go func(inStream <-chan T, outStream chan<- U, expander ExpandFunc[T, U]) {
			if ds.wg != nil {
				defer ds.wg.Done()
			}
			defer close(outStream)
			for {
				select {
				case <-ds.ctx.Done():
					return
				case v, ok := <-inStream:
					if !ok {
						return
					}
					outputs, err := expander(v)
					if err != nil {
						ds.errStream <- newExpandError(param.SegmentName, err)
						if param.SkipError {
							continue
						}
					}
					for _, output := range outputs {
						select {
						case outStream <- output:
						case <-ds.ctx.Done():
							return
						}
					}
				}
			}
		}(ds.inStreams[i], outChannels[i], expandFunc)
	}
	return nextPipe
}

func next[T any](
	pipeType pipeType,
	params Params,
	chanCount int,
	ctx context.Context,
	errStream chan<- error,
	wg *sync.WaitGroup,
) (DataStream[T], pipes.Pipes[T]) {
	switch pipeType {
	case fanOut, broadcast:
		chanCount = params.Num
	case fanIn:
		chanCount = 1
	default:
	}
	streams := make(pipes.Pipes[T], chanCount)
	streams.Initialize(params.BufferSize)
	return DataStream[T]{
			ctx:       ctx,
			errStream: errStream,
			inStreams: streams.Receivers(),
			wg:        wg,
		},
		streams
}

// orDone helps forward values until context is canceled or the stream ends.
func orDone[T any](
	ctx context.Context,
	c <-chan T,
) <-chan T {
	valStream := make(chan T)
	go func(stream chan<- T) {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case stream <- v:
				case <-ctx.Done():
				}
			}
		}
	}(valStream)
	return valStream
}

func applyParams(params ...Params) Params {
	var p Params
	for _, param := range params {
		p = param
	}
	return p
}
