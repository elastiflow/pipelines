package datastreams

import (
	"context"
	"math/rand" // nosemgrep
	"sync"
	"time"
)

// DataStream is a struct that defines a generic stream process stage
type DataStream[T any] struct {
	ctx       context.Context
	errStream chan<- error
	inStreams []<-chan T
}

// New constructs a new DataStream of a given type by passing in a pipelines.Pipeline context, registry, and IO streams
func New[T any](
	ctx context.Context,
	inStream <-chan T,
	errStream chan<- error,
) DataStream[T] {
	return DataStream[T]{
		ctx:       ctx,
		errStream: errStream,
		inStreams: []<-chan T{inStream},
	}
}

// Out the data outputted from a DataStream
func (p DataStream[T]) Out() <-chan T {
	if len(p.inStreams) == 1 {
		return p.inStreams[0]
	}
	return p.FanIn().inStreams[0] // If multiple streams, FanIn to a single stream
}

// Sink outputs DataStream values to a defined Sinker
func (p DataStream[T]) Sink(
	sinker Sinker[T],
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	go func(ctx context.Context, sink Sinker[T], ds DataStream[T], parameters Params) {
		if err := sink.Sink(ctx, ds); err != nil {
			p.errStream <- newSinkError(parameters.SegmentName, err)
		}
	}(p.ctx, sinker, p, param)
	return p
}

// Run executes a user defined process function on the input stream(s)
func (p DataStream[T]) Run(
	proc ProcessFunc[T],
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		go func(inStream <-chan T, outStream chan<- T, process ProcessFunc[T]) {
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

// Filter applies a user defined function to each value in the input stream(s) and only returns values that pass the filter
func (p DataStream[T]) Filter(filter FilterFunc[T], params ...Params) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		go func(inStream <-chan T, outStream chan<- T) {
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

// Take a specific number of inputs from the inputStream(s)
func (p DataStream[T]) Take(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		go func(inStream <-chan T, outStream chan<- T) {
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

// FanOut kicks off a number of DataStream streams and round robins the input values
func (p DataStream[T]) FanOut(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(fanOut, param)
	go func(inStream <-chan T, outStreams senders[T]) {
		defer outChannels.Close()
		// Generate a weak random int ONLY to use in load balancing between outStreams
		r := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
		for val := range inStream {
			select {
			case outStreams[r.Intn(len(outStreams))] <- val:
			case <-p.ctx.Done():
				return
			}
		}
	}(p.inStreams[0], outChannels.Senders())
	return nextPipe
}

// FanIn merges a slice of input streams into a single output stream
func (p DataStream[T]) FanIn(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(fanIn, param)
	outSenders := outChannels.Senders()
	var wg sync.WaitGroup
	multiplex := func(c <-chan T) {
		defer wg.Done()
		for i := range c {
			select {
			case <-p.ctx.Done():
				return
			case outSenders[0] <- i: // Hard coded to index 0 since FanIn only returns one channel
			}
		}
	}
	// Select from all the pipes
	wg.Add(len(p.inStreams))
	for _, c := range p.inStreams {
		go multiplex(c)
	}
	// Wait for all the reads to complete
	go func() {
		wg.Wait()
		outChannels.Close()
	}()
	return nextPipe
}

// OrDone checks to ensure that an external input stream is still running
func (p DataStream[T]) OrDone(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(standard, param)
	for i := 0; i < len(p.inStreams); i++ {
		go func(inStream <-chan T, outStream chan<- T) {
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

// Broadcast sends a copy of each value to a configurable number of new output pipes
func (p DataStream[T]) Broadcast(
	params ...Params,
) DataStream[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.nextT(broadcast, param)
	go func(inStream <-chan T, outStreams senders[T]) {
		defer outChannels.Close()
		for val := range inStream {
			for i := 0; i < len(outStreams); i++ {
				in := val // TODO: Consider deepCopy use case
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

// Tee splits values coming in from a channel so that you can send them off into two separate DataStream outputs
func (p DataStream[T]) Tee(
	params ...Params,
) (DataStream[T], DataStream[T]) {
	{
		param := applyParams(params...)
		nextPipe1, outChannels1 := p.nextT(standard, param)
		nextPipe2, outChannels2 := p.nextT(standard, param)
		for i := 0; i < len(p.inStreams); i++ {
			go func(in <-chan T, o1 chan<- T, o2 chan<- T) {
				defer close(o1)
				defer close(o2)
				for val := range orDone(p.ctx, in) {
					var o1, o2 = o1, o2
					for i := 0; i < 2; i++ {
						select {
						case <-p.ctx.Done():
						case o1 <- val:
							o1 = nil
						case o2 <- val:
							o2 = nil
						}
					}
				}
			}(p.inStreams[i], outChannels1[i], outChannels2[i])
		}
		return nextPipe1, nextPipe2
	}
}

func Map[T any, U any](
	ds DataStream[T],
	transformFunc TransformFunc[T, U],
	params ...Params,
) DataStream[U] {
	param := applyParams(params...)
	nextPipe, outChannels := next[U](standard, param, len(ds.inStreams), ds.ctx, ds.errStream)
	for i := 0; i < len(ds.inStreams); i++ {
		go func(inStream <-chan T, outStream chan<- U, transformer TransformFunc[T, U]) {
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

func next[T any](
	pipeType pipeType,
	params Params,
	chanCount int,
	ctx context.Context,
	errStream chan<- error,
) (DataStream[T], pipes[T]) {
	switch pipeType {
	case fanOut, broadcast:
		chanCount = params.Num
	case fanIn:
		chanCount = 1
	default:
	}
	streams := make(pipes[T], chanCount)
	streams.Initialize(params.BufferSize)
	return DataStream[T]{
			ctx:       ctx,
			errStream: errStream,
			inStreams: streams.Receivers(),
		},
		streams
}

func (p DataStream[T]) nextT(pipeType pipeType, params Params) (DataStream[T], pipes[T]) {
	return next[T](pipeType, params, len(p.inStreams), p.ctx, p.errStream)
}

// orDone checks to ensure that an external input stream is still running
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
