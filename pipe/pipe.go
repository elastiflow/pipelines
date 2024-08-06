package pipe

import (
	"context"
	"fmt"
	"math/rand" // nosemgrep
	"sync"
	"time"
)

// Pipe is a struct that defines a generic stream process stage
type Pipe[T any] struct {
	ctx       context.Context
	errStream chan<- error
	inStreams []<-chan T
}

// New constructs a new Pipe of a given type by passing in a pipelines.Pipeline context, registry, and IO streams
func New[T any](
	ctx context.Context,
	inStream <-chan T,
	errStream chan<- error,
) Pipe[T] {
	return Pipe[T]{
		ctx:       ctx,
		errStream: errStream,
		inStreams: []<-chan T{inStream},
	}
}

// Out the data outputted from a Pipe
func (p Pipe[T]) Out() <-chan T {
	if len(p.inStreams) == 1 {
		return p.inStreams[0]
	}
	return p.FanIn().inStreams[0] // If multiple streams, FanIn to a single stream
}

func applyParams(params ...Params) Params {
	var p Params
	for _, param := range params {
		p = param
	}
	return p
}

// Run executes a user defined process function on the input stream(s)
func (p Pipe[T]) Run(
	proc ProcessFunc[T],
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(standard, param)
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
						p.errStream <- fmt.Errorf("piper.Pipe.Run() error: %w", err)
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

// Take a specific number of inputs from the inputStream(s)
func (p Pipe[T]) Take(
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(standard, param)
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

// FanOut kicks off a number of Pipe streams and round robins the input values
func (p Pipe[T]) FanOut(
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(fanOut, param)
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
func (p Pipe[T]) FanIn(
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(fanIn, param)
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
	// Select from all the channels
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
func (p Pipe[T]) OrDone(
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(standard, param)
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

// Broadcast sends a copy of each value to a configurable number of new output channels
func (p Pipe[T]) Broadcast(
	params ...Params,
) Pipe[T] {
	param := applyParams(params...)
	nextPipe, outChannels := p.next(broadcast, param)
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

// Tee splits values coming in from a channel so that you can send them off into two separate Pipe outputs
func (p Pipe[T]) Tee(
	params ...Params,
) (Pipe[T], Pipe[T]) {
	{
		param := applyParams(params...)
		nextPipe1, outChannels1 := p.next(standard, param)
		nextPipe2, outChannels2 := p.next(standard, param)
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

func (p Pipe[T]) next(pipeType pipeType, params Params) (Pipe[T], channels[T]) {
	var chanCount int
	switch pipeType {
	case fanOut, broadcast:
		chanCount = params.Num
	case fanIn:
		chanCount = 1
	default:
		chanCount = len(p.inStreams)
	}
	streams := make(channels[T], chanCount)
	streams.Initialize(params.BufferSize)
	return Pipe[T]{
			ctx:       p.ctx,
			errStream: p.errStream,
			inStreams: streams.Receivers(),
		},
		streams
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
