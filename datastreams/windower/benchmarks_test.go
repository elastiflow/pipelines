package windower

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

type testStruct struct {
	ID   int
	Name string
}

func (t testStruct) GetKey() int {
	return t.ID
}

func drain[T any](outCh <-chan T) {
	for range outCh {
	}
}

type Keyer interface {
	GetKey() int
}

type baseBenchmarkConfig[T Keyer, R any] struct {
	name           string
	keyCardinality int
	bufferSize     int
	partitioner    datastreams.Partitioner[T, int]
	windowFunc     func([]T) (R, error)
	makeFunc       func(index int) T
	drainErrors    bool
}

type concurrentBenchmarkConfig[T Keyer, R any] struct {
	baseBenchmarkConfig[T, R]
	numProducers int
}

func setupWindow[T Keyer, R any](
	cfg *baseBenchmarkConfig[T, R],
	workers int,
) (in chan T, errs chan error, out datastreams.DataStream[R], cancel context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	in = make(chan T)
	if cfg.drainErrors {
		errs = make(chan error)
	} else {
		errs = make(chan error, 1)
	}

	kds := datastreams.KeyBy[T, int](
		datastreams.New[T](ctx, in, errs),
		func(t T) int { return t.GetKey() % cfg.keyCardinality },
		datastreams.Params{BufferSize: cfg.bufferSize, Num: workers},
	)

	out = datastreams.Window[T, int, R](
		kds,
		cfg.windowFunc,
		cfg.partitioner,
		datastreams.Params{BufferSize: cfg.bufferSize},
	)
	return
}

func runBaseBenchmark[T Keyer, R any](b *testing.B, cfg baseBenchmarkConfig[T, R]) {
	in, errs, win, cancel := setupWindow(&cfg, 1)
	defer cancel()
	if cfg.drainErrors {
		go func() {
			for range errs {

			}
		}()
	}

	go drain(win.OrDone().Out())
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in <- cfg.makeFunc(i)
	}

	b.StopTimer()
}

func runConcurrencyBenchmark[T Keyer, R any](b *testing.B, cfg concurrentBenchmarkConfig[T, R]) {
	in, _, win, cancel := setupWindow[T, R](&cfg.baseBenchmarkConfig, cfg.keyCardinality)
	defer cancel()
	go drain(win.OrDone().Out())

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(cfg.numProducers)
	for p := 0; p < cfg.numProducers; p++ {
		go func(producerID int) {
			defer wg.Done()
			for i := producerID; i < b.N; i += cfg.numProducers {
				in <- cfg.makeFunc(i)
			}
		}(p)
	}
	wg.Wait()

	b.StopTimer()
	close(in)
}
