package windower

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

type testStruct struct {
	ID   int
	Name string
}

func drain(outCh <-chan testStruct) {
	for range outCh {
	}
}

func BenchmarkWindowThroughput(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Count=%d", count), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			in := make(chan testStruct, b.N)
			errCh := make(chan error, 1)

			kds := datastreams.KeyBy[testStruct, int](
				datastreams.New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				datastreams.Params{BufferSize: 50, Num: 1},
			)
			windowFunc := func(batch []testStruct) (testStruct, error) {
				avgId := 0
				for _, item := range batch {
					avgId += item.ID
				}
				return testStruct{
					ID:   avgId,
					Name: "test",
				}, nil
			}

			partitioner := NewInterval[testStruct, int](50 * time.Millisecond)

			win := datastreams.Window[testStruct, int, testStruct](
				kds,
				windowFunc,
				partitioner,
				datastreams.Params{BufferSize: 50},
			)

			go drain(win.OrDone().Out())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()
			b.ReportAllocs()

			close(in)
		})
	}
}

func BenchmarkWindowDuration(b *testing.B) {
	durations := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
	}

	for _, dur := range durations {
		b.Run(fmt.Sprintf("Dur=%s", dur), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			in := make(chan testStruct, b.N)
			errCh := make(chan error, 1)

			kds := datastreams.KeyBy[testStruct, int](
				datastreams.New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				datastreams.Params{BufferSize: 50, Num: 1},
			)

			widowFunc := func(batch []testStruct) (testStruct, error) {
				return testStruct{}, nil
			}

			win := datastreams.Window[testStruct, int, testStruct](
				kds,
				widowFunc,
				NewInterval[testStruct, int](dur),
				datastreams.Params{BufferSize: 50},
			)

			go drain(win.OrDone().Out())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()

			close(in)
		})
	}
}

func BenchmarkWindowBufferSize(b *testing.B) {
	for _, buf := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Buf=%d", buf), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			in := make(chan testStruct, buf)
			errCh := make(chan error, 1)

			kds := datastreams.KeyBy[testStruct, int](
				datastreams.New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				datastreams.Params{BufferSize: buf, Num: 1},
			)

			widowFunc := func(batch []testStruct) (testStruct, error) {
				return testStruct{}, nil
			}
			partitioner := NewInterval[testStruct, int](50 * time.Millisecond)

			win := datastreams.Window[testStruct, int, testStruct](
				kds,
				widowFunc,
				partitioner,
				datastreams.Params{BufferSize: buf},
			)

			go drain(win.OrDone().Out())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()

			close(in)
		})
	}
}

func BenchmarkWindowKeyCardinality(b *testing.B) {
	for _, K := range []int{1, 2, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Keys=%d", K), func(b *testing.B) {
			// Setup
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			in := make(chan testStruct, b.N)
			errCh := make(chan error, 1)

			kds := datastreams.KeyBy[testStruct, int](
				datastreams.New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % K },
				datastreams.Params{BufferSize: 50, Num: 1},
			)
			partitioner := NewInterval[testStruct, int](50 * time.Millisecond)

			winFunc := func(batch []testStruct) (testStruct, error) { return testStruct{}, nil }
			win := datastreams.Window[testStruct, int, testStruct](
				kds,
				winFunc,
				partitioner,
				datastreams.Params{BufferSize: 50},
			)
			go drain(win.OrDone().Out())

			// Measure
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()

			close(in)
		})
	}
}

func BenchmarkWindowAggregatorComplexity(b *testing.B) {
	aggregators := map[string]func([]testStruct) (testStruct, error){
		"NoOp": func(batch []testStruct) (testStruct, error) {
			return testStruct{}, nil
		},
		"SumBatch": func(batch []testStruct) (testStruct, error) {
			sum := 0
			for _, t := range batch {
				sum += t.ID
			}
			return testStruct{ID: sum}, nil
		},
		"SortBatch": func(batch []testStruct) (testStruct, error) {
			ids := make([]int, len(batch))
			for i, t := range batch {
				ids[i] = t.ID
			}
			sort.Ints(ids)
			return testStruct{ID: ids[len(ids)/2]}, nil
		},
	}

	for name, agg := range aggregators {
		b.Run(name, func(b *testing.B) {
			// Setup
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			in := make(chan testStruct, b.N)
			errCh := make(chan error, 1)

			kds := datastreams.KeyBy[testStruct, int](
				datastreams.New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				datastreams.Params{BufferSize: 50, Num: 1},
			)

			partitioner := NewInterval[testStruct, int](50 * time.Millisecond)
			win := datastreams.Window[testStruct, int, testStruct](
				kds,
				agg,
				partitioner,
				datastreams.Params{BufferSize: 50},
			)
			go drain(win.OrDone().Out())

			// Measure
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()

			close(in)
		})
	}
}

func BenchmarkWindowConcurrency(b *testing.B) {
	producers := []int{1, 4, 8}
	partitions := []int{1, 4, 8}

	for _, N := range producers {
		for _, M := range partitions {
			name := fmt.Sprintf("Producers=%d_Parts=%d", N, M)
			b.Run(name, func(b *testing.B) {
				// Setup
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				in := make(chan testStruct, b.N)
				errCh := make(chan error, 1)

				kds := datastreams.KeyBy[testStruct, int](
					datastreams.New[testStruct](ctx, in, errCh),
					func(t testStruct) int { return t.ID % M },
					datastreams.Params{BufferSize: 50, Num: M},
				)
				proc := func(batch []testStruct) (testStruct, error) { return testStruct{}, nil }
				partitioner := NewInterval[testStruct, int](50 * time.Millisecond)
				win := datastreams.Window[testStruct, int, testStruct](
					kds,
					proc,
					partitioner,
					datastreams.Params{BufferSize: 50},
				)
				go drain(win.OrDone().Out())

				// Measure: N concurrent producers splitting the b.N pushes
				b.ResetTimer()
				var wg sync.WaitGroup
				wg.Add(N)
				for p := 0; p < N; p++ {
					go func(p int) {
						defer wg.Done()
						for i := p; i < b.N; i += N {
							in <- testStruct{ID: i, Name: "test"}
						}
					}(p)
				}
				wg.Wait()
				b.StopTimer()

				close(in)
			})
		}
	}
}

func BenchmarkWindowErrorPath(b *testing.B) {
	const errEvery = 100

	agg := func(batch []testStruct) (testStruct, error) {
		if len(batch) > 0 && batch[0].ID%errEvery == 0 {
			return testStruct{}, fmt.Errorf("error at %d", batch[0].ID)
		}
		return testStruct{ID: batch[0].ID}, nil
	}

	partitioner := NewInterval[testStruct, int](50 * time.Millisecond)

	// Setup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in := make(chan testStruct, b.N)
	errCh := make(chan error, b.N)

	kds := datastreams.KeyBy[testStruct, int](
		datastreams.New[testStruct](ctx, in, errCh),
		func(t testStruct) int { return t.ID % 2 },
		datastreams.Params{BufferSize: 50, Num: 1},
	)

	win := datastreams.Window[testStruct, int, testStruct](
		kds,
		agg,
		partitioner,
		datastreams.Params{BufferSize: 50},
	)
	go drain(win.OrDone().Out())

	// Drain the error channel so aggregator errors don't block
	go func() {
		for range errCh {
		}
	}()

	// Measure
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in <- testStruct{ID: i, Name: "test"}
	}
	b.StopTimer()

	close(in)
}
