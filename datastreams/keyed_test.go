package datastreams

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/windower"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStruct struct {
	ID   int
	Name string
}

type testDatum struct {
	ID   int
	Data string
}

func TestKeyBy(t *testing.T) {
	tests := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		process  ProcessFunc[testStruct]
		elements []testStruct
	}{
		{
			name: "should key by even/odd",
			keyBy: func(t testStruct) int {
				return t.ID % 2
			},
			process: func(t testStruct) (testStruct, error) {
				return testStruct{ID: t.ID * 2, Name: t.Name}, nil
			},
			elements: []testStruct{
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			// Some source of integers.
			input := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				defer close(input)
				for _, elem := range inputElements {
					select {
					case <-ctx.Done():
						return
					default:
						input <- elem
					}
				}

			}(ctx, tt.elements)

			// Key the DataStream by "even"/"odd".
			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, input, errCh).WithWaitGroup(&sync.WaitGroup{}),
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1, // only 1 output channel per key
				},
			)

			out := make([]int, 0)
			for res := range kds.OrDone().Out() {
				out = append(out, res.Value().ID)
			}
			assert.Len(t, out, 5)
		})
	}
}

func TestWindow(t *testing.T) {
	testCases := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		keyResBy KeyFunc[testStruct, int]
		elements []testStruct
		process  func(t []testStruct) (testStruct, error)
		expected []testStruct
	}{
		{
			name: "should key given key by even/odd and process",
			keyBy: func(t testStruct) int {
				return t.ID % 2
			},
			process: func(t []testStruct) (testStruct, error) {
				newStr := testStruct{}
				for _, elem := range t {
					newStr.ID += elem.ID
					newStr.Name += elem.Name
				}
				return newStr, nil
			},
			elements: []testStruct{
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			},
			expected: []testStruct{
				{ID: 6, Name: "024"},
				{ID: 4, Name: "13"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			input := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				for _, elem := range inputElements {
					input <- elem
				}

			}(ctx, tt.elements)

			// Key the DataStream by "even"/"odd".
			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, input, errCh).WithWaitGroup(&sync.WaitGroup{}),
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1, // only 1 output channel per key
				},
			)

			partitioner, err := windower.NewIntervalFactory[testStruct](500 * time.Millisecond)
			require.NoError(t, err)

			out := Window[testStruct, int, testStruct](
				kds,
				tt.process,
				partitioner, // process over 500ms
				Params{
					BufferSize: 50,
				},
			)

			endRes := make([]testStruct, 0)
			for res := range out.OrDone().Out() {
				endRes = append(endRes, res)
			}

			assert.ElementsMatch(t, tt.expected, endRes)
		})
	}
}

type UserAge struct {
	Num  int
	User int
}

type UserName struct {
	Name string
	User int
}

type User struct {
	Name string
	Age  int
	User int
}

func TestJoin(t *testing.T) {
	testCases := []struct {
		name          string
		left          []*UserName
		right         []*UserAge
		process       func(t []KeyedUnion[*UserAge, *UserName, int]) (*User, error)
		expected      []*User
		throttleLeft  time.Duration
		throttleRight time.Duration
	}{
		{
			name: "Should join streams with the same user id in the same window",
			left: []*UserName{
				{
					Name: "some-name",
					User: 1,
				},
				{
					Name: "some-other",
					User: 2,
				},
			},
			right: []*UserAge{
				{
					Num:  9,
					User: 1,
				},
				{
					Num:  13,
					User: 2,
				},
			},
			expected: []*User{
				{
					Age:  9,
					User: 1,
					Name: "some-name",
				},
				{
					Age:  13,
					User: 2,
					Name: "some-other",
				},
			},
			process: func(t []KeyedUnion[*UserAge, *UserName, int]) (*User, error) {
				u := &User{}
				for _, rec := range t {
					if left := rec.Left(); left != nil {
						u.Age = left.Value().Num
						u.User = left.Key()
					}

					if right := rec.Right(); right != nil {
						u.Name = right.Value().Name
						u.User = right.Key()
					}
				}
				return u, nil
			},
		},
		{
			name: "Should not join streams with different user ids in the same window",
			left: []*UserName{
				{
					Name: "some-name",
					User: 1,
				},
				{
					Name: "some-other",
					User: 2,
				},
			},
			right: []*UserAge{
				{
					Num:  9,
					User: 3,
				},
				{
					Num:  13,
					User: 4,
				},
			},
			expected: []*User{
				{
					User: 1,
					Name: "some-name",
				},
				{
					User: 2,
					Name: "some-other",
				},
				{
					Age:  13,
					User: 4,
				},
				{
					Age:  9,
					User: 3,
				},
			},
			process: func(t []KeyedUnion[*UserAge, *UserName, int]) (*User, error) {
				u := &User{}
				for _, rec := range t {
					if left := rec.Left(); left != nil {
						u.Age = left.Value().Num
						u.User = left.Key()
					}

					if right := rec.Right(); right != nil {
						u.Name = right.Value().Name
						u.User = right.Key()
					}
				}
				return u, nil
			},
		},
		{
			name: "Should not join elements in different windows",
			left: []*UserName{
				{
					Name: "some-name",
					User: 1,
				},
				{
					Name: "some-other",
					User: 2,
				},
			},
			right: []*UserAge{
				{
					Num:  9,
					User: 1,
				},
				{
					Num:  13,
					User: 2,
				},
			},
			expected: []*User{
				{
					User: 1,
					Name: "some-name",
					Age:  9,
				},
				{
					Name: "some-other",
					User: 2,
				},
				{
					Age:  13,
					User: 2,
				},
			},
			throttleRight: 600 * time.Millisecond,
			process: func(t []KeyedUnion[*UserAge, *UserName, int]) (*User, error) {
				u := &User{}
				for _, rec := range t {
					if left := rec.Left(); left != nil {
						u.Age = left.Value().Num
						u.User = left.Key()
					}

					if right := rec.Right(); right != nil {
						u.Name = right.Value().Name
						u.User = right.Key()
					}
				}
				return u, nil
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			leftStream := make(chan *UserName, 10)
			rightStream := make(chan *UserAge, 10)
			go func(appCtx context.Context, left []*UserName) {
				for _, elem := range left {
					leftStream <- elem
					time.Sleep(tt.throttleLeft)
				}
			}(ctx, tt.left)

			go func(appCtx context.Context, right []*UserAge) {
				for _, elem := range right {
					rightStream <- elem
					time.Sleep(tt.throttleRight)
				}
			}(ctx, tt.right)

			p, err := windower.NewIntervalFactory[KeyedUnion[*UserAge, *UserName, int]](500 * time.Millisecond)
			require.NoError(t, err)
			left := KeyBy[*UserName, int](
				New[*UserName](ctx, leftStream, errCh),
				func(name *UserName) int {
					return name.User
				},
				Params{
					BufferSize: 50,
					Num:        1,
				},
			)

			right := KeyBy[*UserAge, int](
				New[*UserAge](ctx, rightStream, errCh),
				func(name *UserAge) int {
					return name.User
				},
				Params{
					BufferSize: 50,
					Num:        1,
				},
			)

			out := Join[*UserAge, *UserName, int, *User](
				right,
				left,
				tt.process,
				p,
				Params{
					BufferSize: 50,
				},
			)

			endRes := make([]*User, 0)
			for res := range out.OrDone().Out() {
				endRes = append(endRes, res)
			}

			assert.ElementsMatch(t, tt.expected, endRes)
		})
	}
}

func BenchmarkWindowThroughput(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Count=%d", count), func(b *testing.B) {
			// 1) setup
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			in := make(chan testStruct, b.N)
			errCh := make(chan error, 1)

			// key by ID%2 just as an example
			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				Params{BufferSize: 50, Num: 1},
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

			partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
			require.NoError(b, err)
			win := Window[testStruct, int, testStruct](
				kds,
				windowFunc,
				partitioner,
				Params{BufferSize: 50},
			)

			go drain(win.OrDone().Out())

			// 2) measure
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				in <- testStruct{ID: i, Name: "test"}
			}
			b.StopTimer()

			// 3) teardown
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

			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				Params{BufferSize: 50, Num: 1},
			)

			widowFunc := func(batch []testStruct) (testStruct, error) {
				return testStruct{}, nil
			}
			window, err := windower.NewIntervalFactory[testStruct](dur)
			require.NoError(b, err)

			win := Window[testStruct, int, testStruct](
				kds,
				widowFunc,
				window,
				Params{BufferSize: 50},
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

			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				Params{BufferSize: buf, Num: 1},
			)

			widowFunc := func(batch []testStruct) (testStruct, error) {
				return testStruct{}, nil
			}
			partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
			require.NoError(b, err)
			win := Window[testStruct, int, testStruct](
				kds,
				widowFunc,
				partitioner,
				Params{BufferSize: buf},
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

			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % K },
				Params{BufferSize: 50, Num: 1},
			)
			partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
			require.NoError(b, err)

			winFunc := func(batch []testStruct) (testStruct, error) { return testStruct{}, nil }
			win := Window[testStruct, int, testStruct](
				kds,
				winFunc,
				partitioner,
				Params{BufferSize: 50},
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

			kds := KeyBy[testStruct, int](
				New[testStruct](ctx, in, errCh),
				func(t testStruct) int { return t.ID % 2 },
				Params{BufferSize: 50, Num: 1},
			)

			partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
			require.NoError(b, err)
			win := Window[testStruct, int, testStruct](
				kds,
				agg,
				partitioner,
				Params{BufferSize: 50},
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

				kds := KeyBy[testStruct, int](
					New[testStruct](ctx, in, errCh),
					func(t testStruct) int { return t.ID % M },
					Params{BufferSize: 50, Num: M},
				)
				proc := func(batch []testStruct) (testStruct, error) { return testStruct{}, nil }
				partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
				require.NoError(b, err)
				win := Window[testStruct, int, testStruct](
					kds,
					proc,
					partitioner,
					Params{BufferSize: 50},
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

	partitioner, err := windower.NewIntervalFactory[testStruct](50 * time.Millisecond)
	require.NoError(b, err)

	// Setup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in := make(chan testStruct, b.N)
	errCh := make(chan error, b.N)

	kds := KeyBy[testStruct, int](
		New[testStruct](ctx, in, errCh),
		func(t testStruct) int { return t.ID % 2 },
		Params{BufferSize: 50, Num: 1},
	)

	win := Window[testStruct, int, testStruct](
		kds,
		agg,
		partitioner,
		Params{BufferSize: 50},
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

type joinBenchmarkCase struct {
	name           string
	keyCardinality int     // How many unique keys to generate
	joinRatio      float64 // 0.0 (no joins) to 1.0 (all records join)
	windowDur      time.Duration
	bufferSize     int
	concurrency    int
}

func BenchmarkJoinKeyCardinality(b *testing.B) {
	testCases := []joinBenchmarkCase{
		{name: "Keys=2", keyCardinality: 2, joinRatio: 0.5, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Keys=10", keyCardinality: 10, joinRatio: 0.5, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Keys=100", keyCardinality: 100, joinRatio: 0.5, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Keys=1000", keyCardinality: 1000, joinRatio: 0.5, windowDur: 50 * time.Millisecond, bufferSize: 100},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			runJoinBenchmark(b, tc)
		})
	}
}

func BenchmarkJoinRatio(b *testing.B) {
	testCases := []joinBenchmarkCase{
		{name: "Ratio=0.01", keyCardinality: 10, joinRatio: 0.01, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Ratio=0.1", keyCardinality: 10, joinRatio: 0.1, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Ratio=0.5", keyCardinality: 10, joinRatio: 0.5, windowDur: 50 * time.Millisecond, bufferSize: 100},
		{name: "Ratio=1.0", keyCardinality: 10, joinRatio: 1.0, windowDur: 50 * time.Millisecond, bufferSize: 100},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			runJoinBenchmark(b, tc)
		})
	}
}

func BenchmarkJoinThroughput(b *testing.B) {
	testCases := []joinBenchmarkCase{
		{name: "LowCardinality_LowRatio", keyCardinality: 2, joinRatio: 0.1, windowDur: 100 * time.Millisecond, bufferSize: 100},
		{name: "HighCardinality_LowRatio", keyCardinality: 1000, joinRatio: 0.1, windowDur: 100 * time.Millisecond, bufferSize: 100},
		{name: "LowCardinality_HighRatio", keyCardinality: 2, joinRatio: 0.9, windowDur: 100 * time.Millisecond, bufferSize: 100},
		{name: "HighCardinality_HighRatio", keyCardinality: 1000, joinRatio: 0.9, windowDur: 100 * time.Millisecond, bufferSize: 100},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			runJoinBenchmark(b, tc)
		})
	}
}

func runJoinBenchmark(b *testing.B, tc joinBenchmarkCase) {
	// 1) Setup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leftIn := make(chan testStruct, tc.bufferSize)
	rightIn := make(chan testDatum, tc.bufferSize)
	errCh := make(chan error, 1)

	// Create the two initial DataStreams
	leftStream := New[testStruct](ctx, leftIn, errCh)
	rightStream := New[testDatum](ctx, rightIn, errCh)

	// Key both streams by ID % keyCardinality
	leftKeyed := KeyBy[testStruct, int](
		leftStream,
		func(t testStruct) int { return t.ID % tc.keyCardinality },
	)
	rightKeyed := KeyBy[testDatum, int](
		rightStream,
		func(t testDatum) int { return t.ID % tc.keyCardinality },
	)

	// Simple WindowFunc that counts matches
	wf := func(batch []KeyedUnion[testStruct, testDatum, int]) (int, error) {
		matched := 0
		elems := make(map[int]bool)
		for _, item := range batch {
			var key int
			if left := item.Left(); left != nil {
				key = left.Key()
			}
			if right := item.Right(); right != nil {
				key = right.Key()
			}
			if _, exists := elems[key]; exists {
				matched++
			} else {
				elems[key] = true
			}
		}
		return matched, nil
	}

	p, err := windower.NewIntervalFactory[KeyedUnion[testStruct, testDatum, int]](tc.windowDur)
	require.NoError(b, err)

	// The Join pipeline itself
	joinedStream := Join[testStruct, testDatum, int, int](
		leftKeyed,
		rightKeyed,
		wf,
		p,
		Params{BufferSize: tc.bufferSize, Num: tc.concurrency},
	)

	go drain(joinedStream.OrDone().Out())

	// 2) Measure
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Push a left item
		leftIn <- testStruct{ID: i, Name: "left"}

		// Push a right item based on the join ratio
		// We give it a slightly different ID to simulate non-identical records
		// that still share the same key.
		if b.N%int(1.0/tc.joinRatio) == 0 {
			rightIn <- testDatum{ID: i, Data: "right"}
		}
	}
	b.StopTimer()

	// 3) Teardown
	close(leftIn)
	close(rightIn)
}

func drain[T any](ch <-chan T) {
	for range ch {
	}
}
