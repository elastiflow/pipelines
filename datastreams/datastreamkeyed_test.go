package datastreams

import (
	"context"
	"fmt"
	"github.com/elastiflow/pipelines/datastreams/joiner"
	"github.com/elastiflow/pipelines/datastreams/windower"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type testStruct struct {
	ID   int
	Name string
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
				out = append(out, res.ID)
			}
			assert.Len(t, out, 5)
		})
	}
}

func TestWindow(t *testing.T) {
	tests := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		keyResBy KeyFunc[testStruct, int]
		elements []testStruct
		process  func(t []testStruct) (testStruct, error)
	}{
		{
			name: "should key by even/odd",
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			input := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				defer close(input)
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

			out := Window[testStruct, int, testStruct](
				kds,
				windower.NewIntervalFactory(tt.process, 500*time.Millisecond), // process over 500ms
				tt.keyResBy,
				Params{
					BufferSize: 50,
				},
			)

			endRes := make([]testStruct, 0)
			for res := range out.OrDone().Out() {
				endRes = append(endRes, res)
			}

			assert.ElementsMatch(t, []testStruct{
				{ID: 6, Name: "024"},
				{ID: 4, Name: "13"},
			}, endRes)
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		name     string
		keyBy    KeyFunc[testStruct, int]
		keyResBy KeyFunc[testStruct, int]
		left     []testStruct
		right    []testStruct
		process  func(t testStruct) (testStruct, error)
	}{
		{
			name: "should key by even/odd",
			keyBy: func(t testStruct) int {
				return t.ID % 2
			},
			process: func(t testStruct) (testStruct, error) {
				return t, nil
			},
			left: []testStruct{
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			},
			right: []testStruct{
				{ID: 5, Name: "5"},
				{ID: 6, Name: "6"},
				{ID: 7, Name: "7"},
				{ID: 8, Name: "8"},
				{ID: 9, Name: "9"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()

			errCh := make(chan error, 10)
			left := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				defer close(left)
				for _, elem := range inputElements {
					left <- elem
				}

			}(ctx, tt.left)

			right := make(chan testStruct, 10)
			go func(appCtx context.Context, inputElements []testStruct) {
				defer close(right)
				for _, elem := range inputElements {
					right <- elem
				}

			}(ctx, tt.right)

			// Key the DataStream by "even"/"odd".
			kdsLeft := KeyBy[testStruct, int](
				New[testStruct](ctx, left, errCh).WithWaitGroup(&sync.WaitGroup{}),
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1, // only 1 output channel per key
				},
			)

			kdsRight := KeyBy[testStruct, int](
				New[testStruct](ctx, right, errCh).WithWaitGroup(&sync.WaitGroup{}),
				tt.keyBy,
				Params{
					BufferSize: 50,
					Num:        1, // only 1 output channel per key
				},
			)

			out := Join[testStruct, int, testStruct](
				tt.keyResBy,
				kdsLeft,
				kdsRight,
				joiner.NewJoinerFactory(tt.process),
				Params{
					BufferSize: 50,
				},
			)

			endRes := make([]testStruct, 0)
			for res := range out.OrDone().Out() {
				endRes = append(endRes, res)
			}

			assert.ElementsMatch(t, []testStruct{
				{ID: 5, Name: "5"},
				{ID: 6, Name: "6"},
				{ID: 7, Name: "7"},
				{ID: 8, Name: "8"},
				{ID: 9, Name: "9"},
				{ID: 0, Name: "0"},
				{ID: 1, Name: "1"},
				{ID: 2, Name: "2"},
				{ID: 3, Name: "3"},
				{ID: 4, Name: "4"},
			}, endRes)
		})
	}
}

func BenchmarkPipelineOpen(b *testing.B) {
	b.Run("Windowing", func(b *testing.B) {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFunc()

		errCh := make(chan error, b.N)
		input := make(chan testStruct, b.N)
		go func(n int, appCtx context.Context) {
			defer close(input)
			for i := 0; i < n; i++ {
				input <- testStruct{ID: i, Name: fmt.Sprintf("%02d", i)}
			}
		}(b.N, ctx)

		// Key the DataStream by "even"/"odd".
		kds := KeyBy[testStruct, int](
			New[testStruct](ctx, input, errCh).WithWaitGroup(&sync.WaitGroup{}),
			func(t testStruct) int {
				return t.ID % 2
			},
			Params{
				BufferSize: 50,
				Num:        1, // only 1 output channel per key
			},
		)

		out := Window[testStruct, int, testStruct](
			kds,
			windower.NewIntervalFactory(
				func(t []testStruct) (testStruct, error) {
					newStr := testStruct{}
					for _, elem := range t {
						newStr.ID += elem.ID
						newStr.Name += elem.Name
					}
					return newStr, nil
				},
				500*time.Millisecond,
			), // process over 500ms
			func(t testStruct) int {
				return t.ID % 3
			},
			Params{
				BufferSize: 50,
			},
		)

		endRes := make([]testStruct, 0)
		for res := range out.OrDone().Out() {
			endRes = append(endRes, res)
		}
	})
}
