package datastreams

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type testStruct struct {
	ID   int
	Name string
}

func TestKeyedDataStream_KeyBy(t *testing.T) {
	tests := []struct {
		name             string
		keyBy            KeyFunc[testStruct, int]
		process          ProcessFunc[testStruct]
		expectedElements map[int][]int
		elements         []testStruct
	}{
		{
			name: "should key by even/odd",
			keyBy: func(t testStruct) int {
				return t.ID % 2
			},
			process: func(t testStruct) (testStruct, error) {
				return testStruct{ID: t.ID * 2, Name: t.Name}, nil
			},
			expectedElements: map[int][]int{
				0: {0, 2, 4},
				1: {1, 3},
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

			evenStream := kds.Stream(0)
			oddStream := kds.Stream(1)

			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func(ds *DataStream[testStruct]) {
				defer wg.Done()
				eventOut := make([]int, 0)
				for res := range ds.OrDone().Out() {
					eventOut = append(eventOut, res.ID)
				}
				assert.ElementsMatch(t, eventOut, []int{0, 2, 4})
			}(&evenStream)

			go func(ds *DataStream[testStruct]) {
				defer wg.Done()
				oddOut := make([]int, 0)
				for res := range ds.OrDone().Out() {
					oddOut = append(oddOut, res.ID)
				}
				assert.ElementsMatch(t, oddOut, []int{1, 3})
			}(&oddStream)
			wg.Wait()

		})
	}
}
