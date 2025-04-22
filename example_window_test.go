package pipelines_test

import (
	"context"
	"log/slog"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
	"github.com/elastiflow/pipelines/datastreams/windower"
)

func Example_window() {
	errChan := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Create a source with 10 integers
	pl := pipelines.New[int, int](
		ctx,
		sources.FromArray([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		errChan,
	).Start(func(p datastreams.DataStream[int]) datastreams.DataStream[int] {
		partitionFactory := windower.NewIntervalFactory(func(t []int) (int, error) {
			if len(t) == 0 {
				return 0, nil
			}
			res := 0
			for _, elem := range t {
				res += elem
			}
			return res, nil
		}, 500*time.Millisecond) // process over 500ms

		keyedOut := datastreams.Window[int, int, int](
			datastreams.KeyBy[int, int](
				p,
				func(i int) int {
					return i % 3 // Key by modulo 3
				},
			),
			partitionFactory,
			func(t int) int {
				return t % 2
			},
			datastreams.Params{
				BufferSize: 50,
			},
		)
		return keyedOut.OrDone()
	})

	// Handle errors
	go func() {
		defer pl.Close()
		for err := range pl.Errors() {
			select {
			case <-ctx.Done():
				return
			default:
				if err == nil {
					continue
				}
				slog.Error("pipeline error: " + err.Error())
			}
		}
	}()

	// Read from pipeline output
	for v := range pl.Out() {
		select {
		case <-ctx.Done():
			return
		default:
			slog.Info("tumbling window output", slog.Int("value", v))
		}
	}

	// Output (example):
	// {"value":18}
	// {"value":15}
	// {"value":22}
}
