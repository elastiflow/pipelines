package sources

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromArray(t *testing.T) {
	t.Run("given valid values, should return a new values", func(t *testing.T) {
		arr := []string{"a", "b", "c"}
		var errSender chan error

		newSlice := FromArray(arr)

		expected := &array[string]{
			values: arr,
			out:    make(chan string, len(arr)),
		}
		var outVals []string
		ds := newSlice.Source(context.Background(), errSender)
		require.NotNil(t, ds)
		for val := range ds.Out() {
			outVals = append(outVals, val)
			if len(outVals) == len(arr) {
				break
			}
		}
		assert.ElementsMatch(t, expected.values, outVals)
	})
}

func TestArraySource_Source(t *testing.T) {
	testcases := []struct {
		name         string
		values       []string
		setupContext func() (context.Context, context.CancelFunc)
		expected     []string
		opts         Params
	}{
		{
			name:   "given valid values, should send values to output channel",
			values: []string{"a", "b", "c"},
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			expected: []string{"a", "b", "c"},
		},
		{
			name:   "given context is done, should return",
			values: []string{"a", "b", "c"},
			setupContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			expected: []string{},
		},
		{
			name:   "should respect throttle",
			values: []string{"a", "b", "c"},
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 10*time.Millisecond)
			},
			expected: []string{"a"},
			opts: Params{
				Throttle: 50 * time.Millisecond,
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			newSlice := FromArray(tt.values, tt.opts)
			var errSender chan error

			ctx, cancel := tt.setupContext()
			defer cancel()
			source := newSlice.Source(ctx, errSender)
			var consumed []string
			for val := range source.Out() {
				consumed = append(consumed, val)
			}

			assert.ElementsMatch(t, tt.expected, consumed)
		})
	}
}
