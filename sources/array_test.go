package sources

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFromArray(t *testing.T) {
	t.Run("given valid values, should return a new values", func(t *testing.T) {
		arr := []string{"a", "b", "c"}

		newSlice := FromArray(arr)

		expected := &Array[string]{
			values: arr,
			out:    make(chan string, len(arr)),
		}
		assert.ElementsMatch(t, expected.values, newSlice.values)
		assert.NotNil(t, newSlice.out)
	})
}

func TestConsume(t *testing.T) {
	testcases := []struct {
		name         string
		values       []string
		setupContext func() context.Context
		expected     []string
	}{
		{
			name:   "given valid values, should send values to output channel",
			values: []string{"a", "b", "c"},
			setupContext: func() context.Context {
				return context.Background()
			},
			expected: []string{"a", "b", "c"},
		},
		{
			name:   "given context is done, should return",
			values: []string{"a", "b", "c"},
			setupContext: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expected: []string{},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			newSlice := FromArray(tt.values)

			ctx := tt.setupContext()

			newSlice.Consume(ctx, nil)

			var consumed []string
			for val := range newSlice.Out() {
				consumed = append(consumed, val)
			}

			assert.ElementsMatch(t, tt.expected, consumed)
		})
	}
}
