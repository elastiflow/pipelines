package partition

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams/internal/pipes"
	"github.com/stretchr/testify/assert"
)

func TestBatch_PushAndLen(t *testing.T) {
	b := NewBatch[int]()

	b.Push(1)
	b.Push(2)
	b.Push(3)

	assert.Equal(t, 3, b.Len())
}

func TestBatch_NextResetsBuffer(t *testing.T) {
	b := NewBatch[string]()
	b.Push("a")
	b.Push("b")

	next := b.Next()
	assert.ElementsMatch(t, []string{"a", "b"}, next)
	assert.Equal(t, 0, b.Len(), "Next should clear the batch")
}

func TestBatch_NextOnEmpty(t *testing.T) {
	b := NewBatch[int]()
	result := b.Next()
	assert.Empty(t, result)
	assert.Equal(t, 0, b.Len())
}

func TestBase_PushAddsToBatch(t *testing.T) {
	ctx := context.Background()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[string], 1)
	out.Initialize(1)

	base := NewBase[string, string](ctx, out[0], errs)
	base.Push("foo")

	assert.Equal(t, 1, base.Batch.Len())
}

func TestBase_FlushSuccess(t *testing.T) {
	ctx := context.Background()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[string], 1)
	out.Initialize(1)

	base := NewBase[string, string](ctx, out[0], errs)

	// procFunc returns "ok"
	proc := func(items []string) (string, error) {
		return "aggregated", nil
	}

	go base.Flush(ctx, []string{"a", "b"}, proc, errs)

	select {
	case v := <-out[0]:
		assert.Equal(t, "aggregated", v)
	case <-time.After(100 * time.Millisecond):
		t.Error("Flush did not send output")
	}

	assert.Empty(t, errs)
}

func TestBase_FlushError(t *testing.T) {
	ctx := context.Background()
	errs := make(chan error, 1)
	out := make(pipes.Pipes[string], 1)
	out.Initialize(1)

	base := NewBase[string, string](ctx, out[0], errs)

	proc := func(_ []string) (string, error) {
		return "", errors.New("fail")
	}

	base.Flush(ctx, []string{"x"}, proc, errs)

	select {
	case err := <-errs:
		assert.EqualError(t, err, "fail")
	case <-time.After(100 * time.Millisecond):
		t.Error("Flush did not report error")
	}
}
