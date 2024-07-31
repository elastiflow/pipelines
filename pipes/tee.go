package pipes

import (
	"context"

	"github.com/elastiflow/pipelines"
)

// Tee splits values coming in from a channel so that you can send them off into two separate
func Tee(
	ctx context.Context,
	in <-chan pipelines.Event,
) (_, _ <-chan pipelines.Event) {
	{
		out1 := make(chan pipelines.Event)
		out2 := make(chan pipelines.Event)
		go func(o1 chan<- pipelines.Event, o2 chan<- pipelines.Event) {
			defer close(o1)
			defer close(o2)
			for val := range OrDone(ctx, in) {
				var o1, o2 = o1, o2
				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
					case o1 <- val:
						o1 = nil
					case o2 <- val:
						o2 = nil
					}
				}
			}
		}(out1, out2)
		return out1, out2
	}
}
