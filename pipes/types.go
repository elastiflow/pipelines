package pipes

import (
	"context"

	"github.com/elastiflow/pipelines"
)

type Pipe func(context.Context, <-chan pipelines.Event) <-chan pipelines.Event
