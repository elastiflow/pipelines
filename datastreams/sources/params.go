package sources

import "time"

type Params struct {
	BufferSize int
	// ThrottleInterval is the interval between sending values
	Throttle time.Duration
}
