package datastreams

import (
	"sync"
	"time"
)

// Params are used to pass args into DataStream methods.
type Params struct {
	Num               int
	BufferSize        int
	SkipError         bool
	SegmentName       string
	MaxBufferedPerKey int
	TTL               time.Duration
	SlicePool         *sync.Pool
}
