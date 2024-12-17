package pipe

// Params are used to pass args into DataStream methods.
type Params struct {
	Num         int
	BufferSize  int
	SkipError   bool
	SegmentName string
}
