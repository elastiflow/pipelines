package errors

type Error interface {
	error
	Stage() string
}

type SegmentError struct {
	error
	segment string
}

func NewSegment(segment string, err error) *SegmentError {
	return &SegmentError{
		error:   err,
		segment: segment,
	}
}

func (e *SegmentError) Stage() string {
	return e.segment
}
