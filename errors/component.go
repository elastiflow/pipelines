package errors

type Error interface {
	error
	Stage() string
}

type SegmentError struct {
	error
	segment string
}

func NewSegment(name, defaultName string, err error) *SegmentError {
	segment := name
	if segment == "" {
		segment = defaultName
	}
	return &SegmentError{
		error:   err,
		segment: segment,
	}
}

func (e *SegmentError) Stage() string {
	return e.segment
}
