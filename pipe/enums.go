package pipe

type pipeType int

const (
	standard pipeType = iota
	fanOut
	fanIn
	broadcast
)

type segment int

const (
	segmentFilter segment = iota
	segmentProcess
	segmentMap
)

func (s segment) String() string {
	switch s {
	case segmentFilter:
		return "filter"
	case segmentProcess:
		return "process"
	case segmentMap:
		return "map"
	}
	return ""
}
