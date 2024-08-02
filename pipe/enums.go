package pipe

type Type int

const (
	Standard Type = iota
	FanOut
	FanIn
	Broadcast
)
