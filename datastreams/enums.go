package datastreams

type pipeType int

const (
	standard pipeType = iota
	fanOut
	fanIn
	broadcast
)
