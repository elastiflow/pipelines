package pipelines

import "context"

type Pipe func(context.Context, <-chan Event) <-chan Event

type Proc func(context.Context, <-chan Event, chan<- error, *Props) <-chan Event

type PacketProc func(context.Context, <-chan PacketEvent, chan<- error) <-chan PacketEvent

type EventType int

const (
	Default EventType = iota
	Packet
)

type Props struct {
	Type    EventType
	AddrMap map[string]bool
}

func NewProps(t EventType) *Props {
	return &Props{
		Type:    t,
		AddrMap: nil,
	}
}

func NewPacketProps(addrMap map[string]bool) *Props {
	return &Props{
		Type:    Packet,
		AddrMap: addrMap,
	}
}
