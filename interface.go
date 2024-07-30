package pipelines

import "net"

type Event interface {
	Key() uint64
	ID() string
}

type PacketEvent interface {
	Event
	ToPacket() error
	Addr() net.Addr
}

type Pipeline interface {
	Open() <-chan Event
	Close() error
}
