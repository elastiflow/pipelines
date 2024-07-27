package pipelines

import "net"

type Event interface {
	Key() uint64
	ID() string
}

type PacketEvent interface {
	Event
	ToPacket() error
	Addr() *net.UDPAddr
}

type Pipeline[T Event] interface {
	Open() <-chan T
	Close() error
}
