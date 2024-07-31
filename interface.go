package pipelines

import "net"

type identifier interface {
	Key() uint64
	ID() string
}

type Event interface {
	identifier
	Unmarshal(data []byte, dataType string) error
	Marshal(dataType string) ([]byte, error)
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
