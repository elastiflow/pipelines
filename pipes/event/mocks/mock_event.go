package mocks

import (
	"net"

	"github.com/elastiflow/pipelines"
)

type mockEvent struct {
	id   string
	key  uint64
	addr *net.UDPAddr
}

func NewMockEvent(id string, key uint64, addr net.Addr) pipelines.Event {
	return &mockEvent{
		id:   id,
		key:  key,
		addr: addr.(*net.UDPAddr),
	}
}

// NewMockPacketEvent creates a new mock PacketEvent with the given parameters.
func NewMockPacketEvent(id string, key uint64, addr net.Addr) pipelines.PacketEvent {
	return &mockEvent{
		id:   id,
		key:  key,
		addr: addr.(*net.UDPAddr),
	}
}

func (e *mockEvent) Key() uint64 {
	return e.key
}

func (e *mockEvent) ID() string {
	return e.id
}

func (e *mockEvent) ToPacket() error {
	return nil
}

func (e *mockEvent) Addr() net.Addr {
	return e.addr
}
