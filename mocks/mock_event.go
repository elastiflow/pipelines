package mocks

import (
	"encoding/json"
	"net"
	"strings"

	"github.com/elastiflow/pipelines"
	"gopkg.in/yaml.v3"
)

type mockEvent struct {
	Id       string       `yaml:"id,omitempty" json:"id,omitempty"`
	EventKey uint64       `yaml:"key,omitempty" json:"key,omitempty"`
	UDPAddr  *net.UDPAddr `yaml:"addr,omitempty" json:"addr,omitempty"`
}

func NewMockEvent(id string, key uint64, addr net.Addr) pipelines.Event {
	return &mockEvent{
		Id:       id,
		EventKey: key,
		UDPAddr:  addr.(*net.UDPAddr),
	}
}

// NewMockPacketEvent creates a new mock PacketEvent with the given parameters.
func NewMockPacketEvent(id string, key uint64, addr net.Addr) pipelines.PacketEvent {
	return &mockEvent{
		Id:       id,
		EventKey: key,
		UDPAddr:  addr.(*net.UDPAddr),
	}
}

// Unmarshal data into the struct
func (e *mockEvent) Unmarshal(data []byte, dataType string) error {
	if strings.ToLower(dataType) == "yml" || strings.ToLower(dataType) == "yaml" {
		return yaml.Unmarshal(data, e)
	}
	return json.Unmarshal(data, e)
}

// Marshal marshals the struct to YAML
func (e *mockEvent) Marshal(dataType string) ([]byte, error) {
	if strings.ToLower(dataType) == "yml" || strings.ToLower(dataType) == "yaml" {
		return yaml.Marshal(e)
	}
	return json.Marshal(e)
}

func (e *mockEvent) Key() uint64 {
	return e.EventKey
}

func (e *mockEvent) ID() string {
	return e.Id
}

func (e *mockEvent) ToPacket() error {
	return nil
}

func (e *mockEvent) Addr() net.Addr {
	return e.UDPAddr
}
