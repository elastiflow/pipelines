package pipeline

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/pipes"
	"github.com/elastiflow/pipelines/procs"
)

// pipeline is an example pipeline implementation
type pipeline struct {
	startTime  time.Time
	errorChan  chan<- error
	inputChan  <-chan pipelines.Event
	fanNum     uint16
	cancelFunc context.CancelFunc
}

func New(inputChan <-chan pipelines.Event, errChan chan<- error, fanNum uint16) pipelines.Pipeline {
	return &pipeline{
		inputChan: inputChan,
		errorChan: errChan,
		fanNum:    fanNum,
	}
}

func (p *pipeline) Open() <-chan pipelines.Event {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	p.startTime = time.Now()
	return p.pipe(ctx)
}

func (p *pipeline) Close() error {
	p.cancelFunc()
	return nil
}

func (p *pipeline) pipe(ctx context.Context) <-chan pipelines.Event {
	toSnmpPackets := procs.FanOut(ctx, p.inputChan, p.errorChan, procs.ToSNMPPacket, pipelines.NewProps(pipelines.Packet), p.fanNum)
	filterPacketAddr := procs.Filter(ctx, pipes.FanIn(ctx, toSnmpPackets...), p.errorChan, pipelines.NewPacketProps(map[string]bool{"127.0.0.1:65000": true}))
	return filterPacketAddr
}
