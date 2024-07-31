package pipeline

import (
	"context"
	"time"

	"github.com/elastiflow/pipelines"
	customprocs "github.com/elastiflow/pipelines/examples/custom/procs"
	"github.com/elastiflow/pipelines/pipes"
	"github.com/elastiflow/pipelines/procs"
)

// pipeline is an example pipeline implementation
type pipeline struct {
	startTime  time.Time
	errorChan  chan<- error
	inputChan  <-chan pipelines.Event
	fanNum     uint16
	filter     map[string]bool
	cancelFunc context.CancelFunc
}

func New(inChan <-chan pipelines.Event, errChan chan<- error, f map[string]bool, fanNum uint16) pipelines.Pipeline {
	return &pipeline{
		inputChan: inChan,
		errorChan: errChan,
		fanNum:    fanNum,
		filter:    f,
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
	packets := procs.FanOut(ctx, p.inputChan, p.errorChan, customprocs.ToPacketFilter, pipelines.NewProps(pipelines.Packet), p.fanNum)
	filteredPackets := procs.Filter(ctx, pipes.FanIn(ctx, packets...), p.errorChan, pipelines.NewPacketProps(p.filter))
	return filteredPackets
}
