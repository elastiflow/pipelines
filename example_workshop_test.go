package pipelines_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"net/http"
)

type Record struct {
	ID    int
	Name  string
	Value float64
	City  string
}

func PostProcessorFunc(rec raw) ([]map[string]string, error) {
	// Process the record and return the modified record
	// For example, let's say we want to change the name of the record
	return string(rec), nil
}

func ProcessFunc(rec string) (string, error) {
	return fmt.Sprintf("{\"value\":%d}", len(rec)), nil
}

type WorkshopSourcer struct {
	records []raw
}

// NewWorkshopSourcer creates a new WorkshopSourcer with the given records
func NewWorkshopSourcer(records []raw) *WorkshopSourcer {
	return &WorkshopSourcer{
		records: records,
	}
}

func (ws *WorkshopSourcer) Source(ctx context.Context, errSender chan<- error) datastreams.DataStream[raw] {
	// Create a channel to send records
	outChan := make(chan raw)
	// Start a goroutine to send records
	go func(outChan chan<- raw) {
		defer close(outChan)
		for _, record := range ws.records {
			select {
			case outChan <- record:
			case <-ctx.Done():
				return
			}
		}
	}(outChan)

	return datastreams.New(ctx, outChan, errSender)
}

type Poster interface {
	Do(rec []byte) (string, error)
}

type HttpSinker struct {
	// Add any necessary fields for the HTTP sinker
	poster *http.Client
}

// NewHttpSinker creates a new HttpSinker with the given Poster
func NewHttpSinker(p *http.Client) *HttpSinker {
	return &HttpSinker{
		poster: p,
	}
}

func (h *HttpSinker) Sink(ctx context.Context, in datastreams.DataStream[string]) error {
	for i := range in.Out() {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := http.NewRequest("GET", "", bytes.NewReader([]byte(i)))
			if err != nil {
				println(err)
				continue
			}
			// Send the record to the HTTP sink
			fmt.Println("Send to HTTP sink:", i)
		}
	}
	return nil
}

type raw struct {
}

func Example_workshop() {

	// Create and initialize the array of 10 elements

	records := []raw{}

	err := pipelines.New[raw, map[string]string](
		context.Background(),
		NewWorkshopSourcer(records),
		make(chan error),
	).Start(func(p datastreams.DataStream[raw]) datastreams.DataStream[map[string]string] {
		return datastreams.Map[raw, map[string]string](
			p,
			PostProcessorFunc,
			datastreams.Params{
				BufferSize: 10,
			},
		).Run(ProcessFunc)
	}).Sink(NewHttpSinker(&http.Client{}))

	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Output:
	// Send to HTTP sink: {"value":5}
	// Send to HTTP sink: {"value":5}
	// Send to HTTP sink: {"value":3}
	// Send to HTTP sink: {"value":3}

}
