package pipelines_test

import (
	"context"
	"log/slog"
	"time"

	"github.com/elastiflow/pipelines"
	"github.com/elastiflow/pipelines/datastreams"
	"github.com/elastiflow/pipelines/datastreams/sources"
	"github.com/elastiflow/pipelines/datastreams/windower"
)

func SlingWindowFunc(readings []*SensorReading) (*SensorInference, error) {
	if len(readings) == 0 {
		return nil, nil
	}
	var totalTemp, totalHumidity float64
	for _, reading := range readings {
		totalTemp += reading.Temp
		totalHumidity += reading.Humidity
	}
	avgTemp := totalTemp / float64(len(readings))
	avgHumidity := totalHumidity / float64(len(readings))
	return &SensorInference{
		DeviceID:    readings[0].DeviceID,
		AvgTemp:     avgTemp,
		AvgHumidity: avgHumidity,
	}, nil
}

func Example_sliding() {
	errChan := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	partitionFactory, err := windower.NewSlidingFactory[*SensorReading](150*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		slog.Error("failed to create partition factory: " + err.Error())
		return
	}
	var sensorReadings = []*SensorReading{
		{DeviceID: "device-1", Temp: 22.5, Humidity: 45.0, Timestamp: time.Now().Add(-6 * time.Second)},
		{DeviceID: "device-1", Temp: 22.7, Humidity: 46.0, Timestamp: time.Now().Add(-5 * time.Second)},
		{DeviceID: "device-1", Temp: 22.6, Humidity: 45.5, Timestamp: time.Now().Add(-4 * time.Second)},
		{DeviceID: "device-2", Temp: 19.2, Humidity: 55.0, Timestamp: time.Now().Add(-3 * time.Second)},
		{DeviceID: "device-2", Temp: 19.5, Humidity: 54.8, Timestamp: time.Now().Add(-2 * time.Second)},
		{DeviceID: "device-1", Temp: 22.9, Humidity: 44.9, Timestamp: time.Now().Add(-1 * time.Second)},
		{DeviceID: "device-2", Temp: 19.7, Humidity: 54.5, Timestamp: time.Now()},
	}

	// Create a source with 10 integers
	pl := pipelines.New[*SensorReading, *SensorInference](
		ctx,
		sources.FromArray[*SensorReading](sensorReadings, sources.Params{Throttle: 50 * time.Millisecond}),
		errChan,
	).Start(func(p datastreams.DataStream[*SensorReading]) datastreams.DataStream[*SensorInference] {
		keyFunc := func(i *SensorReading) string {
			return i.DeviceID // Key by device ID
		}
		return datastreams.Window[*SensorReading, string, *SensorInference](
			datastreams.KeyBy[*SensorReading, string](p, keyFunc),
			SlingWindowFunc,
			partitionFactory,
			datastreams.Params{
				BufferSize: 50,
			},
		).OrDone()
	})

	// Handle errors
	go func() {
		defer pl.Close()
		for err := range pl.Errors() {
			select {
			case <-ctx.Done():
				return
			default:
				if err == nil {
					continue
				}
				slog.Error("pipeline error: " + err.Error())
			}
		}
	}()

	// Read from pipeline output
	for v := range pl.Out() {
		select {
		case <-ctx.Done():
			return
		default:
			slog.Info("sliding window output", slog.String("device", v.DeviceID), slog.Float64("avg_temp", v.AvgTemp), slog.Float64("avg_humidity", v.AvgHumidity))
		}
	}

	// Output (example):
	// sliding window output device=device-1 avg_temp=22.5 avg_humidity=45
	// sliding window output device=device-1 avg_temp=22.6 avg_humidity=45.5
	// sliding window output device=device-1 avg_temp=22.65 avg_humidity=45.75
	// sliding window output device=device-1 avg_temp=22.65 avg_humidity=45.75
	// sliding window output device=device-2 avg_temp=19.2 avg_humidity=55
	// sliding window output device=device-1 avg_temp=22.6 avg_humidity=45.5
	// sliding window output device=device-2 avg_temp=19.35 avg_humidity=54.9
	// sliding window output device=device-1 avg_temp=22.9 avg_humidity=44.9
	// sliding window output device=device-2 avg_temp=19.5 avg_humidity=54.8
	// sliding window output device=device-1 avg_temp=22.9 avg_humidity=44.9
	// sliding window output device=device-2 avg_temp=19.6 avg_humidity=54.65
	// sliding window output device=device-1 avg_temp=22.9 avg_humidity=44.9
	// sliding window output device=device-2 avg_temp=19.7 avg_humidity=54.5
	// sliding window output device=device-2 avg_temp=19.7 avg_humidity=54.5
}
