package windower

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

func makeTestFunc(index int) testStruct {
	return testStruct{ID: index, Name: fmt.Sprintf("name-%d", index)}
}

func windowTestFunc(structs []testStruct) (testStruct, error) {
	if len(structs) == 0 {
		return testStruct{}, nil
	}
	reduced := testStruct{}
	for i, val := range structs {
		reduced.ID += val.ID
		if i == 0 || val.Name > reduced.Name {
			reduced.Name = val.Name
		}
	}
	return reduced, nil
}

func BenchmarkSliding(b *testing.B) {
	b.Run("Throughput", func(b *testing.B) {
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "Throughput_LowCardinality_IntervalPartitioner",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Throughput_HighCardinality_IntervalPartitioner",
				keyCardinality: 1000,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
		}

		// Loop through the test cases and run the benchmark for each
		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("Duration", func(b *testing.B) {
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "Duration_100ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_200ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](200 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_500ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
		}

		// Loop through the test cases and run the benchmark for each
		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("Concurrency", func(b *testing.B) {
		testCases := []concurrentBenchmarkConfig[testStruct, testStruct]{
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=1_Parts=1",
					keyCardinality: 1,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 1,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=1_Parts=4",
					keyCardinality: 4,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 1,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=1_Parts=8",
					keyCardinality: 8,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 1,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=4_Parts=1",
					keyCardinality: 1,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 4,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=4_Parts=4",
					keyCardinality: 4,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 4,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=4_Parts=8",
					keyCardinality: 8,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 4,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=8_Parts=1",
					keyCardinality: 1,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 8,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=8_Parts=4",
					keyCardinality: 4,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 8,
			},
			{
				baseBenchmarkConfig: baseBenchmarkConfig[testStruct, testStruct]{
					name:           "Producers=8_Parts=8",
					keyCardinality: 4,
					bufferSize:     50,
					partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
					makeFunc:       makeTestFunc,
					windowFunc:     windowTestFunc,
				},
				numProducers: 8,
			},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runConcurrencyBenchmark(b, tc)
			})
		}
	})
	b.Run("WindowConcurrency", func(b *testing.B) {
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "Duration_100ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_200ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](200 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_500ms",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
		}

		// Loop through the test cases and run the benchmark for each
		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("AggregatorComplexity", func(b *testing.B) {
		aggregators := map[string]datastreams.WindowFunc[testStruct, testStruct]{
			"NoOp": func(batch []testStruct) (testStruct, error) {
				return testStruct{}, nil
			},
			"SumBatch": func(batch []testStruct) (testStruct, error) {
				sum := 0
				for _, t := range batch {
					sum += t.ID
				}
				return testStruct{ID: sum}, nil
			},
			"SortBatch": func(batch []testStruct) (testStruct, error) {
				ids := make([]int, len(batch))
				for i, t := range batch {
					ids[i] = t.ID
				}
				sort.Ints(ids)
				return testStruct{ID: ids[len(ids)/2]}, nil
			},
		}

		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "Aggregator_NoOp",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     aggregators["NoOp"],
				makeFunc:       makeTestFunc,
			},
			{
				name:           "Aggregator_SumBatch",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     aggregators["SumBatch"],
				makeFunc:       makeTestFunc,
			},
			{
				name:           "Aggregator_SortBatch",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     aggregators["SortBatch"],
				makeFunc:       makeTestFunc,
			},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("BufferSize", func(b *testing.B) {
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "BufferSize_1",
				keyCardinality: 2,
				bufferSize:     1,
				partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "BufferSize_10",
				keyCardinality: 2,
				bufferSize:     10,
				partitioner:    NewInterval[testStruct, int](200 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_100",
				keyCardinality: 2,
				bufferSize:     100,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Duration_1000",
				keyCardinality: 2,
				bufferSize:     1000,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
		}

		// Loop through the test cases and run the benchmark for each
		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("Cardinality", func(b *testing.B) {
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "Cardinality_1",
				keyCardinality: 1,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](100 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Cardinality_2",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](200 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Cardinality_10",
				keyCardinality: 10,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Cardinality_100",
				keyCardinality: 100,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
			{
				name:           "Cardinality_1000",
				keyCardinality: 1000,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](500 * time.Millisecond),
				makeFunc:       makeTestFunc,
				windowFunc:     windowTestFunc,
			},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
	b.Run("ErrorPath", func(b *testing.B) {
		agg := func(every int) func(batch []testStruct) (testStruct, error) {
			return func(batch []testStruct) (testStruct, error) {
				if len(batch) > 0 && batch[0].ID%every == 0 {
					return testStruct{}, fmt.Errorf("error at %d", batch[0].ID)
				}
				return testStruct{ID: batch[0].ID}, nil
			}
		}
		testCases := []baseBenchmarkConfig[testStruct, testStruct]{
			{
				name:           "ErrorPath_1_in_10",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     agg(10),
				drainErrors:    true,
				makeFunc:       makeTestFunc,
			},
			{
				name:           "ErrorPath_1_in_100",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     agg(100),
				drainErrors:    true,
				makeFunc:       makeTestFunc,
			},
			{
				name:           "ErrorPath_1_in_1000",
				keyCardinality: 2,
				bufferSize:     50,
				partitioner:    NewInterval[testStruct, int](50 * time.Millisecond),
				windowFunc:     agg(1000),
				drainErrors:    true,
				makeFunc:       makeTestFunc,
			},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				runBaseBenchmark(b, tc)
			})
		}
	})
}
