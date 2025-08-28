package windower

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/elastiflow/pipelines/datastreams"
)

// Hammers one Sliding partition with many concurrent Push calls for the same key.
// Run with: go test ./datastreams/windower -race -run TestSliding_NoDataRace_Stress -count=1
func TestSliding_NoDataRace_Stress(t *testing.T) {
	runtime.GOMAXPROCS(2)

	const (
		window = 100 * time.Millisecond
		slide  = 20 * time.Millisecond
	)
	s := NewSliding[int, string](window, slide)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan []int, 1024)
	p := s.Create(ctx, out)

	const (
		producers = 12
		perProd   = 2000
	)
	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perProd; j++ {
				elem := datastreams.NewKeyedElement[int, string]("k", 1)
				p.Push(datastreams.NewTimedKeyedElement[int, string](elem, time.Now()))
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	var flushes, total int
	deadline := time.NewTimer(800 * time.Millisecond)
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for {
			select {
			case batch := <-out:
				if len(batch) > 0 {
					flushes++
					total += len(batch)
				}
			case <-deadline.C:
				return
			}
		}
	}()

	wg.Wait()
	s.Close() // allow final flush
	<-collectDone

	if flushes == 0 {
		t.Fatalf("expected at least one flush; got %d", flushes)
	}
	if total == 0 {
		t.Fatalf("expected at least one item across flushes; got %d", total)
	}
}

// Longer run, ensures multiple flushes occur under sustained load.
func TestSliding_NoDataRace_Stress_Long(t *testing.T) {
	runtime.GOMAXPROCS(2)

	const (
		window = 150 * time.Millisecond
		slide  = 30 * time.Millisecond
	)
	s := NewSliding[int, string](window, slide)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan []int, 2048)
	p := s.Create(ctx, out)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5000; j++ {
				elem := datastreams.NewKeyedElement[int, string]("k", 1)
				p.Push(datastreams.NewTimedKeyedElement[int, string](elem, time.Now()))
			}
		}(i)
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(600 * time.Millisecond)
	flushes := 0
loop:
	for {
		select {
		case <-ticker.C:
		drain:
			for {
				select {
				case b := <-out:
					if len(b) > 0 {
						flushes++
					}
				default:
					break drain
				}
			}
		case <-deadline.C:
			break loop
		}
	}
	wg.Wait()
	s.Close()

	if flushes < 3 {
		t.Fatalf("expected multiple flushes; got %d", flushes)
	}
}
