package aggregate

import (
	"sync"

	"github.com/ysanson/cordage/internal/ingest"
)

// RunParallel drains batches with workers concurrent goroutines, each
// folding rows into its own private Aggregator (Aggregator.Add is not safe
// for concurrent use, so no goroutine may share one), then merges all of
// them into a single Aggregator via Merge. workers <= 1 falls back to a
// single goroutine.
//
// If any worker's Add returns an error, RunParallel calls abort (typically
// the CancelFunc for the context.Context passed to the ingest.Ingest call
// that produces batches) so producer goroutines blocked sending on batches
// unblock promptly, and returns that error with a nil Aggregator.
func RunParallel(schema ingest.Schema, spec AggSpec, batches <-chan *ingest.Batch, workers int, abort func()) (*Aggregator, int64, error) {
	if workers < 1 {
		workers = 1
	}

	aggs := make([]*Aggregator, workers)
	rowCounts := make([]int64, workers)
	for i := range aggs {
		agg, err := New(schema, spec)
		if err != nil {
			return nil, 0, err
		}
		aggs[i] = agg
	}

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for b := range batches {
				if err := aggs[i].Add(b); err != nil {
					b.Release()
					errOnce.Do(func() {
						firstErr = err
						abort()
					})
					return
				}
				rowCounts[i] += int64(b.NumRows)
				b.Release()
			}
		}(i)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, 0, firstErr
	}

	var rows int64
	for _, c := range rowCounts {
		rows += c
	}

	final := aggs[0]
	for _, other := range aggs[1:] {
		if err := final.Merge(other); err != nil {
			return nil, 0, err
		}
	}
	return final, rows, nil
}
