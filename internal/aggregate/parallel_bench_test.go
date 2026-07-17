package aggregate

import (
	"fmt"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

// buildBenchBatches builds totalRows synthetic rows directly as columnar
// ingest.Batch values (no CSV parsing), split into batchSize-row batches —
// isolating RunParallel's fan-out/Add/Merge cost from I/O, so this
// benchmark's scaling curve reflects aggregation-side concurrency alone.
func buildBenchBatches(totalRows, batchSize int) []*ingest.Batch {
	stations := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	var batches []*ingest.Batch
	for start := 0; start < totalRows; start += batchSize {
		end := start + batchSize
		if end > totalRows {
			end = totalRows
		}
		n := end - start
		strs := make([]string, n)
		f64s := make([]float64, n)
		for i := 0; i < n; i++ {
			idx := start + i
			strs[i] = stations[idx%len(stations)]
			f64s[i] = float64(idx%400)/10.0 - 20
		}
		batches = append(batches, &ingest.Batch{
			NumRows: n,
			Cols: []ingest.Column{
				{Type: ingest.TypeString, Strs: strs},
				{Type: ingest.TypeFloat64, F64s: f64s},
			},
		})
	}
	return batches
}

// BenchmarkRunParallel is the aggregation-side scaling signal for M1: a
// fixed set of pre-built in-memory batches, fanned out to workers
// concurrent Aggregators and merged, at increasing worker counts. Compare
// against BenchmarkIngestFileParallel (internal/ingest) to see whether
// aggregation or ingestion is the tighter constraint at a given worker count.
func BenchmarkRunParallel(b *testing.B) {
	const totalRows = 500_000
	const batchSize = 4096
	schema := ingest.Schema{
		Delimiter: ';',
		Columns: []ingest.ColumnSchema{
			{Name: "station", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{
		GroupBy:  []string{"station"},
		Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}}},
	}
	batches := buildBenchBatches(totalRows, batchSize)

	for _, workers := range []int{1, 2, 4, 6, 8, 10, 12, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ch := make(chan *ingest.Batch)
				go func() {
					defer close(ch)
					for _, bt := range batches {
						ch <- bt
					}
				}()

				_, rows, err := RunParallel(schema, spec, ch, workers, func() {})
				if err != nil {
					b.Fatalf("RunParallel: %v", err)
				}
				if rows != totalRows {
					b.Fatalf("got %d rows, want %d", rows, totalRows)
				}
			}
			b.ReportMetric(float64(totalRows), "rows/op")
		})
	}
}
