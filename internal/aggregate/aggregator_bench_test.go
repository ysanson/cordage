package aggregate

import (
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

// benchBatch builds a single large synthetic batch: numRows spread
// across a fixed set of group-by keys, used as the hot-loop regression
// signal for Aggregator.Add called out by M0's benchmark-harness item.
func benchBatch(numRows int) *ingest.Batch {
	cities := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	cols := []ingest.Column{
		{Type: ingest.TypeString, Strs: make([]string, 0, numRows)},
		{Type: ingest.TypeFloat64, F64s: make([]float64, 0, numRows)},
	}
	for i := 0; i < numRows; i++ {
		cols[0].Strs = append(cols[0].Strs, cities[i%len(cities)])
		cols[1].F64s = append(cols[1].F64s, float64(i%1000)*0.1)
	}
	return &ingest.Batch{NumRows: numRows, Cols: cols}
}

func BenchmarkAggregatorAdd(b *testing.B) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "city", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{
		GroupBy:  []string{"city"},
		Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}}},
	}
	const rowsPerBatch = 10_000
	batch := benchBatch(rowsPerBatch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg, err := New(schema, spec)
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		if err := agg.Add(batch); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
	b.ReportMetric(float64(rowsPerBatch), "rows/op")
}
