package aggregate

import (
	"strconv"
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

// benchCompositeKeyBatch builds a batch group-by-able on two string
// columns, exercising the generic (non-single-dim) grouping path — no
// existing benchmark covered this shape before M2.
func benchCompositeKeyBatch(numRows int) *ingest.Batch {
	cities := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	regions := []string{"East", "West", "North", "South"}
	cols := []ingest.Column{
		{Type: ingest.TypeString, Strs: make([]string, 0, numRows)},
		{Type: ingest.TypeString, Strs: make([]string, 0, numRows)},
		{Type: ingest.TypeFloat64, F64s: make([]float64, 0, numRows)},
	}
	for i := 0; i < numRows; i++ {
		cols[0].Strs = append(cols[0].Strs, cities[i%len(cities)])
		cols[1].Strs = append(cols[1].Strs, regions[i%len(regions)])
		cols[2].F64s = append(cols[2].F64s, float64(i%1000)*0.1)
	}
	return &ingest.Batch{NumRows: numRows, Cols: cols}
}

func BenchmarkAggregatorAddCompositeKey(b *testing.B) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "city", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "region", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{
		GroupBy:  []string{"city", "region"},
		Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}}},
	}
	const rowsPerBatch = 10_000
	batch := benchCompositeKeyBatch(rowsPerBatch)

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

// benchHighCardinalityBatch builds a batch with numGroups distinct
// group-by keys (one per row, in the worst case) — used to check the
// groupTable's growth/load-factor behavior degrades gracefully well beyond
// the ~8-104 group shapes the other benchmarks and the real 1BRC dataset
// exercise.
func benchHighCardinalityBatch(numRows, numGroups int) *ingest.Batch {
	cols := []ingest.Column{
		{Type: ingest.TypeString, Strs: make([]string, 0, numRows)},
		{Type: ingest.TypeFloat64, F64s: make([]float64, 0, numRows)},
	}
	for i := 0; i < numRows; i++ {
		cols[0].Strs = append(cols[0].Strs, "key-"+strconv.Itoa(i%numGroups))
		cols[1].F64s = append(cols[1].F64s, float64(i%1000)*0.1)
	}
	return &ingest.Batch{NumRows: numRows, Cols: cols}
}

func BenchmarkAggregatorAddHighCardinality(b *testing.B) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "key", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{
		GroupBy:  []string{"key"},
		Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}}},
	}
	const rowsPerBatch = 50_000
	const numGroups = 20_000
	batch := benchHighCardinalityBatch(rowsPerBatch, numGroups)

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
