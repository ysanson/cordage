package aggregate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

func writeBenchCSV(b *testing.B, rows int) string {
	b.Helper()
	stations := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	var sb strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%s;%.1f\n", stations[i%len(stations)], float64(i%400)/10.0-20)
	}
	path := filepath.Join(b.TempDir(), "bench.csv")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		b.Fatalf("write bench CSV: %v", err)
	}
	return path
}

// BenchmarkPipeline is the full Ingest->Aggregate hot-loop regression
// signal — self-contained (a small synthetic file, not the external
// benchmarks/ dataset), so `go test -bench` works with no setup step.
// The realistic 10M-row, real-city-distribution number lives in
// BENCHMARKS.md, generated via cmd/gen1brc and the cordage CLI; this
// benchmark is for tracking regressions commit-to-commit with benchstat,
// not for that headline number.
func BenchmarkPipeline(b *testing.B) {
	const rowsPerRun = 500_000
	path := writeBenchCSV(b, rowsPerRun)
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src, err := ingest.NewFileSource(path)
		if err != nil {
			b.Fatalf("NewFileSource: %v", err)
		}

		agg, err := New(schema, spec)
		if err != nil {
			b.Fatalf("New: %v", err)
		}

		batches, errs := ingest.Ingest(context.Background(), src, ingest.Config{Schema: schema})
		var rows int
		for bt := range batches {
			if err := agg.Add(bt); err != nil {
				b.Fatalf("Add: %v", err)
			}
			rows += bt.NumRows
			bt.Release()
		}
		if err := <-errs; err != nil {
			b.Fatalf("Ingest: %v", err)
		}
		src.Close()

		if rows != rowsPerRun {
			b.Fatalf("got %d rows, want %d", rows, rowsPerRun)
		}
		_ = agg.Result()
	}
	b.ReportMetric(float64(rowsPerRun), "rows/op")
}
