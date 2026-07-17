package ingest

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func benchCSVContent(rows int) string {
	stations := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	var sb strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%s;%.1f\n", stations[i%len(stations)], float64(i%400)/10.0-20)
	}
	return sb.String()
}

// BenchmarkIngestFile is the hot-loop regression signal for sequential,
// single-chunk ingestion (the M0 path) — self-contained (no dependency
// on the external, gitignored benchmarks/ dataset), so it runs with a
// plain `go test -bench`.
func BenchmarkIngestFile(b *testing.B) {
	const rowsPerRun = 500_000
	path := writeTempFile(b, benchCSVContent(rowsPerRun))
	schema := Schema{
		Delimiter: ';',
		Columns: []ColumnSchema{
			{Name: "station", Type: TypeString, Kind: KindDimension},
			{Name: "temp", Type: TypeFloat64, Kind: KindMeasure},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src, err := NewFileSource(path)
		if err != nil {
			b.Fatalf("NewFileSource: %v", err)
		}

		batches, errs := Ingest(context.Background(), src, Config{Schema: schema})
		var rows int
		for bt := range batches {
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
	}
	b.ReportMetric(float64(rowsPerRun), "rows/op")
}
