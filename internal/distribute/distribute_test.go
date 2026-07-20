package distribute

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/ingest"
)

// startTestWorker spins up a real loopback gRPC server (real TCP, not
// exec.Command) hosting a WorkerServer, and returns its address. The
// server is stopped when t's test finishes.
func startTestWorker(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	distproto.RegisterWorkerServer(srv, NewWorkerServer())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// writeTestCSV writes a station;temperature CSV (with a header line) to
// a temp file and returns its path plus the number of data rows written.
func writeTestCSV(t *testing.T, rows int) (path string, dataRows int) {
	t.Helper()
	stations := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo", "Fukuoka", "Sendai", "Kobe"}
	var sb strings.Builder
	sb.WriteString("station;temperature\n")
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%s;%.1f\n", stations[i%len(stations)], float64(i%400)/10.0-20)
	}
	p := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(p, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write CSV: %v", err)
	}
	return p, rows
}

func testDistributeSchema() ingest.Schema {
	return ingest.Schema{
		Delimiter: ';',
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "station", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temperature", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
}

func testDistributeSpec() aggregate.AggSpec {
	return aggregate.AggSpec{
		GroupBy: []string{"station"},
		Measures: []aggregate.MeasureSpec{
			{Column: "temperature", Funcs: []aggregate.AggFunc{aggregate.Sum, aggregate.Min, aggregate.Max, aggregate.Avg}},
			{Column: "station", Funcs: []aggregate.AggFunc{aggregate.Distinct}},
		},
	}
}

func singleNodeResult(t *testing.T, path string, schema ingest.Schema, spec aggregate.AggSpec) *aggregate.Result {
	t.Helper()
	src, err := ingest.NewFileSource(path)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	agg, err := aggregate.New(schema, spec)
	if err != nil {
		t.Fatalf("aggregate.New: %v", err)
	}

	batches, errs := ingest.Ingest(context.Background(), src, ingest.Config{Schema: schema, Chunks: 1})
	for b := range batches {
		if err := agg.Add(b); err != nil {
			t.Fatalf("Add: %v", err)
		}
		b.Release()
	}
	if err := <-errs; err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	return agg.Result()
}

func approxEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// assertResultsEqual compares two Results group-for-group and
// measure-for-measure. Float measures compare within tolerance (summing
// the same values in a different grouping order -- single accumulator
// vs. merged shard partials -- is not bit-exact, per IEEE-754's
// non-associativity, matching this repo's existing test convention in
// internal/aggregate); everything else (int64 sums/counts, HLL-backed
// Distinct) is exact.
func assertResultsEqual(t *testing.T, got, want *aggregate.Result) {
	t.Helper()
	if len(got.Groups) != len(want.Groups) {
		t.Fatalf("got %d groups, want %d", len(got.Groups), len(want.Groups))
	}
	for i := range want.Groups {
		wantG, gotG := want.Groups[i], got.Groups[i]
		for j := range wantG.Key {
			if wantG.Key[j] != gotG.Key[j] {
				t.Fatalf("group %d key[%d] = %+v, want %+v", i, j, gotG.Key[j], wantG.Key[j])
			}
		}
		if wantG.Count != gotG.Count {
			t.Errorf("group %d (%v): Count = %d, want %d", i, wantG.Key, gotG.Count, wantG.Count)
		}
		for j := range wantG.Measures {
			wantM, gotM := wantG.Measures[j], gotG.Measures[j]
			if wantM.Value.Type != gotM.Value.Type {
				t.Errorf("group %d: measure %d type = %v, want %v", i, j, gotM.Value.Type, wantM.Value.Type)
			}
			if !approxEqual(gotM.Value.F64, wantM.Value.F64) || gotM.Value.I64 != wantM.Value.I64 {
				t.Errorf("group %d (%v): measure %s(%s) = %+v, want %+v", i, wantG.Key, wantM.Func, wantM.Column, gotM.Value, wantM.Value)
			}
		}
	}
}

func TestRunDistributedMatchesSingleNode(t *testing.T) {
	path, _ := writeTestCSV(t, 5000)
	schema := testDistributeSchema()
	spec := testDistributeSpec()

	wantResult := singleNodeResult(t, path, schema, spec)

	const numWorkers = 4
	addrs := make([]string, numWorkers)
	for i := range addrs {
		addrs[i] = startTestWorker(t)
	}

	shards, err := PlanShards(path, addrs, schema)
	if err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	if len(shards) != numWorkers {
		t.Fatalf("got %d shards, want %d", len(shards), numWorkers)
	}

	agg, rows, skipped, err := RunDistributed(context.Background(), Options{
		Schema:   schema,
		Spec:     spec,
		FilePath: path,
		Shards:   shards,
	})
	if err != nil {
		t.Fatalf("RunDistributed: %v", err)
	}
	if skipped != 0 {
		t.Errorf("skipped = %d, want 0", skipped)
	}
	if rows != 5000 {
		t.Errorf("rows = %d, want 5000", rows)
	}

	assertResultsEqual(t, agg.Result(), wantResult)
}

func TestRunDistributedRejectsPercentile(t *testing.T) {
	path, _ := writeTestCSV(t, 100)
	schema := testDistributeSchema()
	spec := aggregate.AggSpec{
		GroupBy:  []string{"station"},
		Measures: []aggregate.MeasureSpec{{Column: "temperature", Funcs: []aggregate.AggFunc{aggregate.Percentile(50)}}},
	}

	// No workers needed: ValidateDistributable must fire before any dial.
	_, _, _, err := RunDistributed(context.Background(), Options{
		Schema:   schema,
		Spec:     spec,
		FilePath: path,
		Shards:   []Shard{{Addr: "127.0.0.1:1", Offset: 0, Length: 1}},
	})
	if err == nil {
		t.Fatal("expected error for a Percentile measure, got nil")
	}
}

func TestRunDistributedAbortsOnWorkerError(t *testing.T) {
	path, _ := writeTestCSV(t, 100)
	schema := testDistributeSchema()
	spec := testDistributeSpec()

	goodAddr := startTestWorker(t)

	shards := []Shard{
		{Addr: goodAddr, Offset: 0, Length: 20, SkipHeader: true},
		// Unreachable address: forces a dial/RPC failure on this shard.
		{Addr: "127.0.0.1:1", Offset: 20, Length: 20},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, _, err := RunDistributed(ctx, Options{
		Schema:   schema,
		Spec:     spec,
		FilePath: path,
		Shards:   shards,
	})
	if err == nil {
		t.Fatal("expected an error from the unreachable shard, got nil")
	}
}

func TestPlanShardsHeaderSkipOnlyOnFirstShard(t *testing.T) {
	path, dataRows := writeTestCSV(t, 2000)
	schema := testDistributeSchema()

	addrs := []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}
	shards, err := PlanShards(path, addrs, schema)
	if err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	if len(shards) != len(addrs) {
		t.Fatalf("got %d shards, want %d", len(shards), len(addrs))
	}
	for i, sh := range shards {
		want := i == 0
		if sh.SkipHeader != want {
			t.Errorf("shard %d SkipHeader = %v, want %v", i, sh.SkipHeader, want)
		}
	}

	// Confirm no row is double-counted or dropped across a shard
	// boundary: run each shard through a real WorkerServer and sum rows.
	srv := NewWorkerServer()
	var total int64
	for i, sh := range shards {
		resp, err := srv.RunShard(context.Background(), &distproto.ShardRequest{
			Schema:   mustSchemaToProto(t, schema, sh.SkipHeader),
			Spec:     mustSpecToProto(t, testDistributeSpec()),
			FilePath: path,
			Offset:   sh.Offset,
			Length:   sh.Length,
		})
		if err != nil {
			t.Fatalf("RunShard %d: %v", i, err)
		}
		total += resp.GetRows()
	}
	if total != int64(dataRows) {
		t.Errorf("total rows across shards = %d, want %d", total, dataRows)
	}
}

func mustSchemaToProto(t *testing.T, s ingest.Schema, hasHeader bool) *distproto.Schema {
	t.Helper()
	p, err := schemaToProto(s)
	if err != nil {
		t.Fatalf("schemaToProto: %v", err)
	}
	p.HasHeader = hasHeader
	return p
}

func mustSpecToProto(t *testing.T, s aggregate.AggSpec) *distproto.AggSpec {
	t.Helper()
	p, err := specToProto(s)
	if err != nil {
		t.Fatalf("specToProto: %v", err)
	}
	return p
}
