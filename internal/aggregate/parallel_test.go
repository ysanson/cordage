package aggregate

import (
	"strconv"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

// splitIntoBatches feeds rows through a real channel as one *ingest.Batch
// per row, so RunParallel's worker goroutines genuinely race to receive
// batches rather than each getting a pre-assigned, deterministic slice.
func splitIntoBatches(t *testing.T, schema ingest.Schema, rows [][]string) <-chan *ingest.Batch {
	t.Helper()
	out := make(chan *ingest.Batch)
	go func() {
		defer close(out)
		for _, row := range rows {
			out <- buildBatch(t, schema, [][]string{row})
		}
	}()
	return out
}

func TestRunParallelMatchesSingleAggregator(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{
		GroupBy: []string{"city"},
		Measures: []MeasureSpec{
			{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}},
			{Column: "hits", Funcs: []AggFunc{Sum, Min, Max}},
		},
	}

	cities := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo"}
	var allRows [][]string
	for i := 0; i < 500; i++ {
		city := cities[i%len(cities)]
		temp := strconv.FormatFloat(float64(i)*0.37, 'f', -1, 64)
		hits := strconv.Itoa(i % 13)
		allRows = append(allRows, []string{city, temp, hits})
	}

	single, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := single.Add(buildBatch(t, schema, allRows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := single.Result()

	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			batches := splitIntoBatches(t, schema, allRows)
			agg, rows, err := RunParallel(schema, spec, batches, workers, func() {})
			if err != nil {
				t.Fatalf("RunParallel: %v", err)
			}
			if rows != int64(len(allRows)) {
				t.Fatalf("rows = %d, want %d", rows, len(allRows))
			}

			gotResult := agg.Result()
			if len(gotResult.Groups) != len(wantResult.Groups) {
				t.Fatalf("got %d groups, want %d", len(gotResult.Groups), len(wantResult.Groups))
			}
			for i := range wantResult.Groups {
				wantG, gotG := wantResult.Groups[i], gotResult.Groups[i]
				if wantG.Key[0].Str != gotG.Key[0].Str {
					t.Fatalf("group %d key = %q, want %q", i, gotG.Key[0].Str, wantG.Key[0].Str)
				}
				if wantG.Count != gotG.Count {
					t.Errorf("%s: Count = %d, want %d", wantG.Key[0].Str, gotG.Count, wantG.Count)
				}
				for j := range wantG.Measures {
					wantM, gotM := wantG.Measures[j], gotG.Measures[j]
					if wantM.Value.Type != gotM.Value.Type {
						t.Errorf("%s: measure %d type = %v, want %v", wantG.Key[0].Str, j, gotM.Value.Type, wantM.Value.Type)
					}
					if !approxEqual(gotM.Value.F64, wantM.Value.F64) || gotM.Value.I64 != wantM.Value.I64 {
						t.Errorf("%s: measure %s(%s) = %+v, want %+v", wantG.Key[0].Str, wantM.Func, wantM.Column, gotM.Value, wantM.Value)
					}
				}
			}
		})
	}
}

func TestRunParallelAddErrorAborts(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{GroupBy: []string{"city"}, Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum}}}}

	// A batch with a mismatched column count makes every worker's Add fail,
	// exercising the abort path without relying on timing/races to trigger it.
	badBatch := &ingest.Batch{NumRows: 1, Cols: []ingest.Column{{Type: ingest.TypeString, Strs: []string{"Tokyo"}}}}

	const workers = 4
	batches := make(chan *ingest.Batch)
	go func() {
		defer close(batches)
		for i := 0; i < workers; i++ {
			batches <- badBatch
		}
	}()

	aborted := make(chan struct{}, 1)
	abort := func() {
		select {
		case aborted <- struct{}{}:
		default:
		}
	}

	_, _, err := RunParallel(schema, spec, batches, workers, abort)
	if err == nil {
		t.Fatal("RunParallel: got nil error, want a column-count mismatch error")
	}
	select {
	case <-aborted:
	default:
		t.Fatal("abort was never called")
	}
}
