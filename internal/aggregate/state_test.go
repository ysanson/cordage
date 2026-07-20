package aggregate

import (
	"strconv"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

// assertResultsEqual compares two Results for exact group-for-group,
// measure-for-measure equality (float64 measures via approxEqual, per
// this file's existing convention -- see TestMergeMatchesSingleAggregator).
func assertResultsEqual(t *testing.T, got, want *Result) {
	t.Helper()
	if len(got.Groups) != len(want.Groups) {
		t.Fatalf("got %d groups, want %d", len(got.Groups), len(want.Groups))
	}
	for i := range want.Groups {
		wantG, gotG := want.Groups[i], got.Groups[i]
		if len(wantG.Key) != len(gotG.Key) {
			t.Fatalf("group %d key length = %d, want %d", i, len(gotG.Key), len(wantG.Key))
		}
		for j := range wantG.Key {
			if wantG.Key[j] != gotG.Key[j] {
				t.Fatalf("group %d key[%d] = %+v, want %+v", i, j, gotG.Key[j], wantG.Key[j])
			}
		}
		if wantG.Count != gotG.Count {
			t.Errorf("group %d: Count = %d, want %d", i, gotG.Count, wantG.Count)
		}
		for j := range wantG.Measures {
			wantM, gotM := wantG.Measures[j], gotG.Measures[j]
			if wantM.Value.Type != gotM.Value.Type {
				t.Errorf("group %d: measure %d type = %v, want %v", i, j, gotM.Value.Type, wantM.Value.Type)
			}
			if !approxEqual(gotM.Value.F64, wantM.Value.F64) || gotM.Value.I64 != wantM.Value.I64 {
				t.Errorf("group %d: measure %s(%s) = %+v, want %+v", i, wantM.Func, wantM.Column, gotM.Value, wantM.Value)
			}
		}
	}
}

func distinctSpec() (ingest.Schema, AggSpec) {
	schema := testSchema()
	spec := AggSpec{
		GroupBy: []string{"city"},
		Measures: []MeasureSpec{
			{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}},
			{Column: "hits", Funcs: []AggFunc{Sum, Min, Max, Distinct}},
		},
	}
	return schema, spec
}

func manyRows(n int) [][]string {
	cities := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo"}
	rows := make([][]string, n)
	for i := 0; i < n; i++ {
		rows[i] = []string{
			cities[i%len(cities)],
			strconv.FormatFloat(float64(i)*0.37, 'f', -1, 64),
			strconv.Itoa(i % 13),
		}
	}
	return rows
}

func TestExportStateMergeStateRoundTrip(t *testing.T) {
	schema, spec := distinctSpec()

	source, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := source.Add(buildBatch(t, schema, manyRows(200))); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := source.Result()

	dest, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := dest.MergeState(source.ExportState()); err != nil {
		t.Fatalf("MergeState: %v", err)
	}

	assertResultsEqual(t, dest.Result(), wantResult)
}

func TestMergeStateMatchesInProcessMerge(t *testing.T) {
	schema, spec := distinctSpec()
	allRows := manyRows(200)

	single, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := single.Add(buildBatch(t, schema, allRows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := single.Result()

	const k = 4
	final, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	chunk := len(allRows) / k
	for i := 0; i < k; i++ {
		start, end := i*chunk, (i+1)*chunk
		if i == k-1 {
			end = len(allRows)
		}
		worker, err := New(schema, spec)
		if err != nil {
			t.Fatalf("New worker %d: %v", i, err)
		}
		if err := worker.Add(buildBatch(t, schema, allRows[start:end])); err != nil {
			t.Fatalf("Add worker %d: %v", i, err)
		}
		if err := final.MergeState(worker.ExportState()); err != nil {
			t.Fatalf("MergeState worker %d: %v", i, err)
		}
	}

	assertResultsEqual(t, final.Result(), wantResult)
}

func TestMergeStateDistinctIsExact(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{Measures: []MeasureSpec{{Column: "city", Funcs: []AggFunc{Distinct}}}}
	allRows := manyRows(500)

	single, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := single.Add(buildBatch(t, schema, allRows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := single.Result()
	wantDistinct := findMeasure(t, wantResult.Groups[0], "city", Distinct).Value.I64

	mid := len(allRows) / 2
	part1, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	part2, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := part1.Add(buildBatch(t, schema, allRows[:mid])); err != nil {
		t.Fatalf("Add part1: %v", err)
	}
	if err := part2.Add(buildBatch(t, schema, allRows[mid:])); err != nil {
		t.Fatalf("Add part2: %v", err)
	}

	final, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := final.MergeState(part1.ExportState()); err != nil {
		t.Fatalf("MergeState part1: %v", err)
	}
	if err := final.MergeState(part2.ExportState()); err != nil {
		t.Fatalf("MergeState part2: %v", err)
	}

	gotDistinct := findMeasure(t, final.Result().Groups[0], "city", Distinct).Value.I64
	if gotDistinct != wantDistinct {
		t.Errorf("merged distinct(city) via MergeState = %d, want exactly %d (HLL merge is exact)", gotDistinct, wantDistinct)
	}
}

func TestExportStateIsIndependentSnapshot(t *testing.T) {
	schema, spec := distinctSpec()

	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(buildBatch(t, schema, manyRows(50))); err != nil {
		t.Fatalf("Add: %v", err)
	}

	snapshot := agg.ExportState()
	var before GroupState
	for _, s := range snapshot {
		if s.Key[0].Str == "Tokyo" {
			before = s
			break
		}
	}
	if before.Key == nil {
		t.Fatalf("no Tokyo group in snapshot")
	}
	beforeHLL := append([]byte(nil), before.Measures[1].HLLRegisters...)
	beforeCount := before.Count

	// Keep adding rows for the same groups after the snapshot was taken.
	if err := agg.Add(buildBatch(t, schema, manyRows(50))); err != nil {
		t.Fatalf("Add after export: %v", err)
	}

	if before.Count != beforeCount {
		t.Errorf("snapshot Count changed after further Add: got %d, want %d", before.Count, beforeCount)
	}
	for i, b := range beforeHLL {
		if before.Measures[1].HLLRegisters[i] != b {
			t.Fatalf("snapshot HLLRegisters mutated after further Add at index %d", i)
			break
		}
	}
}

func TestMergeStateRejectsMismatchedShape(t *testing.T) {
	schema, spec := distinctSpec()

	newTarget := func(t *testing.T) *Aggregator {
		t.Helper()
		agg, err := New(schema, spec)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agg
	}

	validState := func() GroupState {
		return GroupState{
			Key:   []ingest.Value{{Type: ingest.TypeString, Str: "Tokyo"}},
			Count: 1,
			Measures: []MeasureState{
				{ColType: ingest.TypeFloat64, SumF64: 1, MinF64: 1, MaxF64: 1, HaveMinMax: true},
				{ColType: ingest.TypeInt64, SumI64: 1, MinI64: 1, MaxI64: 1, HaveMinMax: true, HLLRegisters: make([]byte, 1<<hllPrecision)},
			},
		}
	}

	t.Run("wrong key arity", func(t *testing.T) {
		agg := newTarget(t)
		s := validState()
		s.Key = append(s.Key, ingest.Value{Type: ingest.TypeString, Str: "extra"})
		if err := agg.MergeState([]GroupState{s}); err == nil {
			t.Error("expected error for wrong key arity, got nil")
		}
	})

	t.Run("wrong key type", func(t *testing.T) {
		agg := newTarget(t)
		s := validState()
		s.Key[0] = ingest.Value{Type: ingest.TypeInt64, I64: 1}
		if err := agg.MergeState([]GroupState{s}); err == nil {
			t.Error("expected error for wrong key type, got nil")
		}
	})

	t.Run("wrong measure count", func(t *testing.T) {
		agg := newTarget(t)
		s := validState()
		s.Measures = s.Measures[:1]
		if err := agg.MergeState([]GroupState{s}); err == nil {
			t.Error("expected error for wrong measure count, got nil")
		}
	})

	t.Run("missing HLL registers when distinct requested", func(t *testing.T) {
		agg := newTarget(t)
		s := validState()
		s.Measures[1].HLLRegisters = nil
		if err := agg.MergeState([]GroupState{s}); err == nil {
			t.Error("expected error for missing HLL registers, got nil")
		}
	})

	t.Run("wrong HLL register length", func(t *testing.T) {
		agg := newTarget(t)
		s := validState()
		s.Measures[1].HLLRegisters = make([]byte, 10)
		if err := agg.MergeState([]GroupState{s}); err == nil {
			t.Error("expected error for wrong HLL register length, got nil")
		}
	})

	t.Run("no partial mutation on error", func(t *testing.T) {
		agg := newTarget(t)
		good := validState()
		bad := validState()
		bad.Key = append(bad.Key, ingest.Value{Type: ingest.TypeString, Str: "extra"})
		if err := agg.MergeState([]GroupState{good, bad}); err == nil {
			t.Fatal("expected error, got nil")
		}
		if len(agg.Result().Groups) != 0 {
			t.Errorf("MergeState partially applied state despite returning an error: got %d groups, want 0", len(agg.Result().Groups))
		}
	})
}

func TestValidateDistributableRejectsPercentile(t *testing.T) {
	spec := AggSpec{Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Percentile(50)}}}}
	if err := ValidateDistributable(spec); err == nil {
		t.Error("expected error for a Percentile measure, got nil")
	}

	spec = AggSpec{Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Avg, Distinct}}}}
	if err := ValidateDistributable(spec); err != nil {
		t.Errorf("expected no error for Sum/Avg/Distinct, got %v", err)
	}
}

func TestMergeStateRejectsPercentileDefenseInDepth(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Percentile(50)}}}}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.MergeState(nil); err == nil {
		t.Error("expected MergeState to reject a Percentile-bearing Aggregator even with no states, got nil")
	}
}
