package aggregate

import (
	"math"
	"strconv"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// buildBatch constructs an *ingest.Batch directly from string fields,
// parsed per schema.Columns' declared types — a self-contained stand-in
// for running rows through the real ingest parser, since tests here only
// need Aggregator's input shape, not ingest's parsing behavior.
func buildBatch(t *testing.T, schema ingest.Schema, rows [][]string) *ingest.Batch {
	t.Helper()
	cols := make([]ingest.Column, len(schema.Columns))
	for i, cs := range schema.Columns {
		cols[i].Type = cs.Type
	}
	for _, row := range rows {
		for i, val := range row {
			switch schema.Columns[i].Type {
			case ingest.TypeString:
				cols[i].Strs = append(cols[i].Strs, val)
			case ingest.TypeInt64:
				n, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					t.Fatalf("bad int64 value %q: %v", val, err)
				}
				cols[i].I64s = append(cols[i].I64s, n)
			case ingest.TypeFloat64:
				f, err := strconv.ParseFloat(val, 64)
				if err != nil {
					t.Fatalf("bad float64 value %q: %v", val, err)
				}
				cols[i].F64s = append(cols[i].F64s, f)
			}
		}
	}
	return &ingest.Batch{NumRows: len(rows), Cols: cols}
}

func testSchema() ingest.Schema {
	return ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "city", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
			{Name: "hits", Type: ingest.TypeInt64, Kind: ingest.KindMeasure},
		},
	}
}

func testRows() [][]string {
	return [][]string{
		{"Tokyo", "23.5", "10"},
		{"Osaka", "21.0", "5"},
		{"Tokyo", "18.2", "7"},
		{"Kyoto", "19.8", "3"},
		{"Osaka", "25.0", "8"},
	}
}

func findGroup(t *testing.T, res *Result, city string) GroupResult {
	t.Helper()
	for _, g := range res.Groups {
		if g.Key[0].Str == city {
			return g
		}
	}
	t.Fatalf("no group found for city %q in %+v", city, res.Groups)
	return GroupResult{}
}

func findMeasure(t *testing.T, g GroupResult, column string, fn AggFunc) MeasureResult {
	t.Helper()
	for _, m := range g.Measures {
		if m.Column == column && m.Func == fn {
			return m
		}
	}
	t.Fatalf("no measure %s(%s) found in %+v", fn, column, g.Measures)
	return MeasureResult{}
}

func TestAggregatorSumMinMaxAvgCount(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{
		GroupBy: []string{"city"},
		Measures: []MeasureSpec{
			{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}},
			{Column: "hits", Funcs: []AggFunc{Sum, Min, Max}},
		},
	}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(buildBatch(t, schema, testRows())); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()

	if len(res.Groups) != 3 {
		t.Fatalf("got %d groups, want 3", len(res.Groups))
	}
	// Result() must sort groups; confirm alphabetical order directly.
	wantOrder := []string{"Kyoto", "Osaka", "Tokyo"}
	for i, want := range wantOrder {
		if got := res.Groups[i].Key[0].Str; got != want {
			t.Errorf("Groups[%d] = %q, want %q", i, got, want)
		}
	}

	cases := []struct {
		city                               string
		count                              int64
		tempSum, tempMin, tempMax, tempAvg float64
		hitsSum, hitsMin, hitsMax          int64
	}{
		{"Tokyo", 2, 41.7, 18.2, 23.5, 20.85, 17, 7, 10},
		{"Osaka", 2, 46.0, 21.0, 25.0, 23.0, 13, 5, 8},
		{"Kyoto", 1, 19.8, 19.8, 19.8, 19.8, 3, 3, 3},
	}
	for _, c := range cases {
		g := findGroup(t, res, c.city)
		if g.Count != c.count {
			t.Errorf("%s: Count = %d, want %d", c.city, g.Count, c.count)
		}

		sum := findMeasure(t, g, "temp", Sum)
		if sum.Value.Type != ingest.TypeFloat64 || !approxEqual(sum.Value.F64, c.tempSum) {
			t.Errorf("%s: temp Sum = %+v, want float64 %v", c.city, sum.Value, c.tempSum)
		}
		min := findMeasure(t, g, "temp", Min)
		if min.Value.Type != ingest.TypeFloat64 || !approxEqual(min.Value.F64, c.tempMin) {
			t.Errorf("%s: temp Min = %+v, want float64 %v", c.city, min.Value, c.tempMin)
		}
		max := findMeasure(t, g, "temp", Max)
		if max.Value.Type != ingest.TypeFloat64 || !approxEqual(max.Value.F64, c.tempMax) {
			t.Errorf("%s: temp Max = %+v, want float64 %v", c.city, max.Value, c.tempMax)
		}
		avg := findMeasure(t, g, "temp", Avg)
		if avg.Value.Type != ingest.TypeFloat64 || !approxEqual(avg.Value.F64, c.tempAvg) {
			t.Errorf("%s: temp Avg = %+v, want float64 %v", c.city, avg.Value, c.tempAvg)
		}

		hitsSum := findMeasure(t, g, "hits", Sum)
		if hitsSum.Value.Type != ingest.TypeInt64 || hitsSum.Value.I64 != c.hitsSum {
			t.Errorf("%s: hits Sum = %+v, want int64 %v", c.city, hitsSum.Value, c.hitsSum)
		}
		hitsMin := findMeasure(t, g, "hits", Min)
		if hitsMin.Value.Type != ingest.TypeInt64 || hitsMin.Value.I64 != c.hitsMin {
			t.Errorf("%s: hits Min = %+v, want int64 %v", c.city, hitsMin.Value, c.hitsMin)
		}
		hitsMax := findMeasure(t, g, "hits", Max)
		if hitsMax.Value.Type != ingest.TypeInt64 || hitsMax.Value.I64 != c.hitsMax {
			t.Errorf("%s: hits Max = %+v, want int64 %v", c.city, hitsMax.Value, c.hitsMax)
		}
	}
}

func TestAggregatorEmptyGroupBy(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Avg}}}}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(buildBatch(t, schema, testRows())); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	if len(res.Groups) != 1 {
		t.Fatalf("got %d groups, want 1 (implicit global group)", len(res.Groups))
	}
	g := res.Groups[0]
	if len(g.Key) != 0 {
		t.Errorf("Key = %+v, want empty", g.Key)
	}
	if g.Count != 5 {
		t.Errorf("Count = %d, want 5", g.Count)
	}
	wantSum := 23.5 + 21.0 + 18.2 + 19.8 + 25.0
	sum := findMeasure(t, g, "temp", Sum)
	if !approxEqual(sum.Value.F64, wantSum) {
		t.Errorf("Sum = %v, want %v", sum.Value.F64, wantSum)
	}
	avg := findMeasure(t, g, "temp", Avg)
	if !approxEqual(avg.Value.F64, wantSum/5) {
		t.Errorf("Avg = %v, want %v", avg.Value.F64, wantSum/5)
	}
}

func TestNewConstructionErrors(t *testing.T) {
	schema := testSchema()

	cases := []struct {
		name string
		spec AggSpec
	}{
		{"unknown group-by column", AggSpec{GroupBy: []string{"bogus"}}},
		{"duplicate group-by column", AggSpec{GroupBy: []string{"city", "city"}}},
		{"unknown measure column", AggSpec{Measures: []MeasureSpec{{Column: "bogus", Funcs: []AggFunc{Sum}}}}},
		{"non-numeric measure column", AggSpec{Measures: []MeasureSpec{{Column: "city", Funcs: []AggFunc{Avg}}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(schema, c.spec); err == nil {
				t.Fatalf("New: expected an error for %s", c.name)
			}
		})
	}
}

func TestAggregatorNaNPoisonsRegardlessOfOrder(t *testing.T) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "city", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{GroupBy: []string{"city"}, Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max, Avg}}}}

	rows := [][]string{
		{"NaNFirst", "NaN"},
		{"NaNFirst", "5.0"},
		{"NaNLast", "5.0"},
		{"NaNLast", "NaN"},
	}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(buildBatch(t, schema, rows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()

	for _, city := range []string{"NaNFirst", "NaNLast"} {
		g := findGroup(t, res, city)
		for _, fn := range []AggFunc{Sum, Min, Max, Avg} {
			v := findMeasure(t, g, "temp", fn)
			if !math.IsNaN(v.Value.F64) {
				t.Errorf("%s: %s = %v, want NaN", city, fn, v.Value.F64)
			}
		}
	}
}

func TestAggregatorCompositeKeyNoCollision(t *testing.T) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "a", Type: ingest.TypeString, Kind: ingest.KindDimension},
			{Name: "b", Type: ingest.TypeString, Kind: ingest.KindDimension},
		},
	}
	spec := AggSpec{GroupBy: []string{"a", "b"}}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Under a separator-byte-join key scheme using 0x1F, both rows would
	// format to the same joined string ("x\x1fy\x1fz"). They must NOT
	// collide into one group.
	rows := [][]string{
		{"x\x1fy", "z"},
		{"x", "y\x1fz"},
	}
	if err := agg.Add(buildBatch(t, schema, rows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	if len(res.Groups) != 2 {
		t.Fatalf("got %d groups, want 2 (no collision)", len(res.Groups))
	}
}

func TestAggregatorZeroRows(t *testing.T) {
	agg, err := New(testSchema(), AggSpec{GroupBy: []string{"city"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res := agg.Result()
	if len(res.Groups) != 0 {
		t.Fatalf("got %d groups, want 0", len(res.Groups))
	}
}

func TestMergeMatchesSingleAggregator(t *testing.T) {
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
	for i := 0; i < 200; i++ {
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
	if err := part1.Merge(part2); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	gotResult := part1.Result()

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
}

func TestAggregatorSingleNumericGroupBy(t *testing.T) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "year", Type: ingest.TypeInt64, Kind: ingest.KindDimension},
			{Name: "temp", Type: ingest.TypeFloat64, Kind: ingest.KindMeasure},
		},
	}
	spec := AggSpec{GroupBy: []string{"year"}, Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum, Min, Max}}}}
	rows := [][]string{
		{"2020", "10.0"},
		{"2021", "20.0"},
		{"2020", "30.0"},
	}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(buildBatch(t, schema, rows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	if len(res.Groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(res.Groups))
	}
	for _, g := range res.Groups {
		switch g.Key[0].I64 {
		case 2020:
			if g.Count != 2 {
				t.Errorf("year 2020: Count = %d, want 2", g.Count)
			}
			sum := findMeasure(t, g, "temp", Sum)
			if !approxEqual(sum.Value.F64, 40.0) {
				t.Errorf("year 2020: Sum = %v, want 40.0", sum.Value.F64)
			}
		case 2021:
			if g.Count != 1 {
				t.Errorf("year 2021: Count = %d, want 1", g.Count)
			}
		default:
			t.Errorf("unexpected group key %v", g.Key[0])
		}
	}
}

// TestAggregatorFloatDimensionNaNBitwiseIdentity documents an explicit M2
// behavior change: previously, every NaN float64 group-by dimension value
// collapsed into a single group (an accident of strconv.AppendFloat always
// formatting NaN as the literal string "NaN"). The new hash-table design
// compares dimension values by bit pattern, so distinct NaN bit patterns
// are distinct groups, and identical bit patterns are the same group.
func TestAggregatorFloatDimensionNaNBitwiseIdentity(t *testing.T) {
	schema := ingest.Schema{
		HasHeader: true,
		Columns: []ingest.ColumnSchema{
			{Name: "val", Type: ingest.TypeFloat64, Kind: ingest.KindDimension},
		},
	}
	spec := AggSpec{GroupBy: []string{"val"}}

	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0x7ff8000000000002)
	col := ingest.Column{Type: ingest.TypeFloat64, F64s: []float64{nan1, nan1, nan2}}
	batch := &ingest.Batch{NumRows: 3, Cols: []ingest.Column{col}}

	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agg.Add(batch); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	if len(res.Groups) != 2 {
		t.Fatalf("got %d groups, want 2 (distinct NaN bit patterns are distinct groups)", len(res.Groups))
	}
	var countByBits = map[uint64]int64{}
	for _, g := range res.Groups {
		countByBits[math.Float64bits(g.Key[0].F64)] = g.Count
	}
	if countByBits[math.Float64bits(nan1)] != 2 {
		t.Errorf("nan1 group Count = %d, want 2 (identical bit patterns merge)", countByBits[math.Float64bits(nan1)])
	}
	if countByBits[math.Float64bits(nan2)] != 1 {
		t.Errorf("nan2 group Count = %d, want 1", countByBits[math.Float64bits(nan2)])
	}
}

// TestAggregatorDistinctOnStringColumn exercises the per-(column,func)
// validation restructure: Distinct must be permitted on a string
// dimension column, even though Sum/Min/Max/Avg/Percentile are not.
func TestAggregatorDistinctOnStringColumn(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{
		GroupBy:  []string{"hits"},
		Measures: []MeasureSpec{{Column: "city", Funcs: []AggFunc{Distinct}}},
	}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rows := [][]string{
		{"Tokyo", "1.0", "5"},
		{"Osaka", "1.0", "5"},
		{"Tokyo", "1.0", "5"},
		{"Kyoto", "1.0", "9"},
	}
	if err := agg.Add(buildBatch(t, schema, rows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	for _, g := range res.Groups {
		m := findMeasure(t, g, "city", Distinct)
		if m.Value.Type != ingest.TypeInt64 {
			t.Fatalf("Distinct value type = %v, want TypeInt64", m.Value.Type)
		}
		switch g.Key[0].I64 {
		case 5:
			if m.Value.I64 != 2 {
				t.Errorf("hits=5: distinct(city) = %d, want 2 (Tokyo, Osaka)", m.Value.I64)
			}
		case 9:
			if m.Value.I64 != 1 {
				t.Errorf("hits=9: distinct(city) = %d, want 1 (Kyoto)", m.Value.I64)
			}
		}
	}
}

// TestAggregatorPercentileArbitraryQuantile proves the AggFunc struct
// widening actually works end to end: any quantile (not just a fixed
// preset) can be requested via the public Percentile(q) constructor.
func TestAggregatorPercentileArbitraryQuantile(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{
		Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Percentile(37.5), Percentile(99.9)}}},
	}
	agg, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var rows [][]string
	for i := 1; i <= 1000; i++ {
		rows = append(rows, []string{"x", strconv.Itoa(i), "0"})
	}
	if err := agg.Add(buildBatch(t, schema, rows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	res := agg.Result()
	g := res.Groups[0]
	p375 := findMeasure(t, g, "temp", Percentile(37.5))
	if p375.Value.Type != ingest.TypeFloat64 {
		t.Fatalf("p37.5 value type = %v, want TypeFloat64", p375.Value.Type)
	}
	if p375.Value.F64 < 300 || p375.Value.F64 > 450 {
		t.Errorf("p37.5 = %v, want roughly 375 (values are 1..1000)", p375.Value.F64)
	}
	if got := p375.Func.String(); got != "p37.5" {
		t.Errorf("Func.String() = %q, want %q", got, "p37.5")
	}
	p999 := findMeasure(t, g, "temp", Percentile(99.9))
	if p999.Value.F64 < 950 {
		t.Errorf("p99.9 = %v, want close to 1000", p999.Value.F64)
	}
}

// TestMergeDistinctIsExact confirms HyperLogLog's exact/order-independent
// merge holds through the real Aggregator.Merge path, not just at the
// hyperLogLog level directly.
func TestMergeDistinctIsExact(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{Measures: []MeasureSpec{{Column: "city", Funcs: []AggFunc{Distinct}}}}

	cities := []string{"Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo"}
	var allRows [][]string
	for i := 0; i < 500; i++ {
		allRows = append(allRows, []string{cities[i%len(cities)], "1.0", "0"})
	}

	single, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := single.Add(buildBatch(t, schema, allRows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := single.Result()

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
	if err := part1.Merge(part2); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	gotResult := part1.Result()

	wantDistinct := findMeasure(t, wantResult.Groups[0], "city", Distinct).Value.I64
	gotDistinct := findMeasure(t, gotResult.Groups[0], "city", Distinct).Value.I64
	if gotDistinct != wantDistinct {
		t.Errorf("merged distinct(city) = %d, want exactly %d (HLL merge is exact)", gotDistinct, wantDistinct)
	}
}

// TestMergePercentileWithinTolerance: t-digest merge order isn't
// perfectly order-independent (documented, expected), so this checks
// "close," not "identical," through the real Aggregator.Merge path.
func TestMergePercentileWithinTolerance(t *testing.T) {
	schema := testSchema()
	spec := AggSpec{Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Percentile(50), Percentile(99)}}}}

	var allRows [][]string
	for i := 1; i <= 2000; i++ {
		allRows = append(allRows, []string{"x", strconv.Itoa(i), "0"})
	}

	single, err := New(schema, spec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := single.Add(buildBatch(t, schema, allRows)); err != nil {
		t.Fatalf("Add: %v", err)
	}
	wantResult := single.Result()

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
	if err := part1.Merge(part2); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	gotResult := part1.Result()

	for _, fn := range []AggFunc{Percentile(50), Percentile(99)} {
		want := findMeasure(t, wantResult.Groups[0], "temp", fn).Value.F64
		got := findMeasure(t, gotResult.Groups[0], "temp", fn).Value.F64
		if math.Abs(got-want) > 20 {
			t.Errorf("%s: merged=%v, single=%v, diff %v exceeds tolerance", fn, got, want, math.Abs(got-want))
		}
	}
}

func TestMergeMismatchedSpecError(t *testing.T) {
	schema := testSchema()
	a, err := New(schema, AggSpec{GroupBy: []string{"city"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := New(schema, AggSpec{GroupBy: []string{"city"}, Measures: []MeasureSpec{{Column: "temp", Funcs: []AggFunc{Sum}}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Merge(b); err == nil {
		t.Fatal("expected an error merging aggregators with different specs")
	}
}
