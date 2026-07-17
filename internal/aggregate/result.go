package aggregate

import (
	"cmp"
	"math"
	"sort"

	"github.com/ysanson/cordage/internal/ingest"
)

// Result is the output of one Aggregator.Result() call.
type Result struct {
	GroupByColumns []string
	Groups         []GroupResult
}

// GroupResult is one group's key and requested measures. Count is the
// number of rows that fell into this group, always populated regardless
// of what Measures were requested (it's needed internally for Avg
// anyway, so exposing it is free).
type GroupResult struct {
	Key      []ingest.Value
	Count    int64
	Measures []MeasureResult
}

// MeasureResult is one requested aggregate function's value for one
// measure column within one group.
//
// Value's type depends on Func: Sum/Min/Max preserve the source column's
// native type (TypeInt64 or TypeFloat64) — exact for int64, avoiding
// float64's 53-bit-mantissa precision loss on large sums/values. Avg and
// Percentile are always TypeFloat64 (an average or a quantile is
// definitionally a ratio/estimate, regardless of the source column's
// type); Distinct is always TypeInt64 (an estimated count). A caller
// switching on Func needs to know this before reading Value.
type MeasureResult struct {
	Column string
	Func   AggFunc
	Value  ingest.Value
}

// measureResultValue computes fn's output value for one measure
// accumulator, given that group's row count (needed for Avg).
func measureResultValue(m measureAccum, count int64, fn AggFunc) ingest.Value {
	nativeSum := func() ingest.Value {
		if m.colType == ingest.TypeInt64 {
			return ingest.Value{Type: ingest.TypeInt64, I64: m.sumI64}
		}
		return ingest.Value{Type: ingest.TypeFloat64, F64: m.sumF64}
	}

	switch fn.kind {
	case kindSum:
		return nativeSum()
	case kindMin:
		if m.colType == ingest.TypeInt64 {
			return ingest.Value{Type: ingest.TypeInt64, I64: m.minI64}
		}
		return ingest.Value{Type: ingest.TypeFloat64, F64: m.minF64}
	case kindMax:
		if m.colType == ingest.TypeInt64 {
			return ingest.Value{Type: ingest.TypeInt64, I64: m.maxI64}
		}
		return ingest.Value{Type: ingest.TypeFloat64, F64: m.maxF64}
	case kindAvg:
		sum := m.sumF64
		if m.colType == ingest.TypeInt64 {
			sum = float64(m.sumI64)
		}
		return ingest.Value{Type: ingest.TypeFloat64, F64: sum / float64(count)}
	case kindDistinct:
		return ingest.Value{Type: ingest.TypeInt64, I64: int64(math.Round(m.distinct.estimate()))}
	case kindPercentile:
		return ingest.Value{Type: ingest.TypeFloat64, F64: m.digest.Quantile(fn.q / 100)}
	default:
		return ingest.Value{}
	}
}

// sortGroups orders groups by a lexicographic tuple comparison over Key,
// giving Result() deterministic output regardless of map iteration
// order. keyTypes[i] is the ColumnType of every Key[i] across all
// groups (fixed once at Aggregator construction, since GroupBy names
// resolve to specific schema columns) — so this is a per-position,
// type-consistent comparator, not a cross-type one.
func sortGroups(groups []GroupResult, keyTypes []ingest.ColumnType) {
	sort.Slice(groups, func(i, j int) bool {
		return compareKeys(groups[i].Key, groups[j].Key, keyTypes) < 0
	})
}

// compareKeys compares two group keys position by position, using
// cmp.Compare for the numeric types — which defines a true total order
// for float64 including NaN (NaN sorts below all non-NaN, NaN == NaN),
// so a group key containing NaN still sorts deterministically.
func compareKeys(a, b []ingest.Value, keyTypes []ingest.ColumnType) int {
	for i, t := range keyTypes {
		var c int
		switch t {
		case ingest.TypeString:
			c = cmp.Compare(a[i].Str, b[i].Str)
		case ingest.TypeInt64:
			c = cmp.Compare(a[i].I64, b[i].I64)
		case ingest.TypeFloat64:
			c = cmp.Compare(a[i].F64, b[i].F64)
		}
		if c != 0 {
			return c
		}
	}
	return 0
}
