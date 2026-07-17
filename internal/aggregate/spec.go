// Package aggregate consumes ingest.Batch values and computes group-by
// aggregates (sum, count, min, max, avg, distinct, percentile) over them.
package aggregate

import (
	"fmt"
	"strconv"
)

// funcKind is the family of aggregate function an AggFunc represents.
type funcKind int

const (
	kindSum funcKind = iota
	kindMin
	kindMax
	kindAvg
	kindDistinct
	kindPercentile
)

// AggFunc is an aggregate function requested for a measure column.
// Percentile functions carry a quantile parameter (0-100 scale — "p99" is
// Percentile(99)), which is why AggFunc is a small struct rather than a
// bare int enum: a fixed set of constants can't express an arbitrary
// quantile like p99.9.
type AggFunc struct {
	kind funcKind
	q    float64 // meaningful only when kind == kindPercentile
}

var (
	Sum      = AggFunc{kind: kindSum}
	Min      = AggFunc{kind: kindMin}
	Max      = AggFunc{kind: kindMax}
	Avg      = AggFunc{kind: kindAvg}
	Distinct = AggFunc{kind: kindDistinct}
)

// Percentile returns the AggFunc requesting the qth percentile, on a 0-100
// scale (Percentile(99) is "p99", Percentile(99.9) is "p99.9").
func Percentile(q float64) AggFunc {
	return AggFunc{kind: kindPercentile, q: q}
}

func (f AggFunc) String() string {
	switch f.kind {
	case kindSum:
		return "sum"
	case kindMin:
		return "min"
	case kindMax:
		return "max"
	case kindAvg:
		return "avg"
	case kindDistinct:
		return "distinct"
	case kindPercentile:
		return "p" + strconv.FormatFloat(f.q, 'f', -1, 64)
	default:
		return fmt.Sprintf("AggFunc(kind=%d)", int(f.kind))
	}
}

// requiresNumeric reports whether f can only be computed over a numeric
// (int64 or float64) measure column. Distinct is the only exception — it
// counts distinct values of any column type, string included.
func (f AggFunc) requiresNumeric() bool {
	return f.kind != kindDistinct
}

// MeasureSpec requests one or more aggregate functions over a single
// column.
type MeasureSpec struct {
	Column string
	Funcs  []AggFunc
}

// AggSpec is the query: which columns to group by, and which aggregate
// functions to compute over which measure columns. Every group's row
// count is always computed and returned, regardless of Measures — it's
// needed internally for Avg anyway, so exposing it is free.
//
// GroupBy may be empty, meaning a single implicit global group (e.g. a
// bare "AVG(temp)" with no GROUP BY).
type AggSpec struct {
	GroupBy  []string
	Measures []MeasureSpec
}
