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

// AggFuncKind is the exported form of AggFunc's internal funcKind, for
// callers (e.g. internal/distribute's proto conversion) that need to
// inspect or reconstruct an AggFunc without this package depending on
// their wire format.
type AggFuncKind int

const (
	FuncSum AggFuncKind = iota
	FuncMin
	FuncMax
	FuncAvg
	FuncDistinct
	FuncPercentile
)

// Kind reports which family of function f is.
func (f AggFunc) Kind() AggFuncKind { return AggFuncKind(f.kind) }

// Quantile reports f's requested quantile (0-100 scale). Meaningful only
// when f.Kind() == FuncPercentile.
func (f AggFunc) Quantile() float64 { return f.q }

// AggFuncFromKind reconstructs an AggFunc from its exported Kind and (for
// FuncPercentile) quantile -- the inverse of Kind()/Quantile(), for
// decoding an AggFunc back out of a wire representation.
func AggFuncFromKind(kind AggFuncKind, quantile float64) (AggFunc, error) {
	switch kind {
	case FuncSum:
		return Sum, nil
	case FuncMin:
		return Min, nil
	case FuncMax:
		return Max, nil
	case FuncAvg:
		return Avg, nil
	case FuncDistinct:
		return Distinct, nil
	case FuncPercentile:
		return Percentile(quantile), nil
	default:
		return AggFunc{}, fmt.Errorf("aggregate: unknown AggFuncKind %d", int(kind))
	}
}

// ValidateDistributable reports an error if spec requests any Percentile
// measure. Distributed (gRPC) execution cannot merge t-digest state
// across processes in M3 -- GroupState/MeasureState have no wire
// representation for it. Callers distributing an AggSpec (the
// coordinator and each WorkerServer alike) should call this before
// dispatching or executing any shard, so a Percentile request fails fast
// with a clear error instead of silently returning a wrong or missing
// value.
func ValidateDistributable(spec AggSpec) error {
	for _, ms := range spec.Measures {
		for _, fn := range ms.Funcs {
			if fn.Kind() == FuncPercentile {
				return fmt.Errorf("aggregate: measure %q requests %s, which distributed mode does not support; run single-node instead", ms.Column, fn)
			}
		}
	}
	return nil
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
