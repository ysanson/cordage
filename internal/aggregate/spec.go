// Package aggregate consumes ingest.Batch values and computes group-by
// aggregates (sum, count, min, max, avg) over them.
package aggregate

import "fmt"

// AggFunc is an aggregate function requested for a measure column.
type AggFunc int

const (
	Sum AggFunc = iota
	Min
	Max
	Avg
)

func (f AggFunc) String() string {
	switch f {
	case Sum:
		return "sum"
	case Min:
		return "min"
	case Max:
		return "max"
	case Avg:
		return "avg"
	default:
		return fmt.Sprintf("AggFunc(%d)", int(f))
	}
}

// MeasureSpec requests one or more aggregate functions over a single
// numeric column.
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
