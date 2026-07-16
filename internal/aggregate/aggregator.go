package aggregate

import (
	"fmt"

	"github.com/ysanson/cordage/internal/ingest"
)

// Aggregator computes group-by aggregates over a stream of ingest.Batch
// values, one Add call per batch. It is not safe for concurrent use:
// Add and Merge must not be called concurrently on the same Aggregator,
// and Merge is only valid after both sides have finished all their Add
// calls. After a.Merge(other) returns successfully, other's internal
// state has been moved into a — other must not be used again.
type Aggregator struct {
	schema ingest.Schema
	spec   AggSpec

	groupByIdx   []int               // schema column index, per AggSpec.GroupBy entry
	groupByTypes []ingest.ColumnType // parallel to groupByIdx

	measureIdx   []int               // schema column index, per AggSpec.Measures entry
	measureTypes []ingest.ColumnType // parallel to measureIdx; always TypeInt64 or TypeFloat64

	groups map[string]*groupAccum
	keyBuf []byte // reused scratch buffer for encodeGroupKey
}

// New resolves spec's column names against schema and validates it, so
// that Add's per-row work never needs to look anything up by name.
func New(schema ingest.Schema, spec AggSpec) (*Aggregator, error) {
	colIndex := make(map[string]int, len(schema.Columns))
	for i, c := range schema.Columns {
		colIndex[c.Name] = i
	}

	groupByIdx := make([]int, len(spec.GroupBy))
	groupByTypes := make([]ingest.ColumnType, len(spec.GroupBy))
	seen := make(map[string]bool, len(spec.GroupBy))
	for i, name := range spec.GroupBy {
		if seen[name] {
			return nil, fmt.Errorf("aggregate: duplicate group-by column %q", name)
		}
		seen[name] = true
		idx, ok := colIndex[name]
		if !ok {
			return nil, fmt.Errorf("aggregate: unknown group-by column %q", name)
		}
		groupByIdx[i] = idx
		groupByTypes[i] = schema.Columns[idx].Type
	}

	measureIdx := make([]int, len(spec.Measures))
	measureTypes := make([]ingest.ColumnType, len(spec.Measures))
	for i, ms := range spec.Measures {
		idx, ok := colIndex[ms.Column]
		if !ok {
			return nil, fmt.Errorf("aggregate: unknown measure column %q", ms.Column)
		}
		t := schema.Columns[idx].Type
		if t != ingest.TypeInt64 && t != ingest.TypeFloat64 {
			return nil, fmt.Errorf("aggregate: measure column %q must be numeric (int64 or float64), got %s", ms.Column, t)
		}
		measureIdx[i] = idx
		measureTypes[i] = t
	}

	return &Aggregator{
		schema:       schema,
		spec:         spec,
		groupByIdx:   groupByIdx,
		groupByTypes: groupByTypes,
		measureIdx:   measureIdx,
		measureTypes: measureTypes,
		groups:       make(map[string]*groupAccum),
	}, nil
}

// Add folds one batch's rows into the running per-group accumulators. It
// does not call batch.Release() — that remains the caller's
// responsibility, matching the existing Ingest consumer pattern.
func (a *Aggregator) Add(batch *ingest.Batch) error {
	if len(batch.Cols) != len(a.schema.Columns) {
		return fmt.Errorf("aggregate: batch has %d columns, schema has %d", len(batch.Cols), len(a.schema.Columns))
	}

	dimBuf := make([]ingest.Value, len(a.groupByIdx))
	for r := 0; r < batch.NumRows; r++ {
		for i, idx := range a.groupByIdx {
			dimBuf[i] = readValue(batch.Cols[idx], r)
		}

		a.keyBuf = encodeGroupKey(a.keyBuf, dimBuf)
		g, ok := a.groups[string(a.keyBuf)] // no-alloc read: string(a.keyBuf) used directly as the index expression
		if !ok {
			key := append([]ingest.Value(nil), dimBuf...)
			g = newGroupAccum(key, a.measureTypes)
			a.groups[string(a.keyBuf)] = g // insert: this copy is unavoidable and only happens once per new group
		}

		g.count++
		for i, idx := range a.measureIdx {
			g.measures[i].add(readValue(batch.Cols[idx], r))
		}
	}
	return nil
}

func readValue(col ingest.Column, row int) ingest.Value {
	switch col.Type {
	case ingest.TypeString:
		return ingest.Value{Type: ingest.TypeString, Str: col.Strs[row]}
	case ingest.TypeInt64:
		return ingest.Value{Type: ingest.TypeInt64, I64: col.I64s[row]}
	case ingest.TypeFloat64:
		return ingest.Value{Type: ingest.TypeFloat64, F64: col.F64s[row]}
	default:
		return ingest.Value{}
	}
}

// Result snapshots the current accumulator state, sorted deterministically
// by group key (see sortGroups).
func (a *Aggregator) Result() *Result {
	groups := make([]GroupResult, 0, len(a.groups))
	for _, g := range a.groups {
		measures := make([]MeasureResult, 0, len(a.spec.Measures))
		for i, ms := range a.spec.Measures {
			for _, fn := range ms.Funcs {
				measures = append(measures, MeasureResult{
					Column: ms.Column,
					Func:   fn,
					Value:  measureResultValue(g.measures[i], g.count, fn),
				})
			}
		}
		groups = append(groups, GroupResult{Key: g.key, Count: g.count, Measures: measures})
	}
	sortGroups(groups, a.groupByTypes)

	return &Result{
		GroupByColumns: append([]string(nil), a.spec.GroupBy...),
		Groups:         groups,
	}
}

// Merge folds other's per-group accumulators into a — the M1 forward-compat
// seam: a future parallel-aggregation path runs one Aggregator per worker
// and folds them together with Merge. M0 never calls this.
func (a *Aggregator) Merge(other *Aggregator) error {
	if !specsEqual(a.spec, other.spec) {
		return fmt.Errorf("aggregate: cannot merge aggregators with different specs")
	}
	for i := range a.measureTypes {
		if a.measureTypes[i] != other.measureTypes[i] {
			return fmt.Errorf("aggregate: cannot merge aggregators: measure %q has mismatched resolved types (%s vs %s)",
				a.spec.Measures[i].Column, a.measureTypes[i], other.measureTypes[i])
		}
	}

	for key, og := range other.groups {
		if g, ok := a.groups[key]; ok {
			g.merge(og)
		} else {
			a.groups[key] = og
		}
	}
	return nil
}

// specsEqual compares two AggSpecs field-by-field, order-sensitive: both
// sides of a Merge come from the same schema+spec in the real M1 path,
// so requiring identical order (rather than just the same set) is the
// correct bar.
func specsEqual(a, b AggSpec) bool {
	if len(a.GroupBy) != len(b.GroupBy) {
		return false
	}
	for i := range a.GroupBy {
		if a.GroupBy[i] != b.GroupBy[i] {
			return false
		}
	}
	if len(a.Measures) != len(b.Measures) {
		return false
	}
	for i := range a.Measures {
		if a.Measures[i].Column != b.Measures[i].Column {
			return false
		}
		if len(a.Measures[i].Funcs) != len(b.Measures[i].Funcs) {
			return false
		}
		for j := range a.Measures[i].Funcs {
			if a.Measures[i].Funcs[j] != b.Measures[i].Funcs[j] {
				return false
			}
		}
	}
	return true
}
