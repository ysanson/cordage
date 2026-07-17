package aggregate

import (
	"fmt"
	"hash/maphash"
	"math"

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
	measureTypes []ingest.ColumnType // parallel to measureIdx; numeric unless the only requested func is Distinct
	measureWants []measureWants      // parallel to measureIdx; which optional structures each measure's Funcs need

	table  groupTable
	seed   maphash.Seed
	keyBuf []byte // reused scratch buffer for encodeGroupKey (generic, 0-or-2+-dim path only)
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
	measureWantsList := make([]measureWants, len(spec.Measures))
	for i, ms := range spec.Measures {
		idx, ok := colIndex[ms.Column]
		if !ok {
			return nil, fmt.Errorf("aggregate: unknown measure column %q", ms.Column)
		}
		t := schema.Columns[idx].Type

		var wants measureWants
		for _, fn := range ms.Funcs {
			if fn.requiresNumeric() && t != ingest.TypeInt64 && t != ingest.TypeFloat64 {
				return nil, fmt.Errorf("aggregate: measure column %q: %s requires a numeric column (int64 or float64), got %s", ms.Column, fn, t)
			}
			switch fn.kind {
			case kindDistinct:
				wants.Distinct = true
			case kindPercentile:
				wants.Percentile = true
			}
		}

		measureIdx[i] = idx
		measureTypes[i] = t
		measureWantsList[i] = wants
	}

	return &Aggregator{
		schema:       schema,
		spec:         spec,
		groupByIdx:   groupByIdx,
		groupByTypes: groupByTypes,
		measureIdx:   measureIdx,
		measureTypes: measureTypes,
		measureWants: measureWantsList,
		table:        newGroupTable(),
		seed:         maphash.MakeSeed(),
	}, nil
}

// Add folds one batch's rows into the running per-group accumulators. It
// does not call batch.Release() — that remains the caller's
// responsibility, matching the existing Ingest consumer pattern.
//
// The single-group-by-column cases (by far the most common shape, e.g. the
// 1BRC-style "GROUP BY station") skip encodeGroupKey entirely and hash the
// column's native value directly; everything else (zero or 2+ columns)
// goes through the generic encodeGroupKey + maphash.Bytes path.
func (a *Aggregator) Add(batch *ingest.Batch) error {
	if len(batch.Cols) != len(a.schema.Columns) {
		return fmt.Errorf("aggregate: batch has %d columns, schema has %d", len(batch.Cols), len(a.schema.Columns))
	}

	if len(a.groupByIdx) == 1 {
		col := batch.Cols[a.groupByIdx[0]]
		switch col.Type {
		case ingest.TypeString:
			for r := 0; r < batch.NumRows; r++ {
				s := col.Strs[r]
				hash := maphash.String(a.seed, s)
				g, ok := a.table.findByString(hash, s)
				if !ok {
					g = newGroupAccum([]ingest.Value{{Type: ingest.TypeString, Str: s}}, a.measureTypes, a.measureWants)
					a.table.insertNew(hash, g)
				}
				a.addMeasures(g, batch, r)
			}
			return nil
		case ingest.TypeInt64:
			for r := 0; r < batch.NumRows; r++ {
				v := col.I64s[r]
				hash := maphash.Comparable(a.seed, v)
				g, ok := a.table.findByI64(hash, v)
				if !ok {
					g = newGroupAccum([]ingest.Value{{Type: ingest.TypeInt64, I64: v}}, a.measureTypes, a.measureWants)
					a.table.insertNew(hash, g)
				}
				a.addMeasures(g, batch, r)
			}
			return nil
		case ingest.TypeFloat64:
			for r := 0; r < batch.NumRows; r++ {
				v := col.F64s[r]
				// hash the bit pattern, not v itself: Go's runtime float64
				// hash returns a fresh random value on every call for any
				// NaN (so a NaN key can never be found in a built-in map),
				// which would make maphash.Comparable(seed, v) hash the
				// same NaN bit pattern differently across rows.
				hash := maphash.Comparable(a.seed, math.Float64bits(v))
				g, ok := a.table.findByF64(hash, v)
				if !ok {
					g = newGroupAccum([]ingest.Value{{Type: ingest.TypeFloat64, F64: v}}, a.measureTypes, a.measureWants)
					a.table.insertNew(hash, g)
				}
				a.addMeasures(g, batch, r)
			}
			return nil
		}
	}

	dimBuf := make([]ingest.Value, len(a.groupByIdx))
	for r := 0; r < batch.NumRows; r++ {
		for i, idx := range a.groupByIdx {
			dimBuf[i] = readValue(batch.Cols[idx], r)
		}

		a.keyBuf = encodeGroupKey(a.keyBuf, dimBuf)
		hash := maphash.Bytes(a.seed, a.keyBuf)
		g, ok := a.table.findByDims(hash, dimBuf)
		if !ok {
			key := append([]ingest.Value(nil), dimBuf...)
			g = newGroupAccum(key, a.measureTypes, a.measureWants)
			a.table.insertNew(hash, g)
		}
		a.addMeasures(g, batch, r)
	}
	return nil
}

func (a *Aggregator) addMeasures(g *groupAccum, batch *ingest.Batch, row int) {
	g.count++
	for i, idx := range a.measureIdx {
		g.measures[i].add(readValue(batch.Cols[idx], row))
	}
}

// hashKey and findByKey re-derive a key's hash/lookup under this
// Aggregator's own seed, dispatching by shape the same way Add does. Merge
// needs these because other's cached hashes were computed under other's
// seed, which is not valid for probing a's table.
func (a *Aggregator) hashKey(key []ingest.Value) uint64 {
	if len(key) == 1 {
		switch key[0].Type {
		case ingest.TypeString:
			return maphash.String(a.seed, key[0].Str)
		case ingest.TypeInt64:
			return maphash.Comparable(a.seed, key[0].I64)
		case ingest.TypeFloat64:
			return maphash.Comparable(a.seed, math.Float64bits(key[0].F64))
		}
	}
	a.keyBuf = encodeGroupKey(a.keyBuf, key)
	return maphash.Bytes(a.seed, a.keyBuf)
}

func (a *Aggregator) findByKey(hash uint64, key []ingest.Value) (*groupAccum, bool) {
	if len(key) == 1 {
		switch key[0].Type {
		case ingest.TypeString:
			return a.table.findByString(hash, key[0].Str)
		case ingest.TypeInt64:
			return a.table.findByI64(hash, key[0].I64)
		case ingest.TypeFloat64:
			return a.table.findByF64(hash, key[0].F64)
		}
	}
	return a.table.findByDims(hash, key)
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
	groups := make([]GroupResult, 0, a.table.size)
	a.table.each(func(g *groupAccum) {
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
	})
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

	other.table.each(func(og *groupAccum) {
		hash := a.hashKey(og.key)
		if g, ok := a.findByKey(hash, og.key); ok {
			g.merge(og)
		} else {
			a.table.insertNew(hash, og)
		}
	})
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
