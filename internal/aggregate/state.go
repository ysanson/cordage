package aggregate

import (
	"fmt"

	"github.com/ysanson/cordage/internal/ingest"
)

// GroupState is a plain, proto-agnostic snapshot of one group's raw
// accumulator state -- mergeable, unlike Result/GroupResult (which are
// finalized: Avg is already divided, Distinct is already rounded).
// ExportState/MergeState are the seam a wire-conversion layer (e.g.
// internal/distribute) builds on without this package importing any
// generated protobuf types.
//
// A GroupState returned by ExportState is a fully independent copy: it
// remains valid and unaffected by any further Add calls on the
// Aggregator it came from.
type GroupState struct {
	Key      []ingest.Value
	Count    int64
	Measures []MeasureState
}

// MeasureState is one measure column's raw accumulator state within one
// group (parallel to AggSpec.Measures, same convention as measureAccum).
// HLLRegisters is nil unless that measure requested Distinct; when
// present it is always exactly 1<<hllPrecision (16384) bytes.
//
// There is no field for percentile/t-digest state -- Percentile measures
// are rejected before this type is ever produced or consumed in a
// distributed context; see ValidateDistributable.
type MeasureState struct {
	ColType      ingest.ColumnType
	SumI64       int64
	MinI64       int64
	MaxI64       int64
	SumF64       float64
	MinF64       float64
	MaxF64       float64
	HaveMinMax   bool
	HLLRegisters []byte
}

// ExportState snapshots every group currently held by a, in unspecified
// order (same convention as groupTable.each -- callers needing
// deterministic output must sort separately, as Result() already does
// for its own purposes). Every returned slice, including HLLRegisters,
// is copied: mutating the Aggregator afterward (further Add calls) never
// affects a previously returned []GroupState.
func (a *Aggregator) ExportState() []GroupState {
	states := make([]GroupState, 0, a.table.size)
	a.table.each(func(g *groupAccum) {
		states = append(states, groupAccumToState(g))
	})
	return states
}

func groupAccumToState(g *groupAccum) GroupState {
	measures := make([]MeasureState, len(g.measures))
	for i, m := range g.measures {
		measures[i] = measureAccumToState(m)
	}
	return GroupState{
		Key:      append([]ingest.Value(nil), g.key...),
		Count:    g.count,
		Measures: measures,
	}
}

func measureAccumToState(m measureAccum) MeasureState {
	ms := MeasureState{
		ColType:    m.colType,
		SumI64:     m.sumI64,
		MinI64:     m.minI64,
		MaxI64:     m.maxI64,
		SumF64:     m.sumF64,
		MinF64:     m.minF64,
		MaxF64:     m.maxF64,
		HaveMinMax: m.haveMinMax,
	}
	if m.distinct != nil {
		ms.HLLRegisters = append([]byte(nil), m.distinct.registers...)
	}
	return ms
}

// MergeState folds externally-produced GroupState snapshots (typically
// decoded from a worker's gRPC response) into a, using the same
// insert-or-merge logic as Merge (groupTable.findByKey / insertNew /
// groupAccum.merge). Unlike Merge -- which steals other's *groupAccum
// pointers and forbids reusing other afterward -- MergeState treats
// states as a read-only, single-use value snapshot: it never mutates
// states, and states need not (and should not) be reused afterward
// either way.
//
// Returns an error -- without partially applying any of states -- if any
// GroupState's shape (key arity/types, measure count/types, HLL register
// presence/length) doesn't match a's own resolved AggSpec, or if any
// measure requests a percentile (a defense-in-depth check mirroring
// ValidateDistributable, in case a caller skipped that pre-flight: t-digest
// state has no wire representation here, so merging it in would silently
// lose data).
func (a *Aggregator) MergeState(states []GroupState) error {
	for i, wants := range a.measureWants {
		if wants.Percentile {
			return fmt.Errorf("aggregate: cannot MergeState: measure %q requests a percentile, which has no wire representation", a.spec.Measures[i].Column)
		}
	}

	accums := make([]*groupAccum, len(states))
	for i, s := range states {
		ga, err := stateToGroupAccum(a, s)
		if err != nil {
			return err
		}
		accums[i] = ga
	}

	for _, ga := range accums {
		hash := a.hashKey(ga.key)
		if g, ok := a.findByKey(hash, ga.key); ok {
			g.merge(ga)
		} else {
			a.table.insertNew(hash, ga)
		}
	}
	return nil
}

// stateToGroupAccum validates s against a's resolved shape (groupByTypes,
// measureTypes, measureWants) and builds a standalone *groupAccum from
// it, ready to be inserted via groupTable.insertNew or folded into an
// existing group via groupAccum.merge.
func stateToGroupAccum(a *Aggregator, s GroupState) (*groupAccum, error) {
	if len(s.Key) != len(a.groupByTypes) {
		return nil, fmt.Errorf("aggregate: MergeState: key has %d dimensions, want %d", len(s.Key), len(a.groupByTypes))
	}
	for i, v := range s.Key {
		if v.Type != a.groupByTypes[i] {
			return nil, fmt.Errorf("aggregate: MergeState: key dimension %d has type %s, want %s", i, v.Type, a.groupByTypes[i])
		}
	}
	if len(s.Measures) != len(a.measureTypes) {
		return nil, fmt.Errorf("aggregate: MergeState: state has %d measures, want %d", len(s.Measures), len(a.measureTypes))
	}

	measures := make([]measureAccum, len(s.Measures))
	for i, ms := range s.Measures {
		if ms.ColType != a.measureTypes[i] {
			return nil, fmt.Errorf("aggregate: MergeState: measure %d has type %s, want %s", i, ms.ColType, a.measureTypes[i])
		}
		wantsDistinct := a.measureWants[i].Distinct
		if wantsDistinct != (ms.HLLRegisters != nil) {
			return nil, fmt.Errorf("aggregate: MergeState: measure %d HLL register presence mismatch (want distinct=%v)", i, wantsDistinct)
		}

		m := measureAccum{
			colType:    ms.ColType,
			sumI64:     ms.SumI64,
			minI64:     ms.MinI64,
			maxI64:     ms.MaxI64,
			sumF64:     ms.SumF64,
			minF64:     ms.MinF64,
			maxF64:     ms.MaxF64,
			haveMinMax: ms.HaveMinMax,
		}
		if wantsDistinct {
			if len(ms.HLLRegisters) != 1<<hllPrecision {
				return nil, fmt.Errorf("aggregate: MergeState: measure %d has %d HLL registers, want %d", i, len(ms.HLLRegisters), 1<<hllPrecision)
			}
			m.distinct = &hyperLogLog{registers: append([]byte(nil), ms.HLLRegisters...)}
		}
		measures[i] = m
	}

	return &groupAccum{
		key:      append([]ingest.Value(nil), s.Key...),
		count:    s.Count,
		measures: measures,
	}, nil
}
