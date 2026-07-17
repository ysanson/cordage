package aggregate

import (
	"encoding/binary"
	"math"

	"github.com/ysanson/cordage/internal/ingest"
)

// measureAccum tracks running sum/min/max for one measure column within
// one group. Sum/min/max are kept in the column's native type (exact for
// int64, avoiding float64's 53-bit-mantissa precision loss); Avg is
// derived from sum/count at result time, always as float64.
//
// Float min/max use math.Min/math.Max rather than raw comparisons: those
// have IEEE-754-consistent, order-independent NaN propagation (a NaN
// poisons the running min/max regardless of when it arrives), whereas a
// hand-rolled "if v < min" comparison would silently behave differently
// depending on whether the NaN arrived first or later in the batch. Sum
// poisons to NaN through plain float addition — no special-casing needed.
type measureAccum struct {
	colType    ingest.ColumnType // TypeInt64 or TypeFloat64 (or TypeString, for a Distinct-only measure)
	sumI64     int64
	minI64     int64
	maxI64     int64
	sumF64     float64
	minF64     float64
	maxF64     float64
	haveMinMax bool
	distinct   *hyperLogLog // non-nil only if this measure requested Distinct
	digest     *tdigest     // non-nil only if this measure requested a percentile
}

// measureWants records which of a measure's optional structures a
// MeasureSpec's Funcs actually need, computed once in Aggregator.New —
// not touched per row — so newMeasureAccum only allocates a HyperLogLog
// sketch or t-digest when that specific measure actually requested
// Distinct or a percentile, rather than paying that cost for every group
// regardless of what was asked for.
type measureWants struct {
	Distinct   bool
	Percentile bool
}

func newMeasureAccum(colType ingest.ColumnType, wants measureWants) measureAccum {
	m := measureAccum{colType: colType}
	if wants.Distinct {
		m.distinct = newHyperLogLog()
	}
	if wants.Percentile {
		m.digest = newTDigest()
	}
	return m
}

func (m *measureAccum) add(v ingest.Value) {
	switch m.colType {
	case ingest.TypeInt64:
		m.sumI64 += v.I64
		if !m.haveMinMax {
			m.minI64, m.maxI64 = v.I64, v.I64
			m.haveMinMax = true
		} else {
			if v.I64 < m.minI64 {
				m.minI64 = v.I64
			}
			if v.I64 > m.maxI64 {
				m.maxI64 = v.I64
			}
		}
	case ingest.TypeFloat64:
		m.sumF64 += v.F64
		if !m.haveMinMax {
			m.minF64, m.maxF64 = v.F64, v.F64
			m.haveMinMax = true
		} else {
			m.minF64 = math.Min(m.minF64, v.F64)
			m.maxF64 = math.Max(m.maxF64, v.F64)
		}
	}

	if m.distinct != nil {
		m.distinct.add(hashValue(v))
	}
	if m.digest != nil {
		f := v.F64
		if m.colType == ingest.TypeInt64 {
			f = float64(v.I64)
		}
		m.digest.Add(f)
	}
}

// mergeMeasureAccum combines two accumulators for the same measure
// column (same colType, enforced by the caller) so that the result is
// identical to having fed every row through a single accumulator.
func mergeMeasureAccum(a, b measureAccum) measureAccum {
	out := measureAccum{colType: a.colType}
	switch a.colType {
	case ingest.TypeInt64:
		out.sumI64 = a.sumI64 + b.sumI64
	case ingest.TypeFloat64:
		out.sumF64 = a.sumF64 + b.sumF64
	}

	switch {
	case a.haveMinMax && b.haveMinMax:
		out.haveMinMax = true
		switch a.colType {
		case ingest.TypeInt64:
			out.minI64, out.maxI64 = min(a.minI64, b.minI64), max(a.maxI64, b.maxI64)
		case ingest.TypeFloat64:
			out.minF64, out.maxF64 = math.Min(a.minF64, b.minF64), math.Max(a.maxF64, b.maxF64)
		}
	case a.haveMinMax:
		out.haveMinMax = true
		out.minI64, out.maxI64, out.minF64, out.maxF64 = a.minI64, a.maxI64, a.minF64, a.maxF64
	case b.haveMinMax:
		out.haveMinMax = true
		out.minI64, out.maxI64, out.minF64, out.maxF64 = b.minI64, b.maxI64, b.minF64, b.maxF64
	}

	if a.distinct != nil {
		out.distinct = mergeHyperLogLog(a.distinct, b.distinct)
	}
	if a.digest != nil {
		// Reuse a.digest's storage rather than allocating a fresh one —
		// safe because a is a value receiver here (mergeMeasureAccum's
		// caller, groupAccum.merge, immediately overwrites its own
		// g.measures[i] with whatever this returns, and other/b must not
		// be reused after a Merge per Aggregator's existing contract).
		out.digest = a.digest
		out.digest.Merge(b.digest)
	}
	return out
}

// groupAccum is the running state for one group-by key.
type groupAccum struct {
	key      []ingest.Value // this group's dimension values, same for every row in it
	count    int64
	measures []measureAccum // parallel to AggSpec.Measures
}

func newGroupAccum(key []ingest.Value, measureTypes []ingest.ColumnType, wants []measureWants) *groupAccum {
	measures := make([]measureAccum, len(measureTypes))
	for i, t := range measureTypes {
		measures[i] = newMeasureAccum(t, wants[i])
	}
	return &groupAccum{key: key, measures: measures}
}

func (g *groupAccum) merge(other *groupAccum) {
	g.count += other.count
	for i := range g.measures {
		g.measures[i] = mergeMeasureAccum(g.measures[i], other.measures[i])
	}
}

// encodeGroupKey serializes dims into buf, reusing buf's backing array
// across calls, for feeding to a hash function (maphash.Bytes) — it is not
// itself required to be collision-free, since groupTable always verifies a
// probe hit against the stored dimension values directly (see
// groupTable.findByDims/dimsEqual). It's kept unambiguous anyway (no two
// distinct tuples ever encode to the same bytes): string segments are
// varint length-prefixed with their raw UTF-8 bytes (unlike joining values
// with a separator byte, which a string could itself contain); numeric
// segments are their native 8-byte representation with no length prefix,
// since a position's width is fixed by its column's type, not its value.
func encodeGroupKey(buf []byte, dims []ingest.Value) []byte {
	buf = buf[:0]
	for _, v := range dims {
		switch v.Type {
		case ingest.TypeString:
			buf = binary.AppendUvarint(buf, uint64(len(v.Str)))
			buf = append(buf, v.Str...)
		case ingest.TypeInt64:
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.I64))
		case ingest.TypeFloat64:
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v.F64))
		}
	}
	return buf
}
