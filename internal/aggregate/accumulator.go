package aggregate

import (
	"encoding/binary"
	"math"
	"strconv"

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
	colType    ingest.ColumnType // TypeInt64 or TypeFloat64
	sumI64     int64
	minI64     int64
	maxI64     int64
	sumF64     float64
	minF64     float64
	maxF64     float64
	haveMinMax bool
}

func newMeasureAccum(colType ingest.ColumnType) measureAccum {
	return measureAccum{colType: colType}
}

func (m *measureAccum) add(v ingest.Value) {
	switch m.colType {
	case ingest.TypeInt64:
		m.sumI64 += v.I64
		if !m.haveMinMax {
			m.minI64, m.maxI64 = v.I64, v.I64
			m.haveMinMax = true
			return
		}
		if v.I64 < m.minI64 {
			m.minI64 = v.I64
		}
		if v.I64 > m.maxI64 {
			m.maxI64 = v.I64
		}
	case ingest.TypeFloat64:
		m.sumF64 += v.F64
		if !m.haveMinMax {
			m.minF64, m.maxF64 = v.F64, v.F64
			m.haveMinMax = true
			return
		}
		m.minF64 = math.Min(m.minF64, v.F64)
		m.maxF64 = math.Max(m.maxF64, v.F64)
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
	return out
}

// groupAccum is the running state for one group-by key.
type groupAccum struct {
	key      []ingest.Value // this group's dimension values, same for every row in it
	count    int64
	measures []measureAccum // parallel to AggSpec.Measures
}

func newGroupAccum(key []ingest.Value, measureTypes []ingest.ColumnType) *groupAccum {
	measures := make([]measureAccum, len(measureTypes))
	for i, t := range measureTypes {
		measures[i] = newMeasureAccum(t)
	}
	return &groupAccum{key: key, measures: measures}
}

func (g *groupAccum) merge(other *groupAccum) {
	g.count += other.count
	for i := range g.measures {
		g.measures[i] = mergeMeasureAccum(g.measures[i], other.measures[i])
	}
}

// encodeGroupKey serializes dims into buf as a sequence of
// length-prefixed segments (a varint byte count, then the segment's raw
// bytes), reusing buf's backing array across calls. This is unambiguous
// regardless of the bytes a dimension value contains — unlike joining
// values with a separator byte, which a string dimension could contain,
// silently merging two distinct tuples into one group.
func encodeGroupKey(buf []byte, dims []ingest.Value) []byte {
	buf = buf[:0]
	var numBuf [24]byte
	for _, v := range dims {
		if v.Type == ingest.TypeString {
			buf = binary.AppendUvarint(buf, uint64(len(v.Str)))
			buf = append(buf, v.Str...)
			continue
		}

		var seg []byte
		switch v.Type {
		case ingest.TypeInt64:
			seg = strconv.AppendInt(numBuf[:0], v.I64, 10)
		case ingest.TypeFloat64:
			seg = strconv.AppendFloat(numBuf[:0], v.F64, 'g', -1, 64)
		}
		buf = binary.AppendUvarint(buf, uint64(len(seg)))
		buf = append(buf, seg...)
	}
	return buf
}
