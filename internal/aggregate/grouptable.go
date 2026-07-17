package aggregate

import (
	"math"

	"github.com/ysanson/cordage/internal/ingest"
)

// groupTable is an open-addressing hash table mapping a group-by key to its
// groupAccum. Unlike a map[string]*groupAccum, the hash never doubles as the
// key itself: a probe hit is only ever confirmed by directly comparing the
// stored groupAccum.key against the incoming dimension value(s). That makes
// hash quality (and any hypothetical collision) purely a performance
// concern — it can lengthen a probe chain, but it can never merge two
// distinct group-by tuples together.
type groupTable struct {
	hashes []uint64
	slots  []*groupAccum
	full   []bool
	size   int
}

const groupTableMaxLoadFactor = 0.7

func newGroupTable() groupTable {
	const initialCap = 16
	return groupTable{
		hashes: make([]uint64, initialCap),
		slots:  make([]*groupAccum, initialCap),
		full:   make([]bool, initialCap),
	}
}

func (t *groupTable) mask() uint64 {
	return uint64(len(t.slots) - 1)
}

// grow doubles capacity and reinserts every occupied slot using its cached
// hash — no rehashing of the underlying key data is needed.
func (t *groupTable) grow() {
	oldHashes, oldSlots, oldFull := t.hashes, t.slots, t.full
	newCap := len(t.slots) * 2
	t.hashes = make([]uint64, newCap)
	t.slots = make([]*groupAccum, newCap)
	t.full = make([]bool, newCap)
	mask := t.mask()

	for i, full := range oldFull {
		if !full {
			continue
		}
		h := oldHashes[i]
		idx := h & mask
		for t.full[idx] {
			idx = (idx + 1) & mask
		}
		t.hashes[idx] = h
		t.slots[idx] = oldSlots[i]
		t.full[idx] = true
	}
}

// insertNew places g under hash. The caller must already know no equal key
// is present (i.e. the matching findBy* call returned ok == false, or g's
// key comes from another table entirely, as in Merge).
func (t *groupTable) insertNew(hash uint64, g *groupAccum) {
	if float64(t.size+1) > groupTableMaxLoadFactor*float64(len(t.slots)) {
		t.grow()
	}
	mask := t.mask()
	idx := hash & mask
	for t.full[idx] {
		idx = (idx + 1) & mask
	}
	t.hashes[idx] = hash
	t.slots[idx] = g
	t.full[idx] = true
	t.size++
}

func (t *groupTable) findByString(hash uint64, s string) (*groupAccum, bool) {
	mask := t.mask()
	for idx := hash & mask; t.full[idx]; idx = (idx + 1) & mask {
		if t.hashes[idx] == hash {
			if g := t.slots[idx]; len(g.key) == 1 && g.key[0].Type == ingest.TypeString && g.key[0].Str == s {
				return g, true
			}
		}
	}
	return nil, false
}

func (t *groupTable) findByI64(hash uint64, v int64) (*groupAccum, bool) {
	mask := t.mask()
	for idx := hash & mask; t.full[idx]; idx = (idx + 1) & mask {
		if t.hashes[idx] == hash {
			if g := t.slots[idx]; len(g.key) == 1 && g.key[0].Type == ingest.TypeInt64 && g.key[0].I64 == v {
				return g, true
			}
		}
	}
	return nil, false
}

func (t *groupTable) findByF64(hash uint64, v float64) (*groupAccum, bool) {
	bits := math.Float64bits(v)
	mask := t.mask()
	for idx := hash & mask; t.full[idx]; idx = (idx + 1) & mask {
		if t.hashes[idx] == hash {
			if g := t.slots[idx]; len(g.key) == 1 && g.key[0].Type == ingest.TypeFloat64 && math.Float64bits(g.key[0].F64) == bits {
				return g, true
			}
		}
	}
	return nil, false
}

func (t *groupTable) findByDims(hash uint64, dims []ingest.Value) (*groupAccum, bool) {
	mask := t.mask()
	for idx := hash & mask; t.full[idx]; idx = (idx + 1) & mask {
		if t.hashes[idx] == hash && dimsEqual(t.slots[idx].key, dims) {
			return t.slots[idx], true
		}
	}
	return nil, false
}

// dimsEqual compares two dimension-value tuples by native value — bit
// pattern for float64, so distinct NaN bit patterns are distinct values and
// identical bit patterns (including NaN) are equal, giving every float64 a
// well-defined identity instead of relying on IEEE-754 == (where NaN != NaN).
func dimsEqual(a, b []ingest.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type {
			return false
		}
		switch a[i].Type {
		case ingest.TypeString:
			if a[i].Str != b[i].Str {
				return false
			}
		case ingest.TypeInt64:
			if a[i].I64 != b[i].I64 {
				return false
			}
		case ingest.TypeFloat64:
			if math.Float64bits(a[i].F64) != math.Float64bits(b[i].F64) {
				return false
			}
		}
	}
	return true
}

// each calls fn once per occupied slot. Iteration order is unspecified —
// callers that need deterministic output (Result) must sort separately.
func (t *groupTable) each(fn func(*groupAccum)) {
	for i, full := range t.full {
		if full {
			fn(t.slots[i])
		}
	}
}
