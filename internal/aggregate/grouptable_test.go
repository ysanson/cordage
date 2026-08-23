package aggregate

import (
	"fmt"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

func stringKeyGroup(s string) *groupAccum {
	return &groupAccum{key: []ingest.Value{{Type: ingest.TypeString, Str: s}}}
}

func TestGroupTableGrowthPreservesEntries(t *testing.T) {
	table := newGroupTable()
	const n = 500
	for i := range n {
		s := fmt.Sprintf("key-%d", i)
		hash := uint64(i) * 2654435761 // arbitrary distinct-ish hashes; growth must still work regardless of distribution
		if _, ok := table.findByString(hash, s); ok {
			t.Fatalf("key %q found before insert", s)
		}
		table.insertNew(hash, stringKeyGroup(s))
	}
	if table.size != n {
		t.Fatalf("table.size = %d, want %d", table.size, n)
	}
	for i := range n {
		s := fmt.Sprintf("key-%d", i)
		hash := uint64(i) * 2654435761
		g, ok := table.findByString(hash, s)
		if !ok {
			t.Fatalf("key %q not found after growth", s)
		}
		if g.key[0].Str != s {
			t.Fatalf("key %q resolved to wrong entry %+v", s, g.key)
		}
	}
	seen := 0
	table.each(func(*groupAccum) { seen++ })
	if seen != n {
		t.Fatalf("each visited %d entries, want %d", seen, n)
	}
}

// TestGroupTableNoCollisionUnderForcedHashCollision directly proves the
// hash-then-verify design: two distinct keys sharing the exact same hash
// value must never be treated as equal. This is the load-bearing
// correctness property the whole grouping-engine replacement depends on.
func TestGroupTableNoCollisionUnderForcedHashCollision(t *testing.T) {
	table := newGroupTable()
	const collidingHash = uint64(42)

	table.insertNew(collidingHash, stringKeyGroup("alpha"))
	if _, ok := table.findByString(collidingHash, "beta"); ok {
		t.Fatal("findByString found \"beta\" using \"alpha\"'s hash — distinct keys must not collide")
	}
	table.insertNew(collidingHash, stringKeyGroup("beta"))

	gotAlpha, ok := table.findByString(collidingHash, "alpha")
	if !ok || gotAlpha.key[0].Str != "alpha" {
		t.Fatalf("findByString(%q) = %+v, %v", "alpha", gotAlpha, ok)
	}
	gotBeta, ok := table.findByString(collidingHash, "beta")
	if !ok || gotBeta.key[0].Str != "beta" {
		t.Fatalf("findByString(%q) = %+v, %v", "beta", gotBeta, ok)
	}
	if table.size != 2 {
		t.Fatalf("table.size = %d, want 2", table.size)
	}
}

func TestDimsEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []ingest.Value
		want bool
	}{
		{"equal strings", []ingest.Value{{Type: ingest.TypeString, Str: "x"}}, []ingest.Value{{Type: ingest.TypeString, Str: "x"}}, true},
		{"different strings", []ingest.Value{{Type: ingest.TypeString, Str: "x"}}, []ingest.Value{{Type: ingest.TypeString, Str: "y"}}, false},
		{"different lengths", []ingest.Value{{Type: ingest.TypeString, Str: "x"}}, nil, false},
		{"different types", []ingest.Value{{Type: ingest.TypeInt64, I64: 1}}, []ingest.Value{{Type: ingest.TypeFloat64, F64: 1}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := dimsEqual(c.a, c.b); got != c.want {
				t.Errorf("dimsEqual(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}
