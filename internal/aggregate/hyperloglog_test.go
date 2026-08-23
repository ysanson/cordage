package aggregate

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/ysanson/cordage/internal/ingest"
)

// TestHyperLogLogAccuracy compares hyperLogLog's estimate against an exact
// reference count-distinct (a plain map) over seeded-PRNG synthetic data,
// at cardinalities spanning the small-range linear-counting correction up
// to a range where the standard estimator alone should hold.
func TestHyperLogLogAccuracy(t *testing.T) {
	cardinalities := []int{1, 3, 5, 100, 10_000, 1_000_000}
	for _, card := range cardinalities {
		t.Run(fmt.Sprintf("cardinality=%d", card), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(42, uint64(card)))
			h := newHyperLogLog()
			exact := make(map[int64]struct{}, card)

			// Feed every value in [0, card) at least once, so the true
			// distinct count is exactly card regardless of sample size —
			// relying on random draws alone to cover the full domain
			// would need far more than a couple of samples per value
			// (coupon-collector effect), which isn't what this test is
			// measuring. Then add some repeat draws for realism (a real
			// column has duplicates); these touch only already-seen values.
			addValue := func(v int64) {
				exact[v] = struct{}{}
				h.add(hashValue(ingest.Value{Type: ingest.TypeInt64, I64: v}))
			}
			for i := range card {
				addValue(int64(i))
			}
			extraDraws := 200_000
			for range extraDraws {
				addValue(int64(rng.IntN(card)))
			}

			want := float64(len(exact))
			got := h.estimate()

			if card <= 5 {
				// Tiny cardinalities exercise the linear-counting
				// correction; relative error is a poor metric when want
				// is this small, so bound the absolute difference instead.
				if math.Abs(got-want) > 3 {
					t.Errorf("cardinality=%d: estimate=%v, want ~%v (abs diff %v)", card, got, want, math.Abs(got-want))
				}
				return
			}

			m := float64(1 << hllPrecision)
			bound := 4 * 1.04 / math.Sqrt(m) // generous multiple of the theoretical standard error
			relErr := math.Abs(got-want) / want
			if relErr > bound {
				t.Errorf("cardinality=%d: estimate=%v, want %v, relErr=%.4f exceeds bound %.4f", card, got, want, relErr, bound)
			}
		})
	}
}

// TestHyperLogLogMergeIsExact confirms register-wise max union is a true
// idempotent monoid: splitting a stream across two sketches and merging
// must produce byte-identical registers to feeding everything through one
// sketch, regardless of how the stream was split.
func TestHyperLogLogMergeIsExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	const n = 50_000
	const card = 5_000

	single := newHyperLogLog()
	part1 := newHyperLogLog()
	part2 := newHyperLogLog()
	for i := range n {
		v := int64(rng.IntN(card))
		hash := hashValue(ingest.Value{Type: ingest.TypeInt64, I64: v})
		single.add(hash)
		if i%2 == 0 {
			part1.add(hash)
		} else {
			part2.add(hash)
		}
	}

	merged := mergeHyperLogLog(part1, part2)
	for i := range merged.registers {
		if merged.registers[i] != single.registers[i] {
			t.Fatalf("register %d = %d, want %d (merge must be exact/order-independent)", i, merged.registers[i], single.registers[i])
		}
	}
}

func TestHashValueDeterministic(t *testing.T) {
	s := ingest.Value{Type: ingest.TypeString, Str: "hello"}
	if hashValue(s) != hashValue(s) {
		t.Fatal("hashValue must be deterministic across calls")
	}
	nan := ingest.Value{Type: ingest.TypeFloat64, F64: math.NaN()}
	if hashValue(nan) != hashValue(nan) {
		t.Fatal("hashValue must be deterministic for NaN too (unlike Go's runtime float64 hash)")
	}
}
