package aggregate

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// percentileOf is a linear-interpolation exact reference (the common
// "type 7" quantile definition), used to check tdigest.Quantile's
// accuracy against ground truth over fully-sorted synthetic data.
func percentileOf(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func TestTDigestAccuracyNormal(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const n = 100_000
	values := make([]float64, n)
	td := newTDigest()
	for i := range values {
		v := rng.NormFloat64()*10 + 50
		values[i] = v
		td.Add(v)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	// Looser tolerance at more extreme quantiles, matching t-digest's
	// known accuracy profile (tighter in the tails relative to the data's
	// spread, but the tails are still sparser in absolute sample count).
	cases := []struct {
		q         float64
		tolerance float64
	}{
		{0.5, 0.5},
		{0.9, 0.5},
		{0.99, 1.0},
		{0.999, 3.0},
	}
	for _, c := range cases {
		want := percentileOf(sorted, c.q)
		got := td.Quantile(c.q)
		if math.Abs(got-want) > c.tolerance {
			t.Errorf("q=%v: got %v, want %v (diff %v exceeds tolerance %v)", c.q, got, want, math.Abs(got-want), c.tolerance)
		}
	}
}

func TestTDigestAccuracySkewed(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const n = 100_000
	values := make([]float64, n)
	td := newTDigest()
	for i := range values {
		v := rng.ExpFloat64() * 20
		values[i] = v
		td.Add(v)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	cases := []struct {
		q         float64
		tolerance float64 // relative, since an exponential distribution's scale varies a lot across quantiles
	}{
		{0.5, 0.1},
		{0.9, 0.1},
		{0.99, 0.15},
		{0.999, 0.25},
	}
	for _, c := range cases {
		want := percentileOf(sorted, c.q)
		got := td.Quantile(c.q)
		relErr := math.Abs(got-want) / want
		if relErr > c.tolerance {
			t.Errorf("q=%v: got %v, want %v, relErr=%.4f exceeds tolerance %.4f", c.q, got, want, relErr, c.tolerance)
		}
	}
}

func TestTDigestSinglePoint(t *testing.T) {
	td := newTDigest()
	td.Add(42.0)
	for _, q := range []float64{0, 0.5, 0.99, 1} {
		if got := td.Quantile(q); got != 42.0 {
			t.Errorf("q=%v: got %v, want 42.0", q, got)
		}
	}
}

func TestTDigestTwoPoints(t *testing.T) {
	td := newTDigest()
	td.Add(10.0)
	td.Add(20.0)
	if mid := td.Quantile(0.5); mid < 10.0 || mid > 20.0 {
		t.Errorf("q=0.5 = %v, want a value within [10, 20]", mid)
	}
}

// TestTDigestSkipsNaN asserts NaN is a true no-op: a digest fed a stream
// with NaNs interspersed must be byte-identical (not just close) to one
// fed the same stream with NaNs removed, since Add returns before
// touching buffer/min/max for NaN input.
func TestTDigestSkipsNaN(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	withNaN := newTDigest()
	without := newTDigest()
	const n = 5000
	for i := range n {
		v := rng.NormFloat64()
		without.Add(v)
		withNaN.Add(v)
		if i%97 == 0 {
			withNaN.Add(math.NaN())
		}
	}
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99} {
		a, b := withNaN.Quantile(q), without.Quantile(q)
		if a != b {
			t.Errorf("q=%v: with-NaN digest = %v, without-NaN digest = %v, want identical", q, a, b)
		}
	}
}

// TestTDigestMergeWithinTolerance: unlike hyperLogLog's exact merge,
// t-digest merge order isn't perfectly order-independent (a documented,
// expected property) — this asserts "close," not "identical."
func TestTDigestMergeWithinTolerance(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	const n = 100_000
	single := newTDigest()
	part1 := newTDigest()
	part2 := newTDigest()
	for i := range n {
		v := rng.NormFloat64()*5 + 100
		single.Add(v)
		if i%2 == 0 {
			part1.Add(v)
		} else {
			part2.Add(v)
		}
	}
	part1.Merge(part2)
	for _, q := range []float64{0.5, 0.9, 0.99} {
		a, b := part1.Quantile(q), single.Quantile(q)
		if math.Abs(a-b) > 1.0 {
			t.Errorf("q=%v: merged=%v, single=%v, diff %v exceeds tolerance", q, a, b, math.Abs(a-b))
		}
	}
}
