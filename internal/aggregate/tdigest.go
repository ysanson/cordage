package aggregate

import (
	"math"
	"sort"
)

const (
	tdigestDefaultCompression = 100  // delta: bounds centroid count to roughly this order
	tdigestBufferCap          = 1024 // values buffered before a compress() pass
)

// centroid is one cluster in a t-digest: a mean and the total weight
// (original point count) it represents.
type centroid struct {
	mean   float64
	weight float64
}

// tdigest estimates quantiles over a stream of float64 values using
// Dunning's t-digest: a sorted list of centroids, each a weighted cluster
// of nearby values, with more (smaller) clusters near the tails than in
// the middle — giving much better tail accuracy per byte of memory than a
// fixed-width histogram.
//
// Values are buffered rather than clustered one at a time (Add just
// appends and updates min/max); once the buffer fills, compress folds it
// and the existing centroids together in one pass. Sorting within each
// flush eliminates order-sensitivity within that batch; residual
// path-dependency across flush boundaries is a documented, accepted
// property of streaming digests generally, not a bug here.
type tdigest struct {
	centroids   []centroid // sorted by mean
	buffer      []float64  // values not yet folded into centroids
	compression float64    // delta
	min, max    float64
	haveMinMax  bool
}

func newTDigest() *tdigest {
	return &tdigest{compression: tdigestDefaultCompression}
}

// Add folds one value into the digest. NaN is deliberately skipped rather
// than poisoning the digest: unlike Sum/Min/Max (where a NaN visibly
// poisons the result), a NaN mixed into a mean-sorted centroid list would
// silently corrupt ordering and produce a plausible-looking but wrong
// quantile — a worse failure mode, so this is an intentional exception to
// this package's usual "NaN poisons regardless of order" convention.
func (t *tdigest) Add(v float64) {
	if math.IsNaN(v) {
		return
	}
	if !t.haveMinMax {
		t.min, t.max = v, v
		t.haveMinMax = true
	} else {
		t.min = math.Min(t.min, v)
		t.max = math.Max(t.max, v)
	}
	t.buffer = append(t.buffer, v)
	if len(t.buffer) >= tdigestBufferCap {
		t.compress()
	}
}

// flushPending compresses any buffered values, a no-op if the buffer is
// already empty.
func (t *tdigest) flushPending() {
	if len(t.buffer) > 0 {
		t.compress()
	}
}

// compress merges t.centroids and t.buffer into a new, sorted centroid
// list bounded to roughly t.compression clusters, via a scale-function-
// bounded greedy clustering pass: existing centroids and buffered raw
// values (weight 1) are treated identically as weighted points, sorted by
// mean, then folded left to right into a running centroid as long as
// doing so keeps k(q1) - k(q0) <= 1, where k is the t-digest scale
// function and q0/q1 are the total weight fraction consumed before/after
// including the candidate point. This is also what Merge uses (after
// concatenating both sides' centroids) — there is no separate merge
// algorithm, since compress already treats a centroid and a raw value the
// same way.
func (t *tdigest) compress() {
	points := make([]centroid, 0, len(t.centroids)+len(t.buffer))
	points = append(points, t.centroids...)
	for _, v := range t.buffer {
		points = append(points, centroid{mean: v, weight: 1})
	}
	t.buffer = t.buffer[:0]

	if len(points) == 0 {
		t.centroids = points
		return
	}
	sort.Slice(points, func(i, j int) bool { return points[i].mean < points[j].mean })

	total := 0.0
	for _, p := range points {
		total += p.weight
	}

	newCentroids := make([]centroid, 0, len(points))
	cur := points[0]
	cumulative := cur.weight
	q0Start := 0.0 // q-position where cur started accumulating, held fixed while cur grows
	for _, p := range points[1:] {
		q1 := (cumulative + p.weight) / total
		// Bounded against q0Start (fixed for cur's whole lifetime), not
		// the position just before p: checking against a baseline that
		// drifts forward on every merge would only bound each individual
		// increment, letting cur's total accumulated span grow without
		// limit as long as each step looked small on its own.
		if tdigestScale(q1, t.compression)-tdigestScale(q0Start, t.compression) <= 1 {
			newWeight := cur.weight + p.weight
			cur.mean = (cur.mean*cur.weight + p.mean*p.weight) / newWeight
			cur.weight = newWeight
			cumulative += p.weight
		} else {
			newCentroids = append(newCentroids, cur)
			q0Start = cumulative / total
			cur = p
			cumulative += p.weight
		}
	}
	t.centroids = append(newCentroids, cur)
}

// tdigestScale is t-digest's scale function k(q, delta) = (delta/2*pi) *
// asin(2q-1), which maps a weight fraction (0-1) to a "size" coordinate
// where equal steps correspond to a bounded number of points — bunching
// centroids tightly near q=0/q=1 (the tails) and loosely near q=0.5 (the
// middle), which is the whole reason t-digest resolves tail quantiles far
// more precisely than a fixed-width histogram of the same size.
func tdigestScale(q, delta float64) float64 {
	if q < 0 {
		q = 0
	} else if q > 1 {
		q = 1
	}
	return (delta / (2 * math.Pi)) * math.Asin(2*q-1)
}

// Quantile returns the estimated value at rank q (0-1). Guards len<=1
// explicitly: a single centroid has no neighbor to interpolate against
// (and would otherwise divide by zero below), so its mean is returned for
// any q.
func (t *tdigest) Quantile(q float64) float64 {
	t.flushPending()
	switch len(t.centroids) {
	case 0:
		return math.NaN()
	case 1:
		return t.centroids[0].mean
	}

	total := 0.0
	for _, c := range t.centroids {
		total += c.weight
	}
	targetRank := q * total

	// Interpolate against min/max at the edges (materially better tail
	// accuracy than treating the first/last centroid's own mean as the
	// boundary), and between adjacent centroid means everywhere else.
	first := t.centroids[0]
	if targetRank <= first.weight/2 {
		frac := targetRank / (first.weight / 2)
		return t.min + frac*(first.mean-t.min)
	}

	cum := first.weight / 2
	for i := 0; i < len(t.centroids)-1; i++ {
		c, next := t.centroids[i], t.centroids[i+1]
		halfSum := (c.weight + next.weight) / 2
		if targetRank <= cum+halfSum {
			frac := (targetRank - cum) / halfSum
			return c.mean + frac*(next.mean-c.mean)
		}
		cum += halfSum
	}

	last := t.centroids[len(t.centroids)-1]
	remaining := total - cum
	if remaining <= 0 {
		return last.mean
	}
	frac := (targetRank - cum) / remaining
	return last.mean + frac*(t.max-last.mean)
}

// Merge folds other into t. Unlike hyperLogLog's exact register-max
// union, this is not perfectly order-independent (a well-documented,
// expected t-digest property) — callers comparing a merged result against
// a single-digest baseline should use a tolerance, not exact equality.
func (t *tdigest) Merge(other *tdigest) {
	t.flushPending()
	other.flushPending()

	t.centroids = append(t.centroids, other.centroids...)
	if other.haveMinMax {
		if !t.haveMinMax {
			t.min, t.max = other.min, other.max
			t.haveMinMax = true
		} else {
			t.min = math.Min(t.min, other.min)
			t.max = math.Max(t.max, other.max)
		}
	}
	t.compress()
}
