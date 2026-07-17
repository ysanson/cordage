package aggregate

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"

	"github.com/ysanson/cordage/internal/ingest"
)

// hllPrecision controls hyperLogLog's register count (2^hllPrecision) and
// therefore its memory/accuracy tradeoff: 14 -> 16384 registers, 16 KiB per
// sketch, ~0.81% standard error (1.04/sqrt(m)). Not yet configurable via
// the CLI grammar — a fixed default until a real workload asks for more.
const hllPrecision = 14

// hashValue hashes one column value's raw bytes via FNV-1a 64-bit —
// deterministic across calls and processes (unlike hash/maphash, which
// reseeds per process), so HyperLogLog's accuracy tests and benchmarks
// are reproducible. This is deliberately a separate hash primitive from
// the group-by key hashing in grouptable.go/aggregator.go (hash/maphash,
// used for hash-table bucketing) — a different job (uniform value
// hashing for register selection) with a different determinism need.
//
// FNV-1a's output is run through mix64 before returning: FNV mixes weakly
// when inputs differ only in a few low-order bytes (e.g. small/sequential
// integers, common in real columns), which otherwise skews which register
// each value lands in and biases the estimate. mix64 restores a proper
// bit avalanche without giving up determinism.
func hashValue(v ingest.Value) uint64 {
	h := fnv.New64a()
	switch v.Type {
	case ingest.TypeString:
		h.Write([]byte(v.Str))
	case ingest.TypeInt64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v.I64))
		h.Write(buf[:])
	case ingest.TypeFloat64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.F64))
		h.Write(buf[:])
	}
	return mix64(h.Sum64())
}

// mix64 is MurmurHash3's 64-bit finalizer (fmix64): a deterministic
// bit-avalanche step where every input bit affects every output bit with
// roughly 50% probability.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// hyperLogLog estimates the number of distinct values added to it, using
// O(2^hllPrecision) fixed memory regardless of how many (or how few)
// distinct values it actually sees.
type hyperLogLog struct {
	registers []uint8
}

func newHyperLogLog() *hyperLogLog {
	return &hyperLogLog{registers: make([]uint8, 1<<hllPrecision)}
}

// add folds one already-hashed value into the sketch: the top hllPrecision
// bits of hash select a register, and that register is set to the largest
// leading-zero-run-length-plus-one seen so far in the remaining bits.
func (h *hyperLogLog) add(hash uint64) {
	idx := hash >> (64 - hllPrecision)
	w := hash << hllPrecision
	rho := uint8(bits.LeadingZeros64(w)) + 1
	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

// estimate returns the estimated distinct-value count, using the standard
// HyperLogLog estimator with a linear-counting correction for the
// small-range case (Flajolet et al.). Because values are hashed to 64
// bits and only hllPrecision + (64-hllPrecision) bits are ever consumed,
// the large-range correction the original 32-bit-hash paper needs is
// unnecessary here — no realistic row count approaches that ceiling.
func (h *hyperLogLog) estimate() float64 {
	m := float64(len(h.registers))
	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += math.Exp2(-float64(r))
		if r == 0 {
			zeros++
		}
	}
	alpha := 0.7213 / (1 + 1.079/m)
	raw := alpha * m * m / sum

	if raw <= 2.5*m && zeros > 0 {
		return m * math.Log(m/float64(zeros))
	}
	return raw
}

// mergeHyperLogLog unions a and b (register-wise max) — exact and
// order-independent, so distinct-counting across N parallel sketches and
// merging them is identical to counting with one sketch over the same data.
func mergeHyperLogLog(a, b *hyperLogLog) *hyperLogLog {
	out := newHyperLogLog()
	for i := range out.registers {
		out.registers[i] = max(a.registers[i], b.registers[i])
	}
	return out
}
