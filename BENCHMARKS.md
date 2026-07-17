# Benchmarks

Numbers here are the M0 baseline: single-goroutine, correctness-first ingestion and
aggregation (see [`DesignDocs/cordage-project-plan.md`](DesignDocs/cordage-project-plan.md)).
Every later milestone (M1's concurrency, M2's algorithmic changes) should be compared
against this row, not against vibes — this file grows one row per milestone.

## Environment

| | |
|---|---|
| CPU | Apple M4 Pro (12 logical cores) |
| OS | macOS 26.5.2 (Darwin 25.5.0, arm64) |
| Go | go1.26.5 darwin/arm64 |
| Binary | `go build -o cordage ./cmd/cordage` (no special build flags) |

## Dataset

A 1BRC-style `station;temperature` subset, generated locally (not checked into git —
see `.gitignore`) rather than depending on the ~13 GB original file:

```
go run ./cmd/gen1brc --rows 10000000 --out benchmarks/data/measurements.csv
```

- 10,000,000 rows, 104 stations (a curated real-city subset — see `cmd/gen1brc/stations.go`),
  each station's temperature drawn from `N(stationMean, 10)`, one decimal place.
- Output: 127.0 MiB, semicolon-delimited, no header.
- Schema used for ingestion: [`benchmarks/schema.json`](benchmarks/schema.json)
  (`station` string dimension, `temperature` float64 measure).
- Deterministic: fixed PRNG seed (`--seed 42`, the default), so re-running the generator
  reproduces the same file byte-for-byte.

## Benchmark commands

Wall time and peak RSS come from `/usr/bin/time -l` (macOS) wrapping the CLI, since
`rows/sec` alone doesn't capture memory — `cordage`'s own stdout line reports
ingestion-internal elapsed time and rows/sec for cross-checking.

Ingestion only:
```
/usr/bin/time -l ./cordage ingest --file benchmarks/data/measurements.csv --schema benchmarks/schema.json
```

Full pipeline — the classic 1BRC query (min/mean/max temperature per station):
```
/usr/bin/time -l ./cordage run \
  --file benchmarks/data/measurements.csv \
  --schema benchmarks/schema.json \
  --group-by station \
  --measure temperature:min,avg,max
```

## Results (M0 baseline, single goroutine)

Each row is the mean of 3 consecutive runs against the same generated file.

| Stage | Wall time | Rows/sec | Peak RSS |
|---|---|---|---|
| Ingest only | 0.613 s | 16,197,834 | 10.8 MiB |
| Ingest + Aggregate (`run`, min/avg/max GROUP BY station) | 0.623 s | 16,047,577 | 11.2 MiB |

Individual runs:

| Run | Ingest-only wall time | Ingest-only rows/sec | Full-pipeline wall time | Full-pipeline rows/sec | Full-pipeline peak RSS |
|---|---|---|---|---|---|
| 1 | 0.62 s | 16,069,548 | 0.63 s | 15,887,891 | 11,599,872 B |
| 2 | 0.61 s | 16,308,525 | 0.62 s | 16,186,774 | 11,616,256 B |
| 3 | 0.61 s | 16,215,428 | 0.62 s | 16,068,065 | 12,124,160 B |

**Takeaway:** aggregation (104 groups, 3 measure functions) adds no measurable overhead
over ingestion alone at this dataset size — CSV parsing (field-splitting, `strconv`
float parsing) is the bottleneck, not group-by/aggregate bookkeeping. Peak RSS stays
around 10-11 MiB against a 127 MiB input, confirming batches are actually streamed and
released (not accumulated) rather than the whole file being held resident.

## Reproducing

```
go build -o cordage ./cmd/cordage
go run ./cmd/gen1brc --rows 10000000 --out benchmarks/data/measurements.csv
/usr/bin/time -l ./cordage run --file benchmarks/data/measurements.csv --schema benchmarks/schema.json --group-by station --measure temperature:min,avg,max
```

## `go test -bench` (hot-loop regression signal)

The CLI+`/usr/bin/time -l` numbers above are the credible end-to-end baseline (they're
the only source of peak RSS), but they're clunky for tracking regressions commit-to-commit.
For that, `internal/ingest` and `internal/aggregate` carry self-contained
`testing.B` benchmarks — no external dataset required, so they run with a plain
`go test -bench`:

- `BenchmarkIngestFile` (`internal/ingest`) — sequential, single-chunk file ingestion (the M0 path).
- `BenchmarkAggregatorAdd` (`internal/aggregate`) — just `Aggregator.Add`'s hot loop over an
  in-memory batch, isolating aggregation cost from I/O.
- `BenchmarkPipeline` (`internal/aggregate`) — the full `Ingest`→`Aggregate` path over a
  synthetic on-disk file, the closest `go test` analog to the `run` numbers above.

```
go test ./internal/ingest/... -run '^$' -bench BenchmarkIngestFile -benchmem
go test ./internal/aggregate/... -run '^$' -bench . -benchmem
```

M0 baseline (Apple M4 Pro, same environment as above):

| Benchmark | ns/op | rows/op | Rows/sec | B/op | allocs/op |
|---|---|---|---|---|---|
| `BenchmarkIngestFile` | 29,036,942 | 500,000 | ~17.2M | 5,206,230 | 500,058 |
| `BenchmarkAggregatorAdd` | 186,094 | 10,000 | ~53.7M | 2,024 | 41 |
| `BenchmarkPipeline` | 29,570,798 | 500,000 | ~16.9M | 5,099,272 | 500,244 |

`BenchmarkAggregatorAdd`'s allocs/op (41, independent of row count) versus the other
two's (~1 per row) makes the allocation source obvious: it's field-to-string conversion
during CSV parsing (one allocation per string field), not aggregation bookkeeping — a
concrete target if M2's algorithmic work looks for allocation wins.

**For M1/M2 comparisons**: run `go test ./... -run '^$' -bench . -benchmem -count=10 > old.txt`
before a change and the same command (`> new.txt`) after, then
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) `old.txt new.txt` for a
statistically-grounded speedup/regression report — this is the credible way to back the
plan's "documented speedup vs M0" exit criteria, rather than eyeballing single runs.
`BenchmarkIngestFile`, `BenchmarkAggregatorAdd`, and `BenchmarkPipeline` themselves are
unmodified by M1 (confirmed below) — they remain the fixed "M0" comparison point.

---

## M1 — Concurrency within a single node

M0's `run` command already supported `--chunks N` (parallel ingestion via
`internal/ingest`'s existing chunked-goroutine design), but its aggregation-side
consumer was a single goroutine calling `Aggregator.Add` sequentially — so chunked
ingestion alone could only go as fast as one `Add` loop could drain it.
`Aggregator` is explicitly not safe for concurrent `Add`, so M1 adds a real worker
pool: `internal/aggregate.RunParallel` (`internal/aggregate/parallel.go`) runs M
goroutines, each with its own private `Aggregator` draining the same shared
ingestion channel, merged into one result via the existing `Merge` at the end.
`cmd/cordage run` gained an independent `--workers` flag (default: match `--chunks`).

Same environment and dataset as the M0 section above (10M rows, 104 stations,
`benchmarks/data/measurements.csv`).

### Step 1: what chunked ingestion alone was worth (before adding the worker pool)

With the M0-era single-goroutine consumer, `--chunks N` sped up *ingestion* a lot
but `run`'s end-to-end throughput plateaued quickly — direct evidence the
aggregation-side consumer was the bottleneck once `chunks` grew:

| Chunks | Ingest-only rows/sec | `run` (single consumer) rows/sec | `run` peak RSS |
|---|---|---|---|
| 1 | 15,823,083 | 15,819,602 | 11.26 MiB |
| 2 | 29,101,525 | 22,328,409 | 14.60 MiB |
| 4 | 38,511,997 | 23,134,308 | 19.43 MiB |
| 6 | 43,861,320 | 24,492,017 | 24.31 MiB |
| 8 | 47,478,393 | 25,111,228 | 28.39 MiB |
| 10 | 52,852,422 | 25,714,230 | 33.78 MiB |
| 12 | 53,595,389 | 26,187,112 | 38.49 MiB |
| 16 | 55,410,451 | 26,537,610 | 47.21 MiB |

Ingestion alone scales to 3.5x M0; `run` tops out at 1.65x and barely moves past
`chunks=2` — exactly as predicted by the per-goroutine ratio already on record
(`Aggregator.Add` ≈53.7M rows/sec vs `RowParser` ≈17.2M rows/sec): a single
consumer keeps up with roughly 2-3 parallel producers before it becomes the
constraint.

### Step 2: matched sweep with the worker pool (`--chunks N --workers N`)

Each row is the mean of 3 runs, same commands as the M0 section plus `--workers`:

| N | Wall time | Rows/sec | Speedup vs M0 | Peak RSS |
|---|---|---|---|---|
| 1 | 0.657 s | 15,155,850 | 0.94x | 11.66 MiB |
| 2 | 0.407 s | 24,643,248 | 1.54x | 15.56 MiB |
| 4 | 0.267 s | 37,281,328 | 2.32x | 22.99 MiB |
| 6 | 0.277 s | 36,289,643 | 2.26x | 28.14 MiB |
| 8 | 0.257 s | 38,812,119 | 2.42x | 33.54 MiB |
| 10 | 0.227 s | 44,100,638 | 2.75x | 39.43 MiB |
| 12 | 0.213 s | 46,155,335 | 2.88x | 44.54 MiB |
| 16 | 0.203 s | 48,671,597 | **3.03x** | 54.43 MiB |

(Speedup vs the M0 baseline's 16,047,577 rows/sec `run` figure.) N=16 nearly
doubles the old single-consumer design's ceiling (26.5M rows/sec, above) at the
same chunk count — the worker pool is what turns available ingestion parallelism
into end-to-end throughput. N=6 dipping slightly below N=4 is measurement noise
(3-run means on a shared laptop), not a real regression — visible run-to-run
variance at this scale, not a pattern.

### Isolating aggregation-side scaling (fixed `--chunks 12`, vary `--workers`)

To check whether aggregation was ever the bottleneck *by itself* at this dataset's
scale, holding ingestion parallelism fixed at 12 (near the ingest-side ceiling
above) and varying only `--workers`:

| Workers | Wall time | Rows/sec |
|---|---|---|
| 1 | 0.373 s | 26,707,933 |
| 2 | 0.240 s | 42,161,967 |
| 4 | 0.227 s | 44,109,287 |
| 8 | 0.233 s | 42,505,383 |

`workers=1` here (26.7M rows/sec) matches the old single-consumer design's
`chunks=12` figure (26.2M, Step 1 table) almost exactly — a good sanity check.
Going to 2 workers nearly doubles throughput (removing the single-consumer
bottleneck); 2→4 is a small further gain; 4→8 is flat-to-slightly-worse (12
producer goroutines + 8 consumer goroutines oversubscribes the 12 logical cores,
with no compensating benefit since aggregation was never the constraint here).
**2-4 workers is enough to keep a fully-loaded 12-chunk ingestion pipeline fed** —
consistent with the ~3x `Add`-vs-`RowParser` per-goroutine speed ratio.

### `go test -bench`: aggregation-only vs I/O-only scaling curves

Two new self-contained benchmarks isolate each side of the pipeline without
depending on the external dataset: `BenchmarkRunParallel` (`internal/aggregate`)
feeds pre-built in-memory batches through `RunParallel` — no I/O, pure
fan-out/`Add`/`Merge` cost; `BenchmarkIngestFileParallel` (`internal/ingest`)
chunks a synthetic on-disk file with a single non-aggregating consumer — pure
parse+I/O cost. Both sweep the same worker/chunk counts, 500,000 rows/op:

| N | `BenchmarkRunParallel` (aggregation only) | `BenchmarkIngestFileParallel` (parse+I/O only) |
|---|---|---|
| 1 | ~51.0M rows/sec | ~17.0M rows/sec |
| 2 | ~102.0M rows/sec | ~30.3M rows/sec |
| 4 | ~175.4M rows/sec | ~39.4M rows/sec |
| 6 | ~250.0M rows/sec | ~48.1M rows/sec |
| 8 | ~301.2M rows/sec | ~53.4M rows/sec |
| 10 | ~301.2M rows/sec | ~55.9M rows/sec |
| 12 | ~306.7M rows/sec | ~58.0M rows/sec |
| 16 | ~322.6M rows/sec | ~61.4M rows/sec |

```
go test ./internal/aggregate/... -run '^$' -bench BenchmarkRunParallel -benchmem -count=5
go test ./internal/ingest/... -run '^$' -bench BenchmarkIngestFileParallel -benchmem -count=5
```

Confirmed unchanged (no regression from M1's changes — these are the fixed "M0"
comparison point): `BenchmarkIngestFile` 28.9-29.2M ns/op, `BenchmarkAggregatorAdd`
186.0-186.4K ns/op, and `BenchmarkPipeline` 29.2-29.4M ns/op all match the M0
table above within run-to-run noise.

### Where the ceiling is: CPU-bound parsing, not aggregation, not I/O

Isolated aggregation (independent per-worker maps, no shared state, no I/O) scales
near-linearly and reaches ~300M+ rows/sec by 8 workers — roughly **6x** the
isolated parse+I/O ceiling (~53M rows/sec) at the same worker count, up from ~3x
at a single goroutine. In other words, aggregation's *relative* headroom over
parsing gets *larger*, not smaller, as concurrency increases: aggregation
parallelizes better because each worker's map is fully independent, while parsing
per chunk pays the same fixed per-field cost (field-splitting, `strconv` float
parsing, one string allocation per field — the exact allocation source already
flagged in the M0 section) regardless of how many other chunks run alongside it.

The parse+I/O curve itself (both the isolated benchmark and the real-dataset CLI
sweep) shows classic diminishing returns rather than a sharp cliff: the biggest
gains land between 1 and 4-6 chunks (roughly 3x), with each further doubling
buying progressively less, flattening out around the machine's 12 logical cores
(10, 12, and 16 chunks/workers are all within ~10% of each other). The 127 MiB
dataset comfortably fits in the OS page cache after the first read, so this isn't
disk I/O wait — it's CPU-bound parsing work (field-splitting + float parsing)
that scales with available *logical* parallelism and runs out of real headroom
once chunk/worker count approaches the core count, with 16 (oversubscribed beyond
12 logical cores) adding only marginal further gain.

**Conclusion:** at this dataset's scale, aggregation was never the constraint —
CSV parsing is, both before and after adding the worker pool. The worker pool's
value in M1 wasn't "aggregation was slow," it was "a single aggregation consumer
couldn't drain more than ~2-3 parallel ingestion chunks," which is exactly what
Step 1's plateau showed and Step 2's matched sweep fixed.

### Caveat: `--chunks 1` with `--workers > 1` gives no benefit

With only one ingestion goroutine, there is only ever one batch in flight on the
shared channel at a time — extra aggregation workers have nothing to fan out
against. `--workers` only pays off once `--chunks` is high enough to actually
keep multiple consumers fed; don't read a flat `--chunks 1` sweep across
`--workers` as "the worker pool doesn't scale."

### Reproducing

```
go build -o cordage ./cmd/cordage
go run ./cmd/gen1brc --rows 10000000 --out benchmarks/data/measurements.csv
for w in 1 2 4 6 8 10 12 16; do
  /usr/bin/time -l ./cordage run --file benchmarks/data/measurements.csv \
    --schema benchmarks/schema.json --chunks $w --workers $w \
    --group-by station --measure temperature:min,avg,max
done
```
