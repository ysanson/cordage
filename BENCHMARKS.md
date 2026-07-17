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
