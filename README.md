# Cordage

A distributed, in-memory analytics engine written in Go. Ingests large
tabular datasets, shards work across concurrent workers, aggregates
results, and serves them through a simple query interface — deployable as
a single binary, locally or on Kubernetes.

**Showcase goals:** algorithms (hashing, aggregation, approximate
structures), cloud (gRPC, Kubernetes, horizontal scaling), data processing
(I/O throughput, encoding formats, benchmarking discipline). See
[`DesignDocs/cordage-project-plan.md`](DesignDocs/cordage-project-plan.md)
for the full milestone-by-milestone plan this was built against.

## What it is / isn't

**Is:**
- A single-purpose aggregation engine: `SELECT col, AGG(col) FROM data WHERE ... GROUP BY col`
- Distributed across worker processes via gRPC; a coordinator does fan-out/fan-in
- One binary, five subcommands (`ingest`, `run`, `worker`, `coordinator`, `query`) —
  no separate client/server builds

**Isn't** (intentionally out of scope, deferred to a follow-up LSM-tree KV
store project):
- Joins, transactions, durability/persistence, indexes
- A real SQL parser — `query` speaks a small fixed grammar (below), not SQL

## Architecture

```
                     ┌─────────────┐
        query ──────▶│ Coordinator │
                     └──────┬──────┘
                            │ gRPC (shard assignment)
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         ┌────────┐   ┌────────┐   ┌────────┐
         │Worker 1│   │Worker 2│   │Worker N│
         │ shard  │   │ shard  │   │ shard  │
         └───┬────┘   └───┬────┘   └───┬────┘
             │            │            │
             └──── partial aggregates ─┘
                            │
                     ┌──────▼──────┐
                     │   Merge     │──────▶ result
                     └─────────────┘
```

**Pipeline stages:** Ingest → Parse/Transform → Aggregate (local, per
worker) → Merge (coordinator) → Respond.

A `cordage coordinator` process splits one input file into newline-aligned
byte-range shards (`internal/distribute.PlanShards`) and dispatches one
shard per `cordage worker` process over gRPC (`worker.proto`'s `RunShard`
RPC). Each worker ingests and aggregates only its own byte range, and
returns raw, mergeable accumulator state (`GroupState` — sums, counts,
min/max, HyperLogLog/t-digest sketches), never row data. The coordinator
folds every shard's state into one final `Aggregator` via `MergeState` —
the same merge primitive M1 already used to fold per-goroutine partial
results within a single process, now folding per-process results across a
network instead.

In `-listen` mode, the coordinator additionally hosts a second gRPC
service (`coordinator.proto`) that answers `cordage query` requests
against an already-configured dataset — each query re-plans shards fresh,
so which workers are currently live can change between queries (see
"Cloud deployment" below).

## Tradeoffs

**Why gRPC over REST?** The coordinator↔worker contract is internal,
high-frequency (one RPC per shard per query), and carries structured
accumulator state (`GroupState`, nested measure sketches) — protobuf's
schema and binary encoding fit that better than hand-rolled JSON over
HTTP, and gRPC's streaming/cancellation primitives (`context.Context`
propagation aborting in-flight shards on a sibling's failure) are exactly
what `RunDistributed` needed. REST would have meant reinventing this on
top of a text format not built for it.

**Why approximate aggregates (HyperLogLog, t-digest)?** Exact count-distinct
and percentiles either hold every value (a full sort, or a set the size of
the cardinality) or don't parallelize/merge cleanly across shards. The
approximate structures are O(1)-size, mergeable, and — per
[BENCHMARKS.md's M2 section](BENCHMARKS.md#m2--algorithmic-depth) — land
well within their theoretical error bounds (0.19% for a 500K-cardinality
HLL at 16 KiB fixed memory; sub-1.5% worst-case t-digest percentile error
at ~928 bytes for 10M values) for a small, honest accuracy cost. Both are
hand-rolled (stdlib only), deliberately, as the strongest "algorithms"
showcase item in this project — not because a library implementation
wouldn't work.

**Why no real SQL parser?** `internal/query.Parse` is a small fixed
grammar (`SELECT col[, AGG(col)]... [GROUP BY col,...]`) — enough to drive
`cordage query` end-to-end without taking on a general SQL parser's scope
(joins, subqueries, expressions) that this project's aggregation-only
scope doesn't need. Explicitly flagged as a stretch goal, not a gap that
blocked any milestone.

## How to run

### `local` mode — single binary, no cluster, 30 seconds

```
go build -o cordage ./cmd/cordage
go run ./cmd/gen1brc --rows 1000000 --out /tmp/measurements.csv
cordage run --file /tmp/measurements.csv --schema benchmarks/schema.json \
  --group-by station --measure temperature:min,avg,max,p99
```

`--schema` points at a small JSON file describing each column
(`benchmarks/schema.json` is a ready-made example: a `station` string
dimension, a `temperature` float64 measure). `run` prints one line per
group plus a throughput summary (`rows=... rows/sec=...`).

### `serve` mode — coordinator + workers, local processes

```
cordage worker --listen 127.0.0.1:19000 &
cordage worker --listen 127.0.0.1:19001 &
cordage coordinator --file /tmp/measurements.csv --schema benchmarks/schema.json \
  --group-by station --measure temperature:min,avg,max \
  --workers 127.0.0.1:19000,127.0.0.1:19001
```

Or as a persistent query server, driven by `cordage query`'s small SQL-like
grammar instead of one-shot flags:

```
cordage coordinator --listen :9000 --file /tmp/measurements.csv --schema benchmarks/schema.json \
  --workers 127.0.0.1:19000,127.0.0.1:19001 &
cordage query --grpc localhost:9000 "SELECT station, AVG(temperature) GROUP BY station"
```

### `serve` mode — Kubernetes (`kind` locally)

```
docker build -t cordage:local .
sed "s#__REPO_ROOT__#$(pwd)#" deploy/kind-config.yaml > /tmp/cordage-kind-config.yaml
kind create cluster --name cordage --config /tmp/cordage-kind-config.yaml
kind load docker-image cordage:local --name cordage
kubectl --context kind-cordage apply -f deploy/k8s/
kubectl --context kind-cordage port-forward svc/cordage-coordinator 9000:9000 &
cordage query --grpc localhost:9000 "SELECT station, AVG(temperature) GROUP BY station"
```

Here the coordinator resolves worker addresses via DNS against a headless
Service (`-worker-discovery-dns`, baked into `deploy/k8s/coordinator-deployment.yaml`)
instead of a static `-workers` list — `kubectl scale deployment/cordage-worker
--replicas=N` changes the worker count without redeploying the coordinator.
Full walkthrough and a real scaling benchmark:
[BENCHMARKS.md's M5 section](BENCHMARKS.md#m5--cloud-deployment).

## Benchmarks

One row per milestone, same 10M-row 1BRC-style dataset
(`--group-by station --measure temperature:min,avg,max`) except where noted.
Full numbers, methodology, and reproduction commands for every row: [BENCHMARKS.md](BENCHMARKS.md).

| Milestone | Configuration | Rows/sec | Speedup vs M0 |
|---|---|---|---|
| M0 — single-node baseline | 1 goroutine | 16,047,577 | 1.00x |
| M1 — intra-process concurrency | `--chunks 16 --workers 16` | 48,671,597 | 3.03x |
| M2 — algorithmic depth | hash-table grouping, exact | *(same throughput as M0/M1; win is 36.5% faster `Add`, approximate structures at O(1) memory — see BENCHMARKS.md)* | — |
| M3 — distribution | 8 real worker processes, loopback gRPC | 85,139,487 | 5.31x |
| M5 — cloud deployment | 8 worker pod replicas, `kind` cluster | 59,122,715 | 3.68x |

M5's absolute number sits below M3's despite identical hardware — the gap
is container networking and a shared `kind` node's control-plane pods, not
a regression in the distribution logic (see BENCHMARKS.md's M5 section for
the full breakdown). The scaling *shape* — replica count up, wall time
down, same correctness under a live scale-up with no coordinator restart —
is M5's actual exit criterion.

## What's next

Cordage's scope stops at aggregation — no joins, no persistence, no
indexes. Those land in the next project: an LSM-tree-backed key-value
store, picking up where this one deliberately stopped.

## License

[MIT](LICENSE)
