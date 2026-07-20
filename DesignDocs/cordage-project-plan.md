# Cordage — Project Plan

A distributed, in-memory analytics engine written in Go. Ingests large tabular datasets, shards work across concurrent workers, aggregates results, and serves them through a simple query interface — deployable as a single binary, locally or on Kubernetes.

**Showcase goals:** algorithms (hashing, aggregation, approximate structures), cloud (gRPC, Kubernetes, horizontal scaling), data processing (I/O throughput, encoding formats, benchmarking discipline).

---

## 1. Scope (what Cordage is / isn't)

**Is:**
- A single-purpose aggregation engine: `SELECT col, AGG(col) FROM data WHERE ... GROUP BY col`
- Distributed across worker processes via gRPC, coordinator does fan-out/fan-in
- Runs as one binary in two modes: `cordage local` (single machine, no cluster) and `cordage serve` (worker/coordinator node, cluster mode)

**Is not (out of scope, intentionally deferred to the next project):**
- Joins, transactions, durability/persistence, indexes, a real SQL parser (start with a small fixed grammar or JSON query spec)
- These land in your *next* project, the LSM-tree KV store — don't scope-creep them in here

---

## 2. Architecture

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

**Pipeline stages:** Ingest → Parse/Transform → Aggregate (local, per worker) → Merge (coordinator) → Respond

---

## 3. Milestones

### M0 — Single-node baseline (this is your "1BRC, but reusable")
- [X] Read a large CSV (start with the 1BRC dataset itself — you already have it) with buffered, chunked I/O
- [X] Implement group-by + aggregate (sum, count, min, max, avg) using a single goroutine, correctness-first
- [X] Add a benchmark harness: rows/sec, wall time, peak RSS — write results to a `BENCHMARKS.md` from day one
- **Exit criteria:** correct output on the 1BRC dataset, benchmarked baseline number to beat

### M1 — Concurrency within a single node
- [X] Parallelize parsing/aggregation across goroutines (worker pool over file chunks)
- [X] Merge per-goroutine partial aggregates into a final result (this *is* the map-reduce shape, just intra-process first)
- [X] Compare against M0 baseline — this is your first real "engineering tradeoffs" story for the README
- **Exit criteria:** documented speedup vs M0, with an explanation of where the ceiling is (I/O-bound vs CPU-bound)

### M2 — Algorithmic depth
- [X] Swap naive `map[string]struct` grouping for a faster hash strategy (custom hashing, avoid string allocs where possible)
- [X] Add approximate aggregates: HyperLogLog for count-distinct, t-digest (or similar) for percentiles — this is your strongest "algorithms" showcase item
- [X] Benchmark exact vs approximate: accuracy/memory/speed tradeoff table in the README
- **Exit criteria:** a working percentile/distinct-count query path, with a tradeoff writeup

### M3 — Distribution
- [X] Define the gRPC service contract: coordinator ↔ worker (shard assignment, partial result return)
- [X] Coordinator splits input (by file, by byte range, or by pre-partitioned files) across N worker processes
- [X] Local multi-process test: spin up N worker binaries + 1 coordinator on one machine, confirm correctness matches M2's single-node result exactly
- **Exit criteria:** distributed result identical to single-node result, plus a scaling chart (1, 2, 4, 8 workers vs throughput)

### M4 — Query interface
- [X] Small fixed query grammar or JSON query spec (defer full SQL parsing — note this explicitly as a stretch goal in the README, don't let it block M5)
- [X] `cordage query` CLI subcommand hits a running coordinator over gRPC
- **Exit criteria:** can run `cordage query --grpc localhost:9000 "SELECT city, AVG(temp) GROUP BY city"` (or your JSON equivalent) end-to-end

### M5 — Cloud deployment
- [ ] Dockerfile for the binary (multi-stage build, small final image)
- [ ] Kubernetes manifests: a Deployment for workers (replica count = scale knob), a Service for the coordinator, ConfigMap for shard/config
- [ ] Deploy to a real cluster (kind/minikube locally is fine, or a cheap managed cluster if you want screenshots of it actually running)
- [ ] Horizontal scaling demo: change replica count, re-run the same query, show the throughput change
- **Exit criteria:** a documented, reproducible `kubectl apply` deployment with a scaling benchmark

### M6 — Polish for showcase
- [ ] README with: architecture diagram, benchmark tables from every milestone, explicit tradeoffs discussed (why no real SQL parser yet, why approximate aggregates, why gRPC over REST)
- [ ] A short demo (asciinema recording or a few screenshots) showing `cordage local` vs `cordage serve` at different scales
- [ ] Tag a `v0.1.0` release

---

## 4. Tech stack

| Concern | Choice | Why |
|---|---|---|
| Language | Go | given |
| RPC | gRPC (`google.golang.org/grpc`) | you specifically want this |
| CLI | `cobra` or stdlib `flag` + subcommands | thin, dual-mode binary |
| Serialization | Protobuf (pairs naturally with gRPC) | consistent wire format coordinator↔worker |
| Approximate structures | roll your own HyperLogLog / t-digest first, compare against a library later | this *is* the algorithms showcase — don't outsource it |
| Deployment | Docker + Kubernetes (kind/minikube for local dev) | matches your cloud goal |
| Benchmarking | Go's built-in `testing.B` + a custom throughput harness for full-pipeline runs | credibility requires numbers, not claims |

---

## 5. README / showcase structure (write this incrementally, not at the end)

1. What it is / what it isn't (be upfront about scope, like this plan is)
2. Architecture diagram
3. Benchmark table, one row per milestone (M0 → M5), so the reader sees the progression
4. "Why gRPC," "why approximate aggregates," "why no SQL parser yet" — a short tradeoffs section
5. How to run: `local` mode in 30 seconds, `serve` mode with Docker Compose or kind in a few more
6. What's next (explicitly point to the KV store project as "part two")

---

## 6. Suggested order of work

Start at M0 even though it feels redundant with 1BRC — it's your control group for every later benchmark, and you'll want that number in the README regardless. Don't jump to distribution (M3) before you've squeezed the single-node path (M1–M2), since a slow single-node aggregator distributed across 8 workers is still a slow aggregator — the algorithmic work compounds, the distribution work doesn't fix bad aggregation.
