# Ingestion Data Flow

This document explains how bytes move through `internal/ingest`, from a file
or stdin to typed, columnar `Batch`es. It complements
[`cordage-project-plan.md`](./cordage-project-plan.md) by drilling into the
first pipeline stage (`Ingest`) in detail.

Source files referenced: `source.go`, `schema.go`, `chunk.go`, `parser.go`,
`batch.go`, `ingest.go` (all in `internal/ingest/`).

## 1. End-to-end flow

```
 Source              Ingest()                                  caller
(file/stdin)   ┌─────────────────────────────────────────┐
     │         │  1. resolveSchema                        │
     ├────────▶│     (explicit columns, or infer          │
     │         │      from header)                        │
     │         │                                           │
     │         │  2. newBatchPool(schema)                  │
     │         │                                           │
     │         │  3. chunkable & Chunks>1?                 │
     │         │       ├─ yes → PlanChunks + AlignChunks   │
     │         │       │        → runChunks (N goroutines) │
     │         │       └─ no  → runSingle (1 goroutine)     │
     │         │                                           │
     │         │  4. each goroutine: RowParser.next()      │      *Batch
     │         │     loop → fills a *Batch → sends it  ────┼────▶ (channel)
     │         └─────────────────────────────────────────┘         │
     │                                                              ▼
     │                                              caller drains batches,
     │                                              then reads <-errs,
     │                                              then Batch.Release()
```

`Ingest` returns immediately with two channels; all of the above runs in a
background goroutine (plus one extra goroutine per chunk when `Chunks > 1`).
The batches channel closes when reading is done (success or failure); the
caller always finishes by receiving from the error channel.

## 2. Reading: `Source` and `ChunkableSource`

Ingestion is input-agnostic because everything downstream only depends on two
small interfaces:

```
Source            = io.Reader + io.Closer
ChunkableSource    = Source + Size() + ChunkReader(offset, length)

 FileSource  ──implements──▶ ChunkableSource   (seekable, known size)
 StreamSource ──implements─▶ Source only       (stdin/pipe: no seek, no size)
```

This split is what lets the same `Ingest()` call handle a file or a live pipe
without branching in caller code: a `*FileSource` can be sliced into
independent byte-range readers via `ChunkReader`; a `*StreamSource` can only
ever be read start-to-finish, so it's always processed as a single chunk.

## 3. Schema resolution

Before any row is parsed, `Ingest` needs a fully-resolved `Schema` (column
names/types known) so the `Batch` pool can be shaped correctly up front —
this is also why resolution happens once, before any chunk goroutine starts.

```
                     schema.Columns already set?
                              │
                 ┌────────────┴────────────┐
                yes                        no
                 │                          │
                 ▼                  HasHeader == false?
         use as-is, no I/O                  │
                                   ┌─────────┴─────────┐
                                  yes                  no
                                   │                    │
                              error: cannot        chunkable AND
                              infer columns         Chunks > 1 AND
                                                    size known?
                                                          │
                                                ┌─────────┴─────────┐
                                               yes                  no
                                                │                    │
                                    peek header via            read header
                                    ChunkReader(0, …)           directly from
                                    (independent of the         the source's
                                    per-chunk readers)          own reader
                                                │                    │
                                    infer columns,          infer columns,
                                    lr = nil                return the primed
                                    (chunks open their      lineReader (lr)
                                    own readers later)      so runSingle
                                                             reuses it instead
                                                             of re-reading
                                                             the header
```

The `lr` return value matters: when there's only one reader for the whole
source (no chunking, or a non-chunkable stream), the header line has to be
consumed from *that same* `bufio.Reader` — its internal buffer may already
hold bytes past the header line, so creating a second reader from `src`
afterwards would silently drop them. `resolveSchema` hands that primed
reader to `runSingle`, which detects `lr != nil` and skips re-reading a
header itself.

## 4. Chunking: `PlanChunks` + `AlignChunks`

Only reached when `cfg.Chunks > 1` and the source is chunkable with a known
size (M0 always sets `Chunks: 1` and skips this entirely). Splitting happens
in two passes so a row is never divided across two chunks:

```
File bytes:  [ row row row | row row | row row row row ]
                          ▲          ▲
              PlanChunks: naive equal-size byte offsets (ignore content)

PlanChunks(size, 3):
 chunk 0: [0 ............. 33)
 chunk 1: [33 ............ 66)
 chunk 2: [66 ........... size)
              ▲ naive boundaries may land mid-row

AlignChunks: probe forward from each naive boundary (in 64 KiB windows via
ChunkReader) to the next '\n', then snap the boundary to just after it.

 chunk 0: [0 ..................... 41)   ← ends right after a '\n'
 chunk 1: [41 .................... 70)   ← starts right after that same '\n'
 chunk 2: [70 .................. size)   ← always ends at EOF
```

The last chunk always ends at the source's total size (no probing needed);
the first chunk always starts at 0. Every other boundary is the offset
immediately following the newline found by `findNextNewline`. Only chunk 0's
`RowParser` is told `skipHeader = true` — a header line only ever appears at
the very start of the file, which is always inside chunk 0's range.

## 5. Parsing one chunk: line → fields → values → batch

Each chunk (or the single sequential reader in M0) is driven by its own
`RowParser`, reading through a `lineReader` that grows its own buffer instead
of relying on `bufio.Scanner`'s fixed token limit:

```
 bytes ──▶ lineReader.readLine() ──▶ one line (no terminator)
                                          │
                          QuoteAware? ────┴──── no (default)
                            │                      │
                   splitFieldsQuoted           splitFields
                   (RFC4180-ish, handles        (bytes.IndexByte scan,
                   "a,b" / "" escaping)          vectorized on amd64/arm64)
                            │                      │
                            └──────────┬───────────┘
                                       ▼
                              [][]byte fields
                                       │
                     field count matches schema.Columns?
                                       │
                              ┌────────┴────────┐
                             no                 yes
                              │                  │
                        row error           parse each field with its
                     (skip or fail,         column's FieldParser into
                      see §6)               a scratch []Value buffer
                                                   │
                                          any field failed to parse?
                                                   │
                                          ┌────────┴────────┐
                                         yes                no
                                          │                  │
                                    row error          commit ALL values to
                                   (skip or fail)       batch.Cols[i], then
                                                         batch.NumRows++
```

The "parse into scratch, then commit all-or-nothing" step is deliberate:
appending each field to its column as soon as it parses would leave earlier
columns holding an orphaned value for a row that's later rejected on a
different column, silently misaligning every column after that point. All
fields for a row are validated before any of them are written to `batch.Cols`.

## 6. Error handling

```
 field-count mismatch  ──┐
                         ├──▶ handleRowError(lineNo, err)
 FieldParser failure   ──┘           │
                             ┌────────┴────────┐
                     ErrorPolicySkip     ErrorPolicyFail
                     (default)                 │
                             │            return a fatal error;
                    call OnSkippedRow     drainChunk releases the
                    (if set), keep        in-flight batch and the
                    reading                whole Ingest() call ends
```

I/O errors (a broken reader, a `ChunkReader` failure) are always fatal,
regardless of `ErrorPolicy` — that policy only governs malformed *content*.

## 7. Batches: columnar storage and pooling

```
 batchPool (one per Ingest call, sized from the resolved schema)
   │
   │ bp.get() ── either a fresh Batch (Cols pre-sized per column type)
   │             or a recycled one, reset via Cols[i].reset() (len→0, cap kept)
   ▼
 *Batch { NumRows, Cols []Column }
   Cols[i] = { Type, Strs []string  }   (if TypeString)
           = { Type, I64s []int64  }   (if TypeInt64)
           = { Type, F64s []float64 }  (if TypeFloat64)
   │
   │ RowParser.next() appends into whichever slice matches each column's type
   ▼
 sent on the batches channel once NumRows == cfg.BatchSize, or at EOF (if non-empty)
   │
   ▼
 caller processes the batch, then calls Batch.Release()
   │
   ▼
 back into batchPool for reuse — avoids allocating fresh column slices
 per batch at multi-hundred-million-row scale
```

Storing one slice per column (rather than one struct per row) is what keeps
per-row allocation near zero, and gives the future aggregation/percentile
work (M2) a layout that's natural to scan in tight loops.

## 8. Concurrency (the M1-ready path)

`runChunks` is dormant in M0 (`Chunks` is always `1`) but already wired: one
goroutine per aligned chunk, each with its own `RowParser` and its own
`ChunkReader`, all feeding the *same* batches channel and sharing the *same*
`batchPool`:

```
 chunk 0 ──▶ RowParser ──▶ drainChunk ──┐
 chunk 1 ──▶ RowParser ──▶ drainChunk ──┤
 chunk 2 ──▶ RowParser ──▶ drainChunk ──┼──▶ shared `out chan<- *Batch`
 chunk N ──▶ RowParser ──▶ drainChunk ──┘
                                          (batch order across chunks is
                                           NOT preserved — whichever
                                           goroutine fills first, sends first)

 any goroutine's fatal error → cancel() → ctx.Done() unblocks every other
 goroutine's blocked channel send/read → runChunks returns the first
 non-context.Canceled error
```

Moving from M0 to M1 is meant to be "raise `cfg.Chunks` and add a merge stage
downstream of the batches channel" — no changes to `ingest.go`'s control flow
are expected.
