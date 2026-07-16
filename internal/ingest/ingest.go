package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

const (
	defaultBufferSize = 1 << 20 // 1 MiB per-chunk read buffer
	defaultBatchSize  = 4096    // rows per Batch
)

// ErrorPolicy controls what happens when a row fails to parse (wrong
// field count, a value that doesn't match its declared type, ...).
// It does not apply to I/O errors, which are always fatal.
type ErrorPolicy int

const (
	// ErrorPolicySkip discards the bad row, invokes Config.OnSkippedRow
	// if set, and continues. This is the default: one malformed row
	// (a corrupt line, a truncated write on a live source) shouldn't
	// take down an otherwise-good multi-gigabyte ingest.
	ErrorPolicySkip ErrorPolicy = iota
	// ErrorPolicyFail aborts ingestion on the first bad row.
	ErrorPolicyFail
)

// Config configures a call to Ingest.
type Config struct {
	// Schema describes the columns to parse. If Schema.Columns is empty
	// and Schema.HasHeader is true, columns are inferred (as all-string
	// dimensions) from the input's header line.
	Schema Schema

	// Chunks is the desired number of independent, concurrently-read
	// byte-range chunks. Values <= 1 (the M0 default) process the
	// source sequentially on a single goroutine. Values > 1 only take
	// effect when the Source is a ChunkableSource with a known size;
	// otherwise Ingest silently falls back to a single chunk.
	Chunks int

	// BatchSize is the number of rows accumulated per Batch before it's
	// sent downstream. Defaults to 4096.
	BatchSize int

	// BufferSize is the buffered-reader size, in bytes, used per chunk.
	// Defaults to 1 MiB.
	BufferSize int

	// OnError selects behavior for malformed rows. Defaults to
	// ErrorPolicySkip.
	OnError ErrorPolicy

	// OnSkippedRow, if set, is called for every row dropped under
	// ErrorPolicySkip, with the 1-based line number (relative to the
	// start of that row's chunk) and the parse error. It may be called
	// concurrently from multiple chunk goroutines.
	OnSkippedRow func(lineNo int64, err error)
}

func (c Config) withDefaults() Config {
	if c.Chunks < 1 {
		c.Chunks = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaultBufferSize
	}
	return c
}

// Ingest reads src to completion and returns a channel of Batches and a
// channel that carries at most one fatal error. The batches channel is
// closed when ingestion finishes (successfully or not); the caller
// should drain it and then check the error channel. Each received Batch
// must eventually have Release called on it.
//
// Ingest reads until EOF and returns; it does not support unbounded,
// continuously-open sources (e.g. a live tail of a growing file) — both
// file and stream sources are treated as finite.
func Ingest(ctx context.Context, src Source, cfg Config) (<-chan *Batch, <-chan error) {
	cfg = cfg.withDefaults()
	batches := make(chan *Batch)
	errs := make(chan error, 1)

	go func() {
		defer close(batches)
		defer close(errs)
		if err := ingest(ctx, src, cfg, batches); err != nil {
			errs <- err
		}
	}()

	return batches, errs
}

func ingest(ctx context.Context, src Source, cfg Config, out chan<- *Batch) error {
	schema, lr, err := resolveSchema(src, cfg)
	if err != nil {
		return err
	}
	bp := newBatchPool(schema, cfg.BatchSize)

	if cs, ok := src.(ChunkableSource); ok && cfg.Chunks > 1 {
		if size, ok := cs.Size(); ok && size > 0 {
			naive := PlanChunks(size, cfg.Chunks)
			aligned, err := AlignChunks(cs, naive)
			if err != nil {
				return err
			}
			return runChunks(ctx, cs, aligned, schema, cfg, bp, out)
		}
	}
	return runSingle(ctx, src, lr, schema, cfg, bp, out)
}

// resolveSchema fills in Schema.Columns when the caller didn't specify
// them explicitly, so that the Batch pool below is always shaped
// correctly before any row is parsed.
//
// When the source is chunkable and will actually be split into multiple
// chunks, columns are inferred via an independent peek read (chunks
// after the first have no header of their own to infer from, so this
// must happen before any chunk goroutine starts).
//
// Otherwise there is exactly one reader for the whole source, so
// inference reads the header directly from it — returning that
// lineReader so the caller reuses it (its internal buffer may already
// hold bytes past the header line) rather than constructing a second
// reader over the same source.
func resolveSchema(src Source, cfg Config) (Schema, *lineReader, error) {
	schema := cfg.Schema
	if len(schema.Columns) > 0 {
		return schema, nil, nil
	}
	if !schema.HasHeader {
		return Schema{}, nil, fmt.Errorf("ingest: schema has no columns and HasHeader is false; cannot infer columns")
	}

	if cs, ok := src.(ChunkableSource); ok && cfg.Chunks > 1 {
		if size, ok := cs.Size(); ok && size > 0 {
			names, err := peekHeaderColumns(cs, size, schema.delimiterOrDefault())
			if err != nil {
				return Schema{}, nil, err
			}
			return schema.withInferredColumns(names), nil, nil
		}
	}

	lr := newLineReader(src, cfg.BufferSize)
	line, err := lr.readLine()
	if err != nil && err != io.EOF {
		return Schema{}, nil, fmt.Errorf("ingest: read header line: %w", err)
	}
	if err == io.EOF {
		return schema, lr, nil
	}
	fields := splitFields(nil, line, schema.delimiterOrDefault())
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = string(f)
	}
	return schema.withInferredColumns(names), lr, nil
}

// peekHeaderColumns reads just enough of a ChunkableSource's start to
// extract the header line, without disturbing any chunk's own read.
func peekHeaderColumns(cs ChunkableSource, size int64, delim byte) ([]string, error) {
	length := int64(probeWindow)
	if size < length {
		length = size
	}
	r, err := cs.ChunkReader(0, length)
	if err != nil {
		return nil, fmt.Errorf("ingest: peek header: %w", err)
	}
	defer r.Close()

	buf := make([]byte, length)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fmt.Errorf("ingest: peek header: %w", err)
	}
	buf = buf[:n]

	idx := bytes.IndexByte(buf, '\n')
	if idx < 0 {
		return nil, fmt.Errorf("ingest: header line exceeds probe window (%d bytes)", probeWindow)
	}
	line := buf[:idx]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	fields := splitFields(nil, line, delim)
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = string(f)
	}
	return names, nil
}

// runSingle reads src sequentially on the calling goroutine — the M0
// path, and the path taken whenever the source isn't chunkable. If lr is
// non-nil, resolveSchema already consumed the header line through it and
// it must be reused; otherwise a fresh reader is created and the header
// (if any) is still pending.
func runSingle(ctx context.Context, src Source, lr *lineReader, schema Schema, cfg Config, bp *batchPool, out chan<- *Batch) error {
	headerAlreadyConsumed := lr != nil
	if lr == nil {
		lr = newLineReader(src, cfg.BufferSize)
	}
	skipHeader := schema.HasHeader && !headerAlreadyConsumed
	rp := newRowParserFromReader(lr, schema, skipHeader, cfg.OnError, cfg.OnSkippedRow)
	return drainChunk(ctx, rp, bp, cfg.BatchSize, out)
}

// runChunks reads each aligned chunk independently and concurrently,
// merging their Batches onto a single output channel. This is the path
// M1 activates by raising Config.Chunks; M0 never reaches it.
func runChunks(ctx context.Context, cs ChunkableSource, chunks []Chunk, schema Schema, cfg Config, bp *batchPool, out chan<- *Batch) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(chunks))
	for i, c := range chunks {
		wg.Add(1)
		go func(idx int, chunk Chunk) {
			defer wg.Done()
			r, err := cs.ChunkReader(chunk.Offset, chunk.Length)
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			defer r.Close()

			skipHeader := schema.HasHeader && idx == 0
			rp := newRowParser(r, schema, cfg.BufferSize, skipHeader, cfg.OnError, cfg.OnSkippedRow)
			if err := drainChunk(ctx, rp, bp, cfg.BatchSize, out); err != nil {
				errCh <- err
				cancel()
			}
		}(i, c)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil && err != context.Canceled {
			return err
		}
	}
	return nil
}

// drainChunk reads every row from rp, filling and emitting Batches of up
// to batchSize rows via out.
func drainChunk(ctx context.Context, rp *RowParser, bp *batchPool, batchSize int, out chan<- *Batch) error {
	batch := bp.get()
	for {
		select {
		case <-ctx.Done():
			batch.Release()
			return ctx.Err()
		default:
		}

		res, err := rp.next(batch)
		if err != nil {
			batch.Release()
			return err
		}

		switch res {
		case rowEOF:
			if batch.NumRows == 0 {
				batch.Release()
				return nil
			}
			select {
			case out <- batch:
			case <-ctx.Done():
				batch.Release()
				return ctx.Err()
			}
			return nil
		case rowOK:
			if batch.NumRows >= batchSize {
				select {
				case out <- batch:
				case <-ctx.Done():
					batch.Release()
					return ctx.Err()
				}
				batch = bp.get()
			}
		case rowSkipped:
			// Nothing to do: the row was dropped and reported via
			// Config.OnSkippedRow; keep reading.
		}
	}
}
