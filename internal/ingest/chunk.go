package ingest

import (
	"bytes"
	"fmt"
	"io"
)

// Chunk is a byte range [Offset, Offset+Length) within a ChunkableSource.
type Chunk struct {
	Offset int64
	Length int64
}

// PlanChunks splits size bytes into n roughly-equal chunks. The
// boundaries are naive byte offsets — they do not yet respect record
// (line) boundaries; pass the result through AlignChunks before using it
// to drive concurrent reads.
//
// n < 1 is treated as 1. This always returns exactly one chunk in M0
// (single-goroutine ingestion); M1 raises n to parallelize.
func PlanChunks(size int64, n int) []Chunk {
	if n < 1 {
		n = 1
	}
	if size <= 0 {
		return []Chunk{{Offset: 0, Length: 0}}
	}

	base := size / int64(n)
	chunks := make([]Chunk, 0, n)
	var offset int64
	for i := 0; i < n; i++ {
		length := base
		if i == n-1 {
			length = size - offset
		}
		chunks = append(chunks, Chunk{Offset: offset, Length: length})
		offset += length
	}
	return chunks
}

// probeWindow is how many bytes AlignChunks reads at a time while
// searching for the next newline past a naive chunk boundary.
const probeWindow = 64 * 1024

// AlignChunks adjusts naive chunk boundaries (as produced by PlanChunks)
// so that every chunk starts immediately after a newline and ends at one
// (except the first chunk, which starts at 0, and the last, which ends
// at the source's total size). This guarantees no chunk ever splits a
// row across a boundary, so each chunk can be parsed independently.
//
// Header lines are not special-cased here: the header (if any) lives at
// the start of chunk 0's byte range, and it is the parser's job — not
// the chunk planner's — to skip the first line only for chunk 0.
func AlignChunks(src ChunkableSource, chunks []Chunk) ([]Chunk, error) {
	if len(chunks) <= 1 {
		return chunks, nil
	}

	size, ok := src.Size()
	if !ok {
		return nil, fmt.Errorf("ingest: cannot align chunks: source has unknown size")
	}

	aligned := make([]Chunk, len(chunks))
	start := chunks[0].Offset
	for i, c := range chunks {
		var end int64
		if i == len(chunks)-1 {
			end = size
		} else {
			naiveBoundary := c.Offset + c.Length
			b, err := findNextNewline(src, naiveBoundary, size)
			if err != nil {
				return nil, err
			}
			end = b
		}
		aligned[i] = Chunk{Offset: start, Length: end - start}
		start = end
	}
	return aligned, nil
}

// findNextNewline returns the offset immediately after the first '\n' at
// or after from, or size if none is found before EOF (i.e. the last line
// has no trailing newline).
func findNextNewline(src ChunkableSource, from, size int64) (int64, error) {
	if from >= size {
		return size, nil
	}

	buf := make([]byte, probeWindow)
	for pos := from; pos < size; pos += probeWindow {
		length := probeWindow
		if remaining := size - pos; remaining < int64(length) {
			length = int(remaining)
		}

		r, err := src.ChunkReader(pos, int64(length))
		if err != nil {
			return 0, err
		}
		n, err := io.ReadFull(r, buf[:length])
		r.Close()
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return 0, fmt.Errorf("ingest: probe for newline: %w", err)
		}

		if idx := bytes.IndexByte(buf[:n], '\n'); idx >= 0 {
			return pos + int64(idx) + 1, nil
		}
	}
	return size, nil
}
