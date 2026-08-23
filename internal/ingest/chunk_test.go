package ingest

import (
	"io"
	"strings"
	"testing"
)

func TestPlanChunks(t *testing.T) {
	cases := []struct {
		size int64
		n    int
	}{
		{100, 4},
		{100, 1},
		{100, 0},
		{7, 3},
		{0, 4},
	}
	for _, c := range cases {
		chunks := PlanChunks(c.size, c.n)
		wantN := max(c.n, 1)
		if c.size <= 0 {
			if len(chunks) != 1 || chunks[0].Length != 0 {
				t.Errorf("PlanChunks(%d, %d) = %+v, want single empty chunk", c.size, c.n, chunks)
			}
			continue
		}
		if len(chunks) != wantN {
			t.Errorf("PlanChunks(%d, %d): len = %d, want %d", c.size, c.n, len(chunks), wantN)
		}
		var total int64
		for i, ch := range chunks {
			if ch.Offset != total {
				t.Errorf("PlanChunks(%d, %d): chunk %d offset = %d, want %d", c.size, c.n, i, ch.Offset, total)
			}
			total += ch.Length
		}
		if total != c.size {
			t.Errorf("PlanChunks(%d, %d): total length = %d, want %d", c.size, c.n, total, c.size)
		}
	}
}

func TestAlignChunksNeverSplitsARow(t *testing.T) {
	var sb strings.Builder
	for i := range 500 {
		sb.WriteString(strings.Repeat("x", i%7+1))
		sb.WriteByte('\n')
	}
	content := sb.String()
	path := writeTempFile(t, content)
	src, err := NewFileSource(path)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	size, _ := src.Size()
	for _, n := range []int{1, 2, 3, 5, 8} {
		naive := PlanChunks(size, n)
		aligned, err := AlignChunks(src, naive)
		if err != nil {
			t.Fatalf("AlignChunks(n=%d): %v", n, err)
		}

		var rebuilt strings.Builder
		for i, c := range aligned {
			if i > 0 && c.Offset != aligned[i-1].Offset+aligned[i-1].Length {
				t.Fatalf("n=%d: chunk %d not contiguous with previous", n, i)
			}
			r, err := src.ChunkReader(c.Offset, c.Length)
			if err != nil {
				t.Fatalf("ChunkReader: %v", err)
			}
			data, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if c.Length > 0 && data[len(data)-1] != '\n' && i != len(aligned)-1 {
				t.Errorf("n=%d: chunk %d does not end on a newline", n, i)
			}
			rebuilt.Write(data)
		}
		if aligned[len(aligned)-1].Offset+aligned[len(aligned)-1].Length != size {
			t.Errorf("n=%d: last chunk does not reach EOF", n)
		}
		if rebuilt.String() != content {
			t.Errorf("n=%d: reconstructed content does not match original", n)
		}
	}
}

func TestAlignChunksSingleChunkUnchanged(t *testing.T) {
	chunks := []Chunk{{Offset: 0, Length: 42}}
	// A nil ChunkableSource is fine here: AlignChunks must not touch the
	// source at all when there's only one chunk to align.
	aligned, err := AlignChunks(nil, chunks)
	if err != nil {
		t.Fatalf("AlignChunks: %v", err)
	}
	if len(aligned) != 1 || aligned[0] != chunks[0] {
		t.Errorf("AlignChunks single-chunk = %+v, want unchanged %+v", aligned, chunks)
	}
}
