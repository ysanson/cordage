package ingest

import (
	"fmt"
	"io"
	"os"
)

// Source is anything ingestion can read bytes from sequentially. Both a
// local file and a pipe/stdin satisfy it — this is what makes ingestion
// input-agnostic.
type Source interface {
	io.Reader
	io.Closer
}

// ChunkableSource is a Source that also supports independent, concurrent
// byte-range reads (e.g. a seekable local file). Only sources satisfying
// this interface can be split into multiple chunks; a plain Source (e.g.
// stdin) is always read as a single chunk.
//
// This interface exists in M0 purely so M1 can parallelize across chunks
// without changing the Source contract: M0 always requests one chunk,
// M1 requests N.
type ChunkableSource interface {
	Source
	// Size reports the total byte size, if known. ok is false for
	// sources without a fixed size.
	Size() (size int64, ok bool)
	// ChunkReader returns an independent reader over [offset, offset+length).
	// Multiple chunk readers may be used concurrently from different
	// goroutines.
	ChunkReader(offset, length int64) (io.ReadCloser, error)
}

// FileSource is a Source backed by a local file. Since files are
// seekable and have a known size, it also satisfies ChunkableSource.
type FileSource struct {
	f *os.File
}

// NewFileSource opens path for reading.
func NewFileSource(path string) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ingest: open file source: %w", err)
	}
	return &FileSource{f: f}, nil
}

func (s *FileSource) Read(p []byte) (int, error) { return s.f.Read(p) }
func (s *FileSource) Close() error                { return s.f.Close() }

func (s *FileSource) Size() (int64, bool) {
	info, err := s.f.Stat()
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

func (s *FileSource) ChunkReader(offset, length int64) (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(s.f, offset, length)), nil
}

// StreamSource is a Source backed by an arbitrary, non-seekable
// io.Reader (stdin, a pipe, a network connection). It is read to EOF as
// a single chunk; it does not satisfy ChunkableSource.
type StreamSource struct {
	r io.Reader
}

// NewStreamSource wraps r as a Source. If r implements io.Closer,
// Close on the returned Source closes r too; otherwise Close is a no-op.
func NewStreamSource(r io.Reader) *StreamSource {
	return &StreamSource{r: r}
}

func (s *StreamSource) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *StreamSource) Close() error {
	if c, ok := s.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
