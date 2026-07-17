package ingest

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t testing.TB, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func TestFileSourceReadAndClose(t *testing.T) {
	path := writeTempFile(t, "hello world")
	src, err := NewFileSource(path)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	data, err := io.ReadAll(src)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("data = %q, want %q", data, "hello world")
	}
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestFileSourceSizeAndChunkReader(t *testing.T) {
	path := writeTempFile(t, "0123456789")
	src, err := NewFileSource(path)
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	size, ok := src.Size()
	if !ok || size != 10 {
		t.Fatalf("Size() = (%d, %v), want (10, true)", size, ok)
	}

	r, err := src.ChunkReader(3, 4)
	if err != nil {
		t.Fatalf("ChunkReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll chunk: %v", err)
	}
	if string(got) != "3456" {
		t.Errorf("chunk = %q, want %q", got, "3456")
	}
}

// Compile-time assertion that FileSource is chunkable and StreamSource is not.
var (
	_ ChunkableSource = (*FileSource)(nil)
	_ Source          = (*StreamSource)(nil)
)

func TestStreamSourceRead(t *testing.T) {
	src := NewStreamSource(bytes.NewBufferString("piped data"))
	data, err := io.ReadAll(src)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "piped data" {
		t.Errorf("data = %q, want %q", data, "piped data")
	}
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestStreamSourceIsNotChunkable(t *testing.T) {
	var s Source = NewStreamSource(bytes.NewBufferString("x"))
	if _, ok := s.(ChunkableSource); ok {
		t.Fatal("StreamSource must not satisfy ChunkableSource")
	}
}
