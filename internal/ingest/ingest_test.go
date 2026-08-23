package ingest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

func basicSchema() Schema {
	return Schema{
		Delimiter: ',',
		HasHeader: true,
		Columns: []ColumnSchema{
			{Name: "city", Type: TypeString, Kind: KindDimension},
			{Name: "temperature", Type: TypeFloat64, Kind: KindMeasure},
		},
	}
}

// drain collects every batch and the (possibly nil) terminal error.
func drain(t *testing.T, batches <-chan *Batch, errs <-chan error) ([]*Batch, error) {
	t.Helper()
	var got []*Batch
	for b := range batches {
		got = append(got, b)
	}
	return got, <-errs
}

func TestIngestBasicFile(t *testing.T) {
	src, err := NewFileSource("testdata/basic.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	batches, errs := Ingest(context.Background(), src, Config{Schema: basicSchema()})
	got, err := drain(t, batches, errs)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d batches, want 1", len(got))
	}
	b := got[0]
	if b.NumRows != 3 {
		t.Fatalf("NumRows = %d, want 3", b.NumRows)
	}
	wantCities := []string{"Tokyo", "Osaka", "Kyoto"}
	wantTemps := []float64{23.5, 21.0, 19.8}
	for i := range wantCities {
		if b.Cols[0].Strs[i] != wantCities[i] {
			t.Errorf("city[%d] = %q, want %q", i, b.Cols[0].Strs[i], wantCities[i])
		}
		if b.Cols[1].F64s[i] != wantTemps[i] {
			t.Errorf("temperature[%d] = %v, want %v", i, b.Cols[1].F64s[i], wantTemps[i])
		}
	}
}

func TestIngestInferredSchemaFromHeader(t *testing.T) {
	src, err := NewFileSource("testdata/basic.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	schema := Schema{Delimiter: ',', HasHeader: true} // no Columns: infer from header
	batches, errs := Ingest(context.Background(), src, Config{Schema: schema})
	got, err := drain(t, batches, errs)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(got) != 1 || got[0].NumRows != 3 {
		t.Fatalf("got %v batches", got)
	}
	if got[0].Cols[0].Strs[0] != "Tokyo" || got[0].Cols[1].Strs[0] != "23.5" {
		t.Errorf("inferred columns should be all-string: got %+v", got[0].Cols)
	}
}

func TestIngestStreamMatchesFile(t *testing.T) {
	data, err := readTestdata(t, "testdata/basic.csv")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}

	fileSrc, err := NewFileSource("testdata/basic.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer fileSrc.Close()
	fileBatches, fileErrs := Ingest(context.Background(), fileSrc, Config{Schema: basicSchema()})
	fromFile, err := drain(t, fileBatches, fileErrs)
	if err != nil {
		t.Fatalf("Ingest(file): %v", err)
	}

	streamSrc := NewStreamSource(bytes.NewReader(data))
	streamBatches, streamErrs := Ingest(context.Background(), streamSrc, Config{Schema: basicSchema()})
	fromStream, err := drain(t, streamBatches, streamErrs)
	if err != nil {
		t.Fatalf("Ingest(stream): %v", err)
	}

	if len(fromFile) != len(fromStream) || fromFile[0].NumRows != fromStream[0].NumRows {
		t.Fatalf("file batches %v != stream batches %v", fromFile, fromStream)
	}
	for i := 0; i < fromFile[0].NumRows; i++ {
		if fromFile[0].Cols[0].Strs[i] != fromStream[0].Cols[0].Strs[i] {
			t.Errorf("row %d city mismatch: file=%q stream=%q", i, fromFile[0].Cols[0].Strs[i], fromStream[0].Cols[0].Strs[i])
		}
		if fromFile[0].Cols[1].F64s[i] != fromStream[0].Cols[1].F64s[i] {
			t.Errorf("row %d temperature mismatch: file=%v stream=%v", i, fromFile[0].Cols[1].F64s[i], fromStream[0].Cols[1].F64s[i])
		}
	}
}

func TestIngestMalformedSkipDefault(t *testing.T) {
	src, err := NewFileSource("testdata/malformed.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	var skipped []int64
	cfg := Config{
		Schema:       basicSchema(),
		OnSkippedRow: func(lineNo int64, err error) { skipped = append(skipped, lineNo) },
	}
	batches, errs := Ingest(context.Background(), src, cfg)
	got, err := drain(t, batches, errs)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(got) != 1 || got[0].NumRows != 2 {
		t.Fatalf("got %v, want 1 batch of 2 good rows", got)
	}
	if len(skipped) != 2 {
		t.Fatalf("skipped %v, want 2 rows skipped", skipped)
	}
	if got[0].Cols[0].Strs[0] != "Tokyo" || got[0].Cols[0].Strs[1] != "Kyoto" {
		t.Errorf("surviving rows = %v, want [Tokyo Kyoto]", got[0].Cols[0].Strs)
	}
}

func TestIngestMalformedFailPolicy(t *testing.T) {
	src, err := NewFileSource("testdata/malformed.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	cfg := Config{Schema: basicSchema(), OnError: ErrorPolicyFail}
	batches, errs := Ingest(context.Background(), src, cfg)
	_, err = drain(t, batches, errs)
	if err == nil {
		t.Fatal("expected an error under ErrorPolicyFail")
	}
}

func TestIngestQuotedFields(t *testing.T) {
	src, err := NewFileSource("testdata/quoted.csv")
	if err != nil {
		t.Fatalf("NewFileSource: %v", err)
	}
	defer src.Close()

	schema := Schema{
		Delimiter:  ',',
		HasHeader:  true,
		QuoteAware: true,
		Columns: []ColumnSchema{
			{Name: "city", Type: TypeString, Kind: KindDimension},
			{Name: "notes", Type: TypeString, Kind: KindDimension},
		},
	}
	batches, errs := Ingest(context.Background(), src, Config{Schema: schema})
	got, err := drain(t, batches, errs)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(got) != 1 || got[0].NumRows != 2 {
		t.Fatalf("got %v", got)
	}
	if got[0].Cols[1].Strs[0] != "largest, city" {
		t.Errorf("notes[0] = %q, want %q", got[0].Cols[1].Strs[0], "largest, city")
	}
	if got[0].Cols[1].Strs[1] != `quote " inside` {
		t.Errorf("notes[1] = %q, want %q", got[0].Cols[1].Strs[1], `quote " inside`)
	}
}

func TestIngestMultiChunkMatchesSingleChunk(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,value\n")
	const n = 5000
	var wantSum float64
	for i := range n {
		v := float64(i) + 0.5
		wantSum += v
		fmt.Fprintf(&sb, "row-%d,%v\n", i, v)
	}
	path := writeTempFile(t, sb.String())
	schema := Schema{
		Delimiter: ',',
		HasHeader: true,
		Columns: []ColumnSchema{
			{Name: "id", Type: TypeString, Kind: KindDimension},
			{Name: "value", Type: TypeFloat64, Kind: KindMeasure},
		},
	}

	sumAndCount := func(chunks int) (float64, int) {
		src, err := NewFileSource(path)
		if err != nil {
			t.Fatalf("NewFileSource: %v", err)
		}
		defer src.Close()

		batches, errs := Ingest(context.Background(), src, Config{Schema: schema, Chunks: chunks})
		got, err := drain(t, batches, errs)
		if err != nil {
			t.Fatalf("Ingest(chunks=%d): %v", chunks, err)
		}
		var sum float64
		var count int
		for _, b := range got {
			count += b.NumRows
			for _, v := range b.Cols[1].F64s {
				sum += v
			}
		}
		return sum, count
	}

	singleSum, singleCount := sumAndCount(1)
	if singleCount != n {
		t.Fatalf("single-chunk count = %d, want %d", singleCount, n)
	}
	if singleSum != wantSum {
		t.Fatalf("single-chunk sum = %v, want %v", singleSum, wantSum)
	}

	multiSum, multiCount := sumAndCount(4)
	if multiCount != n {
		t.Fatalf("multi-chunk count = %d, want %d", multiCount, n)
	}
	if multiSum != wantSum {
		t.Fatalf("multi-chunk sum = %v, want %v", multiSum, wantSum)
	}
}

func readTestdata(t *testing.T, path string) ([]byte, error) {
	t.Helper()
	src, err := NewFileSource(path)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(src); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
