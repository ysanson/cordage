package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ysanson/cordage/internal/ingest"
)

func runIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	filePath := fs.String("file", "", "path to the input file (reads stdin if omitted or \"-\")")
	schemaPath := fs.String("schema", "", "path to a JSON schema file; if omitted, columns are inferred from the header row as all-string dimensions")
	delimiter := fs.String("delimiter", ",", "field delimiter, a single character; ignored if -schema is set")
	noHeader := fs.Bool("no-header", false, "the input has no header row; ignored if -schema is set")
	onErrorFlag := fs.String("on-error", "skip", "row error policy: skip or fail")
	chunks := fs.Int("chunks", 1, "number of concurrent byte-range chunks to read a file with (ignored for stdin)")
	batchSize := fs.Int("batch-size", 0, "rows per batch (0 = package default)")
	bufferSize := fs.Int("buffer-size", 0, "per-chunk read buffer size in bytes (0 = package default)")
	quiet := fs.Bool("quiet", false, "suppress per-row skip messages")
	if err := fs.Parse(args); err != nil {
		return err
	}

	schema, err := resolveIngestSchema(*schemaPath, *delimiter, *noHeader)
	if err != nil {
		return err
	}

	onError, err := parseErrorPolicy(*onErrorFlag)
	if err != nil {
		return err
	}

	src, err := openIngestSource(*filePath)
	if err != nil {
		return err
	}
	defer src.Close()

	var skipped int64
	cfg := ingest.Config{
		Schema:     schema,
		Chunks:     *chunks,
		BatchSize:  *batchSize,
		BufferSize: *bufferSize,
		OnError:    onError,
		OnSkippedRow: func(lineNo int64, err error) {
			skipped++
			if !*quiet {
				fmt.Fprintf(os.Stderr, "skip line %d: %v\n", lineNo, err)
			}
		},
	}

	start := time.Now()
	batches, errs := ingest.Ingest(context.Background(), src, cfg)

	var rows int64
	for b := range batches {
		rows += int64(b.NumRows)
		b.Release()
	}
	elapsed := time.Since(start)

	if err := <-errs; err != nil {
		return err
	}

	fmt.Printf("rows=%d skipped=%d elapsed=%s rows/sec=%.0f\n", rows, skipped, elapsed, float64(rows)/elapsed.Seconds())
	return nil
}

func resolveIngestSchema(schemaPath, delimiter string, noHeader bool) (ingest.Schema, error) {
	if schemaPath != "" {
		return ingest.LoadSchemaFile(schemaPath)
	}
	if len(delimiter) != 1 {
		return ingest.Schema{}, fmt.Errorf("-delimiter must be exactly one character, got %q", delimiter)
	}
	return ingest.Schema{
		Delimiter: delimiter[0],
		HasHeader: !noHeader,
	}, nil
}

func parseErrorPolicy(s string) (ingest.ErrorPolicy, error) {
	switch s {
	case "skip":
		return ingest.ErrorPolicySkip, nil
	case "fail":
		return ingest.ErrorPolicyFail, nil
	default:
		return 0, fmt.Errorf("invalid -on-error value %q (want skip or fail)", s)
	}
}

func openIngestSource(path string) (ingest.Source, error) {
	if path == "" || path == "-" {
		return ingest.NewStreamSource(os.Stdin), nil
	}
	return ingest.NewFileSource(path)
}
