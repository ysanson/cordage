package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/ingest"
)

func runRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	filePath := fs.String("file", "", "path to the input file (reads stdin if omitted or \"-\")")
	schemaPath := fs.String("schema", "", "path to a JSON schema file (required: aggregation needs typed columns, which header-only inference can't provide)")
	onErrorFlag := fs.String("on-error", "skip", "row error policy: skip or fail")
	chunks := fs.Int("chunks", 1, "number of concurrent byte-range chunks to read a file with (ignored for stdin)")
	batchSize := fs.Int("batch-size", 0, "rows per batch (0 = package default)")
	bufferSize := fs.Int("buffer-size", 0, "per-chunk read buffer size in bytes (0 = package default)")
	quiet := fs.Bool("quiet", false, "suppress per-row skip messages")
	groupBy := fs.String("group-by", "", "comma-separated group-by column names (empty = one global group)")
	var measures measureFlags
	fs.Var(&measures, "measure", `measure spec "column:func1,func2,..." (funcs: sum,min,max,avg); repeatable`)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *schemaPath == "" {
		return fmt.Errorf("-schema is required for run: aggregation needs typed columns, which header-only inference can't provide")
	}
	schema, err := ingest.LoadSchemaFile(*schemaPath)
	if err != nil {
		return err
	}

	spec := aggregate.AggSpec{Measures: []aggregate.MeasureSpec(measures)}
	if *groupBy != "" {
		spec.GroupBy = strings.Split(*groupBy, ",")
	}
	agg, err := aggregate.New(schema, spec)
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
		if err := agg.Add(b); err != nil {
			b.Release()
			return err
		}
		rows += int64(b.NumRows)
		b.Release()
	}
	elapsed := time.Since(start)

	if err := <-errs; err != nil {
		return err
	}

	printResult(agg.Result())
	fmt.Printf("rows=%d skipped=%d elapsed=%s rows/sec=%.0f\n", rows, skipped, elapsed, float64(rows)/elapsed.Seconds())
	return nil
}

func printResult(res *aggregate.Result) {
	for _, g := range res.Groups {
		var sb strings.Builder
		for i, col := range res.GroupByColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "%s=%s", col, formatValue(g.Key[i]))
		}
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		fmt.Fprintf(&sb, "count=%d", g.Count)
		for _, m := range g.Measures {
			fmt.Fprintf(&sb, " %s(%s)=%s", m.Func, m.Column, formatValue(m.Value))
		}
		fmt.Println(sb.String())
	}
}

func formatValue(v ingest.Value) string {
	switch v.Type {
	case ingest.TypeString:
		return v.Str
	case ingest.TypeInt64:
		return strconv.FormatInt(v.I64, 10)
	case ingest.TypeFloat64:
		return strconv.FormatFloat(v.F64, 'g', -1, 64)
	default:
		return ""
	}
}

// measureFlags accumulates repeated -measure "column:func1,func2,..." flags.
type measureFlags []aggregate.MeasureSpec

func (m *measureFlags) String() string {
	if m == nil {
		return ""
	}
	parts := make([]string, len(*m))
	for i, ms := range *m {
		funcNames := make([]string, len(ms.Funcs))
		for j, f := range ms.Funcs {
			funcNames[j] = f.String()
		}
		parts[i] = ms.Column + ":" + strings.Join(funcNames, ",")
	}
	return strings.Join(parts, ";")
}

func (m *measureFlags) Set(s string) error {
	column, funcsPart, ok := strings.Cut(s, ":")
	if !ok || column == "" || funcsPart == "" {
		return fmt.Errorf(`invalid -measure %q, want "column:func1,func2,..."`, s)
	}
	funcNames := strings.Split(funcsPart, ",")
	funcs := make([]aggregate.AggFunc, 0, len(funcNames))
	for _, name := range funcNames {
		f, err := parseAggFunc(name)
		if err != nil {
			return err
		}
		funcs = append(funcs, f)
	}
	*m = append(*m, aggregate.MeasureSpec{Column: column, Funcs: funcs})
	return nil
}

func parseAggFunc(name string) (aggregate.AggFunc, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "sum":
		return aggregate.Sum, nil
	case "min":
		return aggregate.Min, nil
	case "max":
		return aggregate.Max, nil
	case "avg":
		return aggregate.Avg, nil
	default:
		return 0, fmt.Errorf("unknown aggregate function %q (want sum, min, max, or avg)", name)
	}
}
