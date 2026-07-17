// Command gen1brc generates a synthetic "station;temperature" dataset in
// the same spirit as the 1BRC challenge, for benchmarking Cordage's
// ingest/aggregate pipeline without depending on the (much larger)
// original file. It is a development tool, not part of the cordage
// product binary.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func main() {
	rows := flag.Int64("rows", 10_000_000, "number of rows to generate")
	out := flag.String("out", "benchmarks/data/measurements.csv", "output file path")
	seed := flag.Uint64("seed", 42, "PRNG seed, for reproducible datasets")
	stddev := flag.Float64("stddev", 10, "standard deviation of each station's temperature distribution")
	flag.Parse()

	if err := run(*rows, *out, *seed, *stddev); err != nil {
		fmt.Fprintf(os.Stderr, "gen1brc: %v\n", err)
		os.Exit(1)
	}
}

func run(rows int64, out string, seed uint64, stddev float64) error {
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	rng := rand.New(rand.NewPCG(seed, seed^0xdeadbeef))

	start := time.Now()
	var line []byte
	for i := int64(0); i < rows; i++ {
		st := stations[rng.IntN(len(stations))]
		temp := st.mean + rng.NormFloat64()*stddev

		line = line[:0]
		line = append(line, st.name...)
		line = append(line, ';')
		line = strconv.AppendFloat(line, temp, 'f', 1, 64)
		line = append(line, '\n')
		if _, err := w.Write(line); err != nil {
			return fmt.Errorf("write row %d: %w", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat output file: %w", err)
	}
	fmt.Printf("wrote %d rows (%d stations) to %s (%.1f MiB) in %s\n",
		rows, len(stations), out, float64(info.Size())/(1<<20), time.Since(start))
	return nil
}
