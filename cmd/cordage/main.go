// Command cordage is the Cordage CLI: a single binary that runs in
// different modes (today: ingest; later: local, serve, query).
package main

import (
	"fmt"
	"os"
)

var commands = map[string]func(args []string) error{
	"ingest":      runIngest,
	"run":         runRun,
	"worker":      runWorker,
	"coordinator": runCoordinator,
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "cordage: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err := cmd(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "cordage: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cordage <command> [flags]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  ingest       read a file or stdin through the ingestion pipeline and report stats")
	fmt.Fprintln(os.Stderr, "  run          ingest, then group-by/aggregate, and print the result plus throughput stats")
	fmt.Fprintln(os.Stderr, "  worker       run a gRPC worker server that ingests/aggregates a shard on request")
	fmt.Fprintln(os.Stderr, "  coordinator  split a file across worker addresses, dispatch shards, merge, and print the result")
}
