package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"flag"

	"google.golang.org/grpc"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/distribute"
	"github.com/ysanson/cordage/internal/ingest"
)

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	filePath := fs.String("file", "", "path to the input file (required: distributed mode cannot split stdin across worker processes)")
	schemaPath := fs.String("schema", "", "path to a JSON schema file (required)")
	onErrorFlag := fs.String("on-error", "skip", "row error policy: skip or fail")
	batchSize := fs.Int("batch-size", 0, "rows per batch (0 = package default)")
	bufferSize := fs.Int("buffer-size", 0, "per-chunk read buffer size in bytes (0 = package default)")
	groupBy := fs.String("group-by", "", "comma-separated group-by column names (empty = one global group)")
	workerAddrs := fs.String("workers", "", "comma-separated worker addresses, host:port,host:port,... (mutually exclusive with -worker-discovery-dns; one is required)")
	workerDiscoveryDNS := fs.String("worker-discovery-dns", "", "host:port of a Kubernetes headless Service; re-resolved via DNS before every run so worker addresses track replica count (mutually exclusive with -workers)")
	listen := fs.String("listen", "", "if set, run as a long-lived query server on this address (host:port) instead of one-shot mode; mutually exclusive with -group-by/-measure")
	var measures measureFlags
	fs.Var(&measures, "measure", `measure spec "column:func1,func2,..." (funcs: sum,min,max,avg,distinct; percentile funcs are not supported in distributed mode); repeatable`)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *filePath == "" || *filePath == "-" {
		return fmt.Errorf("-file is required for coordinator: distributed mode cannot split stdin across worker processes")
	}
	if *schemaPath == "" {
		return fmt.Errorf("-schema is required for coordinator")
	}
	if (*workerAddrs == "") == (*workerDiscoveryDNS == "") {
		return fmt.Errorf("coordinator: exactly one of -workers or -worker-discovery-dns is required")
	}

	var discoverer distribute.Discoverer
	if *workerAddrs != "" {
		discoverer = distribute.StaticDiscoverer(strings.Split(*workerAddrs, ","))
	} else {
		host, portStr, err := net.SplitHostPort(*workerDiscoveryDNS)
		if err != nil {
			return fmt.Errorf("-worker-discovery-dns must be host:port: %w", err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("-worker-discovery-dns port must be numeric: %w", err)
		}
		discoverer = distribute.DNSDiscoverer(host, port)
	}

	schema, err := ingest.LoadSchemaFile(*schemaPath)
	if err != nil {
		return err
	}

	spec := aggregate.AggSpec{Measures: []aggregate.MeasureSpec(measures)}
	if *groupBy != "" {
		spec.GroupBy = strings.Split(*groupBy, ",")
	}
	// Fail fast on a bad -group-by/-measure or a Percentile measure
	// (which distributed mode cannot merge) before touching the file.
	if _, err := aggregate.New(schema, spec); err != nil {
		return err
	}
	if err := aggregate.ValidateDistributable(spec); err != nil {
		return err
	}

	onError, err := parseErrorPolicy(*onErrorFlag)
	if err != nil {
		return err
	}

	if *listen != "" {
		if *groupBy != "" || len(measures) > 0 {
			return fmt.Errorf("coordinator: -listen cannot be combined with -group-by/-measure; each query supplies its own spec")
		}
		return runCoordinatorServe(*listen, distribute.CoordinatorConfig{
			Schema:     schema,
			FilePath:   *filePath,
			Discover:   discoverer,
			OnError:    onError,
			BatchSize:  *batchSize,
			BufferSize: *bufferSize,
		})
	}

	addrs, err := discoverer()
	if err != nil {
		return err
	}
	shards, err := distribute.PlanShards(*filePath, addrs, schema)
	if err != nil {
		return err
	}

	start := time.Now()
	agg, rows, skipped, err := distribute.RunDistributed(context.Background(), distribute.Options{
		Schema:     schema,
		Spec:       spec,
		FilePath:   *filePath,
		OnError:    onError,
		BatchSize:  *batchSize,
		BufferSize: *bufferSize,
		Shards:     shards,
	})
	elapsed := time.Since(start)
	if err != nil {
		return err
	}

	printResult(agg.Result())
	fmt.Printf("rows=%d skipped=%d elapsed=%s rows/sec=%.0f shards=%d\n",
		rows, skipped, elapsed, float64(rows)/elapsed.Seconds(), len(shards))
	return nil
}

// runCoordinatorServe starts a long-lived gRPC server hosting only the
// Coordinator service (workers stay separate `cordage worker` processes,
// unchanged) and blocks forever, answering queries against cfg.
func runCoordinatorServe(listen string, cfg distribute.CoordinatorConfig) error {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	distproto.RegisterCoordinatorServer(srv, distribute.NewCoordinatorServer(cfg))
	fmt.Printf("cordage coordinator listening on %s (file=%s)\n", lis.Addr(), cfg.FilePath)
	return srv.Serve(lis)
}
