package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/distribute"
	"github.com/ysanson/cordage/internal/query"
)

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	grpcAddr := fs.String("grpc", "", "address of a long-lived `cordage coordinator -listen` process, host:port (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *grpcAddr == "" {
		return fmt.Errorf("-grpc is required for query")
	}
	if fs.NArg() != 1 {
		return fmt.Errorf(`query requires exactly one positional argument: the query text, e.g. cordage query -grpc host:port "SELECT city, AVG(temp) GROUP BY city"`)
	}

	spec, err := query.Parse(fs.Arg(0))
	if err != nil {
		return err
	}
	pbSpec, err := distribute.SpecToProto(spec)
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("query: dial %s: %w", *grpcAddr, err)
	}
	defer conn.Close()

	start := time.Now()
	resp, err := distproto.NewCoordinatorClient(conn).Query(context.Background(), &distproto.QueryRequest{Spec: pbSpec})
	elapsed := time.Since(start)
	if err != nil {
		return err
	}

	result, err := distribute.ResultFromProto(resp)
	if err != nil {
		return err
	}

	printResult(result)
	fmt.Printf("elapsed=%s\n", elapsed)
	return nil
}
