package distribute

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
)

// startTestCoordinator spins up a real loopback gRPC server hosting a
// CoordinatorServer, and returns its address. The server is stopped when
// t's test finishes.
func startTestCoordinator(t *testing.T, cfg CoordinatorConfig) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	distproto.RegisterCoordinatorServer(srv, NewCoordinatorServer(cfg))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dialTestCoordinator(t *testing.T, addr string) distproto.CoordinatorClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return distproto.NewCoordinatorClient(conn)
}

func TestCoordinatorServerQuery(t *testing.T) {
	path, _ := writeTestCSV(t, 5000)
	schema := testDistributeSchema()
	spec := testDistributeSpec()

	wantResult := singleNodeResult(t, path, schema, spec)

	const numWorkers = 4
	addrs := make([]string, numWorkers)
	for i := range addrs {
		addrs[i] = startTestWorker(t)
	}

	coordAddr := startTestCoordinator(t, CoordinatorConfig{
		Schema:      schema,
		FilePath:    path,
		WorkerAddrs: addrs,
	})
	client := dialTestCoordinator(t, coordAddr)

	pbSpec := mustSpecToProto(t, spec)
	resp, err := client.Query(context.Background(), &distproto.QueryRequest{Spec: pbSpec})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	got, err := protoToResult(resp)
	if err != nil {
		t.Fatalf("protoToResult: %v", err)
	}
	assertResultsEqual(t, got, wantResult)
}

func TestCoordinatorServerQueryRejectsPercentile(t *testing.T) {
	path, _ := writeTestCSV(t, 100)
	schema := testDistributeSchema()
	spec := aggregate.AggSpec{
		GroupBy:  []string{"station"},
		Measures: []aggregate.MeasureSpec{{Column: "temperature", Funcs: []aggregate.AggFunc{aggregate.Percentile(50)}}},
	}

	addr := startTestWorker(t)
	coordAddr := startTestCoordinator(t, CoordinatorConfig{
		Schema:      schema,
		FilePath:    path,
		WorkerAddrs: []string{addr},
	})
	client := dialTestCoordinator(t, coordAddr)

	pbSpec := mustSpecToProto(t, spec)
	_, err := client.Query(context.Background(), &distproto.QueryRequest{Spec: pbSpec})
	if err == nil {
		t.Fatal("expected error for a Percentile measure, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", status.Code(err))
	}
}

func TestCoordinatorServerQueryUnknownColumn(t *testing.T) {
	path, _ := writeTestCSV(t, 100)
	schema := testDistributeSchema()
	spec := aggregate.AggSpec{
		GroupBy:  []string{"nonexistent_column"},
		Measures: []aggregate.MeasureSpec{{Column: "temperature", Funcs: []aggregate.AggFunc{aggregate.Avg}}},
	}

	addr := startTestWorker(t)
	coordAddr := startTestCoordinator(t, CoordinatorConfig{
		Schema:      schema,
		FilePath:    path,
		WorkerAddrs: []string{addr},
	})
	client := dialTestCoordinator(t, coordAddr)

	pbSpec := mustSpecToProto(t, spec)
	_, err := client.Query(context.Background(), &distproto.QueryRequest{Spec: pbSpec})
	if err == nil {
		t.Fatal("expected error for an unknown group-by column, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", status.Code(err))
	}
}
