package distribute

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/ingest"
)

// WorkerServer implements distproto.WorkerServer: it ingests exactly one
// pre-planned, newline-aligned byte range of a locally-readable file and
// returns that shard's raw, mergeable partial state. It is stateless
// between calls; every RunShard is independent.
type WorkerServer struct {
	distproto.UnimplementedWorkerServer
}

// NewWorkerServer constructs a WorkerServer.
func NewWorkerServer() *WorkerServer { return &WorkerServer{} }

// RunShard ingests req's byte range of req.FilePath and returns that
// shard's raw accumulator state. req.FilePath must be independently
// openable by this process -- M3 never ships file bytes over the wire,
// only a path, offset, and length (see worker.proto).
func (s *WorkerServer) RunShard(ctx context.Context, req *distproto.ShardRequest) (*distproto.ShardResult, error) {
	// Ensures a failed Add below never leaves ingest.Ingest's producer
	// goroutine blocked sending on a full batches channel.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	schema, err := protoToSchema(req.GetSchema())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	spec, err := protoToSpec(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := aggregate.ValidateDistributable(spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	onError, err := protoToOnError(req.GetOnError())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	agg, err := aggregate.New(schema, spec)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	fileSrc, err := ingest.NewFileSource(req.GetFilePath())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	defer fileSrc.Close()

	// A SectionReader-backed io.ReadCloser, not a ChunkableSource: this
	// always drives ingest.Ingest down its single-goroutine runSingle
	// path regardless of Config.Chunks, which is exactly what one
	// worker handling one shard needs.
	chunkSrc, err := fileSrc.ChunkReader(req.GetOffset(), req.GetLength())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var skipped int64
	cfg := ingest.Config{
		Schema:     schema,
		BatchSize:  int(req.GetBatchSize()),
		BufferSize: int(req.GetBufferSize()),
		OnError:    onError,
		OnSkippedRow: func(int64, error) {
			skipped++
		},
	}
	batches, errs := ingest.Ingest(ctx, chunkSrc, cfg)

	var rows int64
	for b := range batches {
		if err := agg.Add(b); err != nil {
			b.Release()
			return nil, status.Errorf(codes.Internal, "aggregate: %v", err)
		}
		rows += int64(b.NumRows)
		b.Release()
	}
	if err := <-errs; err != nil {
		return nil, status.Errorf(codes.Internal, "ingest: %v", err)
	}

	pbGroups, err := groupStatesToProto(agg.ExportState())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &distproto.ShardResult{Groups: pbGroups, Rows: rows, Skipped: skipped}, nil
}
