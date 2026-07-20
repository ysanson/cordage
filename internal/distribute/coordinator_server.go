package distribute

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/ingest"
)

// CoordinatorConfig is a long-lived Coordinator's static, load-once
// configuration -- everything about a `cordage coordinator --listen`
// process that does not vary from query to query. Every incoming Query
// RPC calls Discover fresh and re-plans shards against this same
// Schema/FilePath (see PlanShards) -- there is no caching and no
// live-tailing of a growing file, and Discover may itself return a
// different address set from call to call (e.g. a Kubernetes worker
// Deployment's replica count changing between queries).
type CoordinatorConfig struct {
	Schema     ingest.Schema
	FilePath   string
	Discover   Discoverer
	OnError    ingest.ErrorPolicy
	BatchSize  int
	BufferSize int
}

// CoordinatorServer implements distproto.CoordinatorServer: it turns one
// QueryRequest's AggSpec into a full distributed run over cfg's static
// file/workers -- exactly what `cordage coordinator`'s one-shot mode
// already does -- and returns the finalized Result. Row/skipped-row
// counts from the run are intentionally not surfaced on the wire; those
// are operationally interesting to whoever runs the coordinator process,
// not to a remote query client.
type CoordinatorServer struct {
	distproto.UnimplementedCoordinatorServer
	cfg CoordinatorConfig
}

// NewCoordinatorServer constructs a CoordinatorServer bound to cfg.
func NewCoordinatorServer(cfg CoordinatorConfig) *CoordinatorServer {
	return &CoordinatorServer{cfg: cfg}
}

// Query decodes req's AggSpec, fail-fasts on a bad spec or a Percentile
// measure (mirroring cmd/cordage's one-shot coordinator flow), re-plans
// shards fresh, runs the distributed aggregation, and returns the
// finalized Result.
func (s *CoordinatorServer) Query(ctx context.Context, req *distproto.QueryRequest) (*distproto.Result, error) {
	spec, err := protoToSpec(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := aggregate.New(s.cfg.Schema, spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := aggregate.ValidateDistributable(spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	addrs, err := s.cfg.Discover()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	shards, err := PlanShards(s.cfg.FilePath, addrs, s.cfg.Schema)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	agg, _, _, err := RunDistributed(ctx, Options{
		Schema:     s.cfg.Schema,
		Spec:       spec,
		FilePath:   s.cfg.FilePath,
		OnError:    s.cfg.OnError,
		BatchSize:  s.cfg.BatchSize,
		BufferSize: s.cfg.BufferSize,
		Shards:     shards,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	pbResult, err := resultToProto(agg.Result())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return pbResult, nil
}
