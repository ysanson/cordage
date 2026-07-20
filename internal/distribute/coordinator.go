package distribute

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ysanson/cordage/internal/aggregate"
	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/ingest"
)

// Shard is one worker's assignment: a newline-aligned byte range of a
// file that worker at Addr must be able to open independently.
type Shard struct {
	Addr   string
	Offset int64
	Length int64
	// SkipHeader is the value this shard's wire Schema.HasHeader must
	// carry: true only for the shard whose range starts at byte 0, and
	// only when the original Schema.HasHeader is true (every other
	// shard's data never starts with a header line).
	SkipHeader bool
}

// PlanShards stats path, splits it into len(workerAddrs) newline-aligned
// byte-range chunks (via the existing ingest.PlanChunks + AlignChunks --
// the same primitives single-node chunked ingestion already uses), and
// pairs each chunk with one worker address in order.
func PlanShards(path string, workerAddrs []string, schema ingest.Schema) ([]Shard, error) {
	if len(workerAddrs) == 0 {
		return nil, fmt.Errorf("distribute: no worker addresses given")
	}

	src, err := ingest.NewFileSource(path)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	size, ok := src.Size()
	if !ok {
		return nil, fmt.Errorf("distribute: cannot determine size of %s", path)
	}

	naive := ingest.PlanChunks(size, len(workerAddrs))
	aligned, err := ingest.AlignChunks(src, naive)
	if err != nil {
		return nil, err
	}

	shards := make([]Shard, len(aligned))
	for i, c := range aligned {
		shards[i] = Shard{
			Addr:       workerAddrs[i],
			Offset:     c.Offset,
			Length:     c.Length,
			SkipHeader: schema.HasHeader && i == 0,
		}
	}
	return shards, nil
}

// Options configures one RunDistributed call.
type Options struct {
	Schema     ingest.Schema
	Spec       aggregate.AggSpec
	FilePath   string
	OnError    ingest.ErrorPolicy
	BatchSize  int
	BufferSize int
	Shards     []Shard
}

// RunDistributed dispatches one RunShard RPC per Shard concurrently,
// waits for all of them, and folds every ShardResult into a single
// *aggregate.Aggregator via MergeState -- the distributed analog of
// aggregate.RunParallel, with gRPC calls standing in for goroutines and
// MergeState standing in for Merge. If ValidateDistributable(opts.Spec)
// fails, or any shard's RunShard call errors, RunDistributed cancels the
// shared context (aborting in-flight RPCs to the remaining workers,
// mirroring RunParallel's sync.Once-guarded abort) and returns that
// error with a nil Aggregator.
func RunDistributed(ctx context.Context, opts Options) (*aggregate.Aggregator, int64, int64, error) {
	if err := aggregate.ValidateDistributable(opts.Spec); err != nil {
		return nil, 0, 0, err
	}
	if len(opts.Shards) == 0 {
		return nil, 0, 0, fmt.Errorf("distribute: no shards to dispatch")
	}

	onErrorStr, err := onErrorToProto(opts.OnError)
	if err != nil {
		return nil, 0, 0, err
	}
	pbSpec, err := specToProto(opts.Spec)
	if err != nil {
		return nil, 0, 0, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]*distproto.ShardResult, len(opts.Shards))

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	wg.Add(len(opts.Shards))
	for i, sh := range opts.Shards {
		go func(i int, sh Shard) {
			defer wg.Done()

			fail := func(err error) {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
			}

			pbSchema, err := schemaToProto(opts.Schema)
			if err != nil {
				fail(err)
				return
			}
			pbSchema.HasHeader = sh.SkipHeader

			conn, err := grpc.NewClient(sh.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fail(fmt.Errorf("distribute: dial shard %d (%s): %w", i, sh.Addr, err))
				return
			}
			defer conn.Close()

			resp, err := distproto.NewWorkerClient(conn).RunShard(ctx, &distproto.ShardRequest{
				Schema:     pbSchema,
				Spec:       pbSpec,
				FilePath:   opts.FilePath,
				Offset:     sh.Offset,
				Length:     sh.Length,
				OnError:    onErrorStr,
				BatchSize:  int32(opts.BatchSize),
				BufferSize: int32(opts.BufferSize),
			})
			if err != nil {
				fail(fmt.Errorf("distribute: shard %d (%s): %w", i, sh.Addr, err))
				return
			}
			results[i] = resp
		}(i, sh)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, 0, 0, firstErr
	}

	final, err := aggregate.New(opts.Schema, opts.Spec)
	if err != nil {
		return nil, 0, 0, err
	}

	var rows, skipped int64
	for _, r := range results {
		states, err := protoToGroupStates(r.GetGroups())
		if err != nil {
			return nil, 0, 0, err
		}
		if err := final.MergeState(states); err != nil {
			return nil, 0, 0, err
		}
		rows += r.GetRows()
		skipped += r.GetSkipped()
	}

	return final, rows, skipped, nil
}
