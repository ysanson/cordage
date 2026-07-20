# Proto schema

Source of truth for the coordinator↔worker gRPC contract (`internal/distribute`).
Generated Go code lives in `internal/distproto` and is committed — regenerate it
with `make proto` after editing any `.proto` file here, never hand-edit `*.pb.go`.

## One-time setup

```
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Regenerate

```
make proto
```

## Layout

- `common.proto` — `Schema`/`ColumnSchema`/`Value`, mirroring `internal/ingest`.
- `spec.proto` — `AggSpec`/`MeasureSpec`/`AggFunc`, mirroring `internal/aggregate`.
- `state.proto` — `GroupState`/`MeasureState`, the raw mergeable accumulator
  state shipped in a `ShardResult` (no percentile/t-digest field: distributed
  percentile merging is out of scope, see `aggregate.ValidateDistributable`).
- `worker.proto` — `ShardRequest`/`ShardResult` and the `Worker` service.
