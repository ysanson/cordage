.PHONY: proto

# Regenerate internal/distproto from proto/cordage/v1/*.proto. Requires
# protoc (brew install protobuf) plus protoc-gen-go/protoc-gen-go-grpc:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# Generated code is committed; go build/go test never invoke protoc.
proto:
	protoc --proto_path=proto \
		--go_out=. --go_opt=module=github.com/ysanson/cordage \
		--go-grpc_out=. --go-grpc_opt=module=github.com/ysanson/cordage \
		proto/cordage/v1/common.proto proto/cordage/v1/spec.proto \
		proto/cordage/v1/state.proto proto/cordage/v1/worker.proto \
		proto/cordage/v1/result.proto proto/cordage/v1/coordinator.proto
