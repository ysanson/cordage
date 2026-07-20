package main

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/ysanson/cordage/internal/distproto"
	"github.com/ysanson/cordage/internal/distribute"
)

func runWorker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	listen := fs.String("listen", ":50051", "address to listen on, host:port")
	if err := fs.Parse(args); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	distproto.RegisterWorkerServer(srv, distribute.NewWorkerServer())
	fmt.Printf("cordage worker listening on %s\n", lis.Addr())
	return srv.Serve(lis)
}
