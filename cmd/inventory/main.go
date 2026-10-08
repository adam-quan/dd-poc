// Command inventory runs the downstream gRPC service called from activities.
package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/temporalio/doordash-sandbox-poc/internal/demo"
	"github.com/temporalio/doordash-sandbox-poc/service/inventory"
)

func main() {
	addr := demo.Env("INVENTORY_ADDR", "127.0.0.1:50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	s := inventory.Serve(lis)
	slog.Info("inventory gRPC server listening", "addr", addr)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	s.GracefulStop()
}
