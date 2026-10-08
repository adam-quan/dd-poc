// Command prodworker is the production deployment of order-service. It polls
// the base task queue in the production namespace (dashprod traffic) and in
// the sandbox namespace (dashtest traffic with no matching sandbox), and hosts
// the sandbox registry.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"

	"github.com/temporalio/doordash-sandbox-poc/internal/demo"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
	"github.com/temporalio/doordash-sandbox-poc/service/order"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("prodworker", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	c, err := shim.Dial(shim.ConfigFromEnv(order.Service))
	if err != nil {
		return err
	}
	defer c.Close()
	acts, closeDeps, err := demo.Activities(ctx, "order-events-audit")
	if err != nil {
		return err
	}
	defer closeDeps()

	ws := c.NewProductionWorkers(order.Register(acts), worker.Options{})
	if err := ws.Start(); err != nil {
		return err
	}
	defer ws.Stop()
	slog.Info("production workers running", "service", order.Service,
		"queues", []string{"production/" + order.Service, "sandbox/" + order.Service, "sandbox/" + shim.RegistryTaskQueue})
	<-ctx.Done()
	return nil
}
