// Command devtest is what a developer runs to test their local build of
// order-service in their own sandbox. It starts a test run (fresh rid,
// dedicated task queue, sandbox worker, routing lease), drives orders through
// it, verifies every hop, and tears everything down on exit (including
// Ctrl-C).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/temporalio/doordash-sandbox-poc/internal/demo"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim/shimtest"
	"github.com/temporalio/doordash-sandbox-poc/service/order"
)

func main() {
	name := flag.String("name", shimtest.SandboxName(), "sandbox name (developer)")
	app := flag.String("app", "web", "app component of the sbr")
	orders := flag.Int("orders", 1, "orders to run through the sandbox")
	hold := flag.Duration("hold", 0, "keep the sandbox up this long after the scenario (e.g. to send traffic with -sbr from cmd/starter)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *name, *app, *orders, *hold); err != nil {
		slog.Error("devtest failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, name, app string, orders int, hold time.Duration) error {
	c, err := shim.Dial(shim.ConfigFromEnv(order.Service))
	if err != nil {
		return err
	}
	defer c.Close()
	acts, closeDeps, err := demo.Activities(ctx, "")
	if err != nil {
		return err
	}
	defer closeDeps()

	tr, err := c.StartTestRun(ctx, shim.TestRunOptions{App: app, SandboxName: name, Register: order.Register(acts)})
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := tr.Close(cctx); err != nil {
			slog.Error("cleanup", "err", err)
		}
		fmt.Printf("\n[%s] cleaned up run %s: lease withdrawn, leftovers terminated, worker stopped\n", name, tr.RunID())
	}()
	fmt.Printf("[%s] test run started\n   sbr=%s\n   rid=%s\n   task queue=%s\n", name, tr.SBR(), tr.RunID(), tr.TaskQueue())

	exp := order.Expectation{Routing: tr.Routing(), Namespace: c.Config().SandboxNamespace, TaskQueue: tr.TaskQueue(), WorkerPrefix: tr.WorkerIdentity()}
	failed := 0
	for i := 1; i <= orders; i++ {
		rep, err := order.RunScenario(tr.Context(ctx), c, fmt.Sprintf("%d", 1000+i))
		if err != nil {
			return err
		}
		order.PrintHops(os.Stdout, fmt.Sprintf("%s order %d", name, i), rep)
		errs := order.Verify(rep, exp)
		for _, e := range errs {
			fmt.Printf("   FAIL %v\n", e)
		}
		if len(errs) == 0 {
			fmt.Printf("   OK: every hop carried %s and ran on %s\n", tr.Routing(), tr.TaskQueue())
		}
		failed += len(errs)
	}
	if hold > 0 {
		fmt.Printf("\n[%s] holding sandbox for %s (Ctrl-C to stop early)...\n", name, hold)
		select {
		case <-ctx.Done():
		case <-time.After(hold):
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d hop checks failed", failed)
	}
	return nil
}
