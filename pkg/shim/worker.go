package shim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

// Registrar registers a service's workflows and activities on a worker. The
// same function is used for production and sandbox workers, so a developer's
// sandbox runs exactly the code they are testing.
type Registrar func(r worker.Registry)

// ProductionWorkers is the production deployment of a service. It polls the
// service's base task queue in both namespaces:
//
//   - production namespace: serves ptid=dashprod traffic.
//   - sandbox namespace: serves ptid=dashtest traffic that has no matching
//     sandbox (no sbr, or no active run for it).
//
// It never polls a sandbox run's task queue, and its guard refuses anything
// that is not its tenant.
type ProductionWorkers struct {
	workers []worker.Worker
}

// NewProductionWorkers builds (but does not start) the production workers.
func (c *Client) NewProductionWorkers(register Registrar, opts worker.Options) *ProductionWorkers {
	mk := func(ns string) worker.Worker {
		o := opts
		o.Identity = identity("prod/" + ns + "/" + c.cfg.Service)
		p := &workerPolicy{role: roleProduction, namespace: ns, prodNS: c.cfg.ProdNamespace,
			identity: o.Identity, searchAttr: !c.cfg.DisableSearchAttributes}
		o.Interceptors = append([]interceptor.WorkerInterceptor{p.interceptor()}, o.Interceptors...)
		w := worker.New(c.Namespace(ns), c.cfg.Service, o)
		register(w)
		return w
	}
	return &ProductionWorkers{workers: []worker.Worker{
		mk(c.cfg.ProdNamespace),
		mk(c.cfg.SandboxNamespace),
		c.newRegistryWorker(),
	}}
}

func (p *ProductionWorkers) Start() error {
	for i, w := range p.workers {
		if err := w.Start(); err != nil {
			for _, s := range p.workers[:i] {
				s.Stop()
			}
			return err
		}
	}
	return nil
}

func (p *ProductionWorkers) Stop() {
	for _, w := range p.workers {
		w.Stop()
	}
}

// ReplayerOptions installs the same interceptor stack as shim workers, for
// worker.NewWorkflowReplayerWithOptions determinism checks against real
// histories.
func (c *Client) ReplayerOptions() worker.WorkflowReplayerOptions {
	p := &workerPolicy{role: roleReplay, identity: "replayer", searchAttr: !c.cfg.DisableSearchAttributes}
	return worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{c.tracing, p.interceptor()},
	}
}

// TestRunOptions configures a developer test run.
type TestRunOptions struct {
	// SBR is the sandbox routing key. If empty it is built from App and
	// SandboxName as <service>-<app>-sandbox-<sandboxName>.
	SBR         string
	App         string
	SandboxName string
	// RID overrides the generated run ID (normally leave empty).
	RID string
	// Register registers the workflows/activities under test.
	Register Registrar
	// WorkerOptions for the sandbox worker. Identity and the guard
	// interceptor are set by the shim.
	WorkerOptions worker.Options
}

// TestRun is one developer test run: a unique rid, a dedicated task queue in
// the sandbox namespace, a worker polling only that queue and a routing
// lease. Close (or lease expiry, if the process dies) tears all of it down.
type TestRun struct {
	c         *Client
	routing   Routing
	taskQueue string
	lease     Lease
	worker    worker.Worker
	registry  worker.Worker
	stopRenew context.CancelFunc
	renewDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// StartTestRun generates a run ID, starts a sandbox worker on the run's own
// task queue and registers the sandbox route.
func (c *Client) StartTestRun(ctx context.Context, o TestRunOptions) (*TestRun, error) {
	if o.Register == nil {
		return nil, errors.New("shim: TestRunOptions.Register is required")
	}
	sbr := o.SBR
	if sbr == "" {
		if o.App == "" || o.SandboxName == "" {
			return nil, errors.New("shim: set SBR, or App and SandboxName")
		}
		sbr = SBR(c.cfg.Service, o.App, o.SandboxName)
	}
	if !SBRService(sbr, c.cfg.Service) {
		return nil, fmt.Errorf("shim: sbr %q does not belong to service %q", sbr, c.cfg.Service)
	}
	rid := o.RID
	if rid == "" {
		rid = NewRunID()
	}
	r := Routing{PTID: TenantTest, SBR: sbr, RID: rid}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	tq := SandboxTaskQueue(sbr, rid)

	wo := o.WorkerOptions
	wo.Identity = identity("sandbox/" + sbr + "/" + rid)
	p := &workerPolicy{role: roleSandbox, namespace: c.cfg.SandboxNamespace, prodNS: c.cfg.ProdNamespace,
		expect: r, identity: wo.Identity, searchAttr: !c.cfg.DisableSearchAttributes}
	wo.Interceptors = append([]interceptor.WorkerInterceptor{p.interceptor()}, wo.Interceptors...)
	w := worker.New(c.sandbox, tq, wo)
	o.Register(w)

	run := &TestRun{
		c: c, routing: r, taskQueue: tq, worker: w, registry: c.newRegistryWorker(),
		lease: Lease{Service: c.cfg.Service, SBR: sbr, RID: rid, TaskQueue: tq, Owner: wo.Identity, TTL: c.cfg.LeaseTTL},
	}
	if err := run.registry.Start(); err != nil {
		return nil, err
	}
	if err := w.Start(); err != nil {
		run.registry.Stop()
		return nil, err
	}
	// Register the route only once the worker is polling.
	if err := c.upsertLease(ctx, run.lease); err != nil {
		w.Stop()
		run.registry.Stop()
		return nil, err
	}
	renewCtx, cancel := context.WithCancel(context.Background())
	run.stopRenew, run.renewDone = cancel, make(chan struct{})
	go run.renew(renewCtx)
	c.log.Info("test run started", "sbr", sbr, "rid", rid, "taskQueue", tq, "worker", wo.Identity)
	return run, nil
}

func (t *TestRun) renew(ctx context.Context) {
	defer close(t.renewDone)
	tick := time.NewTicker(t.lease.TTL / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := t.c.upsertLease(ctx, t.lease); err != nil && ctx.Err() == nil {
				t.c.log.Warn("lease renewal failed", "err", err, "rid", t.routing.RID)
			}
		}
	}
}

func (t *TestRun) Routing() Routing       { return t.routing }
func (t *TestRun) RunID() string          { return t.routing.RID }
func (t *TestRun) SBR() string            { return t.routing.SBR }
func (t *TestRun) TaskQueue() string      { return t.taskQueue }
func (t *TestRun) WorkerIdentity() string { return t.lease.Owner }

// Context returns parent with this run's baggage (ptid=dashtest, sbr, rid).
// Everything started through the shim with it lands on this run's workers.
func (t *TestRun) Context(parent context.Context) context.Context {
	return MustWithRouting(parent, t.routing)
}

// Close tears the run down: the route is withdrawn first (no new traffic),
// leftover workflows on the run's task queue are terminated, then the worker
// stops. With no pollers and no backlog the server unloads the task queue.
func (t *TestRun) Close(ctx context.Context) error {
	t.closeOnce.Do(func() {
		t.stopRenew()
		<-t.renewDone
		var errs []error
		if err := t.c.removeLease(ctx, t.lease); err != nil {
			errs = append(errs, err)
		}
		n, err := terminateTaskQueueWorkflows(ctx, t.c.sandbox, t.taskQueue, "sandbox test run closed")
		if err != nil {
			errs = append(errs, fmt.Errorf("terminate leftovers: %w", err))
		}
		t.worker.Stop()
		t.registry.Stop()
		t.c.log.Info("test run closed", "sbr", t.routing.SBR, "rid", t.routing.RID, "terminatedLeftovers", n)
		t.closeErr = errors.Join(errs...)
	})
	return t.closeErr
}

// Abandon stops the worker without any cleanup, simulating a developer
// process that crashed. The registry reaps the run once its lease expires.
func (t *TestRun) Abandon() {
	t.closeOnce.Do(func() {
		t.stopRenew()
		<-t.renewDone
		t.worker.Stop()
		t.registry.Stop()
	})
}
