package shim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
)

// Search attributes stamped on every workflow started through the shim.
var (
	SAPtid = temporal.NewSearchAttributeKeyKeyword("Ptid")
	SASbr  = temporal.NewSearchAttributeKeyKeyword("Sbr")
	SARid  = temporal.NewSearchAttributeKeyKeyword("Rid")
)

// Client is a baggage-aware Temporal client for one service. Every call reads
// ptid/sbr/rid from the ctx baggage and picks the namespace (and, for starts,
// the task queue) accordingly. Callers never choose either directly.
type Client struct {
	cfg     Config
	prod    client.Client
	sandbox client.Client
	tracer  trace.Tracer
	log     *slog.Logger
	tracing interceptor.Interceptor
	owner   bool
}

// Dial connects to both namespaces over one gRPC connection.
func Dial(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if cfg.Service == "" {
		return nil, errors.New("shim: Config.Service is required")
	}
	tp := tracerProvider(cfg)
	tracing, err := newTracingInterceptor(tp)
	if err != nil {
		return nil, err
	}
	sdkLogger := log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.SDKLogLevel})))
	opts := client.Options{
		HostPort:     cfg.HostPort,
		Namespace:    cfg.ProdNamespace,
		Interceptors: []interceptor.ClientInterceptor{tracing},
		Logger:       sdkLogger,
	}
	prod, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("shim: dial %s/%s: %w", cfg.HostPort, cfg.ProdNamespace, err)
	}
	opts.Namespace = cfg.SandboxNamespace
	sandbox, err := client.NewClientFromExisting(prod, opts)
	if err != nil {
		prod.Close()
		return nil, err
	}
	return &Client{
		cfg:     cfg,
		prod:    prod,
		sandbox: sandbox,
		tracer:  tp.Tracer("github.com/temporalio/doordash-sandbox-poc/shim"),
		log:     cfg.Logger.With("service", cfg.Service),
		tracing: tracing,
		owner:   true,
	}, nil
}

// ForService returns a client sharing this connection that routes for another
// service (e.g. to start a workflow owned by a different team).
func (c *Client) ForService(service string) *Client {
	cp := *c
	cp.cfg.Service = service
	cp.log = c.cfg.Logger.With("service", service)
	cp.owner = false
	return &cp
}

// Close closes the underlying connection (only on the client returned by Dial).
func (c *Client) Close() {
	if !c.owner {
		return
	}
	c.sandbox.Close()
	c.prod.Close()
}

func (c *Client) Config() Config { return c.cfg }

// Namespace returns the raw SDK client for a namespace. Escape hatch for admin
// tasks (describe, list); it bypasses routing.
func (c *Client) Namespace(ns string) client.Client {
	if ns == c.cfg.SandboxNamespace {
		return c.sandbox
	}
	return c.prod
}

// Route is a routing decision for a workflow start.
type Route struct {
	Namespace string
	TaskQueue string
	Routing   Routing
	// Sandbox is true when the start is pinned to a developer's run.
	Sandbox bool
	Reason  string
}

// Resolve decides where a workflow started with ctx would go. It also returns
// ctx with the effective baggage (e.g. rid filled in from the lease when only
// sbr was supplied).
func (c *Client) Resolve(ctx context.Context) (Route, context.Context, error) {
	r := RoutingFromContext(ctx)
	if err := r.Validate(); err != nil {
		return Route{}, ctx, err
	}
	base := c.cfg.Service
	switch {
	case r.IsProd():
		return Route{Namespace: c.cfg.ProdNamespace, TaskQueue: base, Routing: r, Reason: "ptid=dashprod"}, ctx, nil
	case r.SBR == "":
		return Route{Namespace: c.cfg.SandboxNamespace, TaskQueue: base, Routing: r,
			Reason: "ptid=dashtest without sbr -> production workers in sandbox namespace"}, ctx, nil
	case !SBRService(r.SBR, base):
		return Route{Namespace: c.cfg.SandboxNamespace, TaskQueue: base, Routing: r,
			Reason: fmt.Sprintf("sbr %s targets another service -> production workers", r.SBR)}, ctx, nil
	}
	lease, ok, err := c.lookupLease(ctx, r)
	if err != nil {
		// Registry unavailable: fail safe to the production workers of the
		// sandbox namespace rather than dropping test traffic.
		c.log.Warn("sandbox registry lookup failed; routing to production workers", "err", err, "routing", r.String())
	}
	if !ok {
		return Route{Namespace: c.cfg.SandboxNamespace, TaskQueue: base, Routing: r,
			Reason: fmt.Sprintf("no active sandbox for sbr=%s rid=%s -> production workers", r.SBR, orDash(r.RID))}, ctx, nil
	}
	if r.RID == "" {
		r.RID = lease.RID
		var werr error
		if ctx, werr = WithRouting(ctx, r); werr != nil {
			return Route{}, ctx, werr
		}
	}
	return Route{Namespace: c.cfg.SandboxNamespace, TaskQueue: lease.TaskQueue, Routing: r, Sandbox: true,
		Reason: "matched sandbox lease"}, ctx, nil
}

// ExecuteWorkflow starts a workflow routed by the ctx baggage. Namespace and
// TaskQueue in opts are always overridden by the routing decision.
func (c *Client) ExecuteWorkflow(ctx context.Context, opts client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	route, ctx, err := c.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	ctx, end := ensureSpan(ctx, c.tracer, "shim.ExecuteWorkflow")
	defer end()
	if opts.TaskQueue != "" && opts.TaskQueue != route.TaskQueue {
		c.log.Debug("overriding caller task queue", "requested", opts.TaskQueue, "routed", route.TaskQueue)
	}
	opts.TaskQueue = route.TaskQueue
	opts.ID = ScopedWorkflowID(route.Routing, opts.ID)
	if !c.cfg.DisableSearchAttributes {
		opts.TypedSearchAttributes = withRoutingSA(opts.TypedSearchAttributes, route.Routing)
	}
	c.log.Info("start workflow", "workflowID", opts.ID, "namespace", route.Namespace,
		"taskQueue", route.TaskQueue, "routing", route.Routing.String(), "reason", route.Reason)
	return c.Namespace(route.Namespace).ExecuteWorkflow(ctx, opts, workflow, args...)
}

// SignalWorkflow signals a workflow; the signal carries the ctx baggage.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error {
	cl, r, ctx, end, err := c.forMessage(ctx, "shim.SignalWorkflow")
	if err != nil {
		return err
	}
	defer end()
	return cl.SignalWorkflow(ctx, ScopedWorkflowID(r, workflowID), runID, signalName, arg)
}

// QueryWorkflow queries a workflow; the query carries the ctx baggage.
func (c *Client) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error) {
	cl, r, ctx, end, err := c.forMessage(ctx, "shim.QueryWorkflow")
	if err != nil {
		return nil, err
	}
	defer end()
	return cl.QueryWorkflow(ctx, ScopedWorkflowID(r, workflowID), runID, queryType, args...)
}

// UpdateWorkflow sends an update; the update carries the ctx baggage.
func (c *Client) UpdateWorkflow(ctx context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	cl, r, ctx, end, err := c.forMessage(ctx, "shim.UpdateWorkflow")
	if err != nil {
		return nil, err
	}
	defer end()
	opts.WorkflowID = ScopedWorkflowID(r, opts.WorkflowID)
	return cl.UpdateWorkflow(ctx, opts)
}

// GetWorkflow returns a handle; with an empty runID it follows continue-as-new.
func (c *Client) GetWorkflow(ctx context.Context, workflowID, runID string) (client.WorkflowRun, error) {
	cl, r, _, end, err := c.forMessage(ctx, "shim.GetWorkflow")
	if err != nil {
		return nil, err
	}
	defer end()
	return cl.GetWorkflow(ctx, ScopedWorkflowID(r, workflowID), runID), nil
}

// TerminateWorkflow terminates a workflow in the ctx tenant's namespace.
func (c *Client) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error {
	cl, r, ctx, end, err := c.forMessage(ctx, "shim.TerminateWorkflow")
	if err != nil {
		return err
	}
	defer end()
	return cl.TerminateWorkflow(ctx, ScopedWorkflowID(r, workflowID), runID, reason)
}

func (c *Client) forMessage(ctx context.Context, op string) (client.Client, Routing, context.Context, func(), error) {
	r := RoutingFromContext(ctx)
	if err := r.Validate(); err != nil {
		return nil, r, ctx, nil, err
	}
	if r.IsTest() && r.SBR != "" && r.RID == "" {
		// sbr-only traffic: address the same run a start would have picked.
		route, rctx, err := c.Resolve(ctx)
		if err != nil {
			return nil, r, ctx, nil, err
		}
		r, ctx = route.Routing, rctx
	}
	ns := c.cfg.ProdNamespace
	if r.IsTest() {
		ns = c.cfg.SandboxNamespace
	}
	ctx, end := ensureSpan(ctx, c.tracer, op)
	return c.Namespace(ns), r, ctx, end, nil
}

// ScopedWorkflowID keeps business workflow IDs from colliding across
// concurrent test runs that share the sandbox namespace: for dashtest traffic
// carrying a rid, "order-42" becomes "order-42@<rid>". Idempotent.
func ScopedWorkflowID(r Routing, id string) string {
	if id == "" || !r.IsTest() || r.RID == "" {
		return id
	}
	suffix := "@" + r.RID
	if strings.HasSuffix(id, suffix) {
		return id
	}
	return id + suffix
}

func withRoutingSA(sa temporal.SearchAttributes, r Routing) temporal.SearchAttributes {
	updates := []temporal.SearchAttributeUpdate{sa.Copy(), SAPtid.ValueSet(r.PTID)}
	if r.SBR != "" {
		updates = append(updates, SASbr.ValueSet(r.SBR))
	}
	if r.RID != "" {
		updates = append(updates, SARid.ValueSet(r.RID))
	}
	return temporal.NewSearchAttributes(updates...)
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}
