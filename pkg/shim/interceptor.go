package shim

import (
	"context"
	"fmt"
	"os"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// workerRole describes which traffic a worker may execute.
type workerRole int

const (
	// roleProduction: the production build of the service, polling the base
	// task queue. In the prod namespace it only admits dashprod; in the
	// sandbox namespace it only admits dashtest (the "no matching sbr"
	// fallback).
	roleProduction workerRole = iota
	// roleSandbox: one developer's test run. Admits only dashtest traffic
	// carrying exactly its sbr+rid.
	roleSandbox
	// roleReplay: replayer for determinism checks; admits everything.
	roleReplay
)

// workerPolicy is the admission guard installed on every shim worker. Task
// queues already isolate traffic; the guard is defense in depth against a
// misrouted start (e.g. via a raw client) ever executing user code.
type workerPolicy struct {
	role       workerRole
	namespace  string
	prodNS     string
	expect     Routing // for roleSandbox
	identity   string
	searchAttr bool
}

func (p *workerPolicy) admit(r Routing) error {
	switch {
	case p.role == roleReplay:
		return nil
	case p.role == roleProduction && p.namespace == p.prodNS:
		if !r.IsProd() {
			return fmt.Errorf("production worker %s refuses non-production traffic (%s)", p.identity, r)
		}
	case p.role == roleProduction:
		if !r.IsTest() {
			return fmt.Errorf("sandbox-namespace production worker %s refuses %s", p.identity, r)
		}
	case p.role == roleSandbox:
		if !r.IsTest() || r.SBR != p.expect.SBR || r.RID != p.expect.RID {
			return fmt.Errorf("sandbox worker %s (sbr=%s rid=%s) refuses %s", p.identity, p.expect.SBR, p.expect.RID, r)
		}
	}
	return nil
}

func (p *workerPolicy) interceptor() interceptor.WorkerInterceptor { return &guardInterceptor{p: p} }

type guardInterceptor struct {
	interceptor.WorkerInterceptorBase
	p *workerPolicy
}

func (g *guardInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	i := &guardActivityInbound{p: g.p}
	i.Next = next
	return i
}

func (g *guardInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	i := &guardWorkflowInbound{p: g.p, st: &workflowState{signals: map[string]Routing{}}}
	i.Next = next
	return i
}

// ---- activities -------------------------------------------------------------

type workerIdentityKey struct{}

type guardActivityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	p *workerPolicy
}

func (a *guardActivityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	r := routingFromHeader(interceptor.Header(ctx))
	if err := a.p.admit(r); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "ShimRoutingViolation", nil)
	}
	ctx = context.WithValue(ctx, workerIdentityKey{}, a.p.identity)
	return a.Next.ExecuteActivity(ctx, in)
}

// WorkerIdentity returns the identity of the shim worker running the current
// activity.
func WorkerIdentity(ctx context.Context) string {
	s, _ := ctx.Value(workerIdentityKey{}).(string)
	return s
}

// ---- workflows --------------------------------------------------------------

type workflowStateKey struct{}
type messageRoutingKey struct{}

// workflowState is per workflow execution. It is populated only from history
// (headers), so it is identical on replay.
type workflowState struct {
	routing   Routing
	signals   map[string]Routing
	lastQuery Routing
}

type guardWorkflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	p  *workerPolicy
	st *workflowState
}

func (w *guardWorkflowInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	o := &guardWorkflowOutbound{p: w.p, st: w.st}
	o.Next = outbound
	return w.Next.Init(o)
}

func (w *guardWorkflowInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	r := routingFromHeader(interceptor.WorkflowHeader(ctx))
	if err := w.p.admit(r); err != nil {
		// Panicking fails the workflow task: the server keeps the workflow but
		// no user code ever runs on the wrong worker.
		panic("shim routing violation: " + err.Error())
	}
	w.st.routing = r
	ctx = workflow.WithValue(ctx, workflowStateKey{}, w.st)
	return w.Next.ExecuteWorkflow(ctx, in)
}

func (w *guardWorkflowInbound) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) error {
	r := routingFromHeader(interceptor.WorkflowHeader(ctx))
	if r.PTID != "" && r.PTID != w.st.routing.PTID {
		workflow.GetLogger(ctx).Warn("cross-tenant signal", "signal", in.SignalName, "from", r.String(), "workflow", w.st.routing.String())
	}
	w.st.signals[in.SignalName] = r
	return w.Next.HandleSignal(ctx, in)
}

func (w *guardWorkflowInbound) HandleQuery(ctx workflow.Context, in *interceptor.HandleQueryInput) (any, error) {
	w.st.lastQuery = routingFromHeader(interceptor.WorkflowHeader(ctx))
	return w.Next.HandleQuery(ctx, in)
}

func (w *guardWorkflowInbound) ValidateUpdate(ctx workflow.Context, in *interceptor.UpdateInput) error {
	return w.Next.ValidateUpdate(withMessageRouting(ctx), in)
}

func (w *guardWorkflowInbound) ExecuteUpdate(ctx workflow.Context, in *interceptor.UpdateInput) (any, error) {
	return w.Next.ExecuteUpdate(withMessageRouting(ctx), in)
}

func withMessageRouting(ctx workflow.Context) workflow.Context {
	return workflow.WithValue(ctx, messageRoutingKey{}, routingFromHeader(interceptor.WorkflowHeader(ctx)))
}

type guardWorkflowOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	p  *workerPolicy
	st *workflowState
}

// ExecuteActivity pins activities to the workflow's own task queue on sandbox
// workers, so a developer's activities never land on shared workers.
func (o *guardWorkflowOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if o.p.role == roleSandbox {
		opts := workflow.GetActivityOptions(ctx)
		own := workflow.GetInfo(ctx).TaskQueueName
		if opts.TaskQueue != "" && opts.TaskQueue != own {
			workflow.GetLogger(ctx).Warn("pinning activity to sandbox task queue", "requested", opts.TaskQueue, "pinned", own)
			opts.TaskQueue = own
			ctx = workflow.WithActivityOptions(ctx, opts)
		}
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

// ExecuteChildWorkflow keeps children on the parent's task queue for sandbox
// workers, scopes explicit child IDs per run and stamps routing search
// attributes.
func (o *guardWorkflowOutbound) ExecuteChildWorkflow(ctx workflow.Context, childWorkflowType string, args ...any) workflow.ChildWorkflowFuture {
	opts := workflow.GetChildWorkflowOptions(ctx)
	if o.p.role == roleSandbox {
		own := workflow.GetInfo(ctx).TaskQueueName
		if opts.TaskQueue != "" && opts.TaskQueue != own {
			workflow.GetLogger(ctx).Warn("pinning child workflow to sandbox task queue", "requested", opts.TaskQueue, "pinned", own)
		}
		opts.TaskQueue = own
	}
	opts.WorkflowID = ScopedWorkflowID(o.st.routing, opts.WorkflowID)
	if o.p.searchAttr {
		opts.TypedSearchAttributes = withRoutingSA(opts.TypedSearchAttributes, o.st.routing)
	}
	return o.Next.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, opts), childWorkflowType, args...)
}

func (o *guardWorkflowOutbound) SignalExternalWorkflow(ctx workflow.Context, workflowID, runID, signalName string, arg any) workflow.Future {
	return o.Next.SignalExternalWorkflow(ctx, ScopedWorkflowID(o.st.routing, workflowID), runID, signalName, arg)
}

// ---- workflow-side accessors ------------------------------------------------

func stateFrom(ctx workflow.Context) *workflowState {
	st, _ := ctx.Value(workflowStateKey{}).(*workflowState)
	if st == nil {
		return &workflowState{signals: map[string]Routing{}}
	}
	return st
}

// RoutingFromWorkflow returns the baggage the workflow was started (or
// continued-as-new) with.
func RoutingFromWorkflow(ctx workflow.Context) Routing { return stateFrom(ctx).routing }

// UpdateRouting returns the baggage carried by the update being handled. Use
// it with the ctx passed to an update handler or validator.
func UpdateRouting(ctx workflow.Context) Routing {
	r, _ := ctx.Value(messageRoutingKey{}).(Routing)
	return r
}

// SignalRouting returns the baggage carried by the most recent signal of the
// given name.
func SignalRouting(ctx workflow.Context, signalName string) Routing {
	return stateFrom(ctx).signals[signalName]
}

// QueryRouting returns the baggage carried by the query currently being
// answered. Call it from a query handler with the workflow's ctx.
func QueryRouting(ctx workflow.Context) Routing { return stateFrom(ctx).lastQuery }

func identity(role string) string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s@%s:%d", role, host, os.Getpid())
}
