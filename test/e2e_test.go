// End-to-end tests against a running dev server (scripts/dev-server.sh).
package test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim/shimkafka"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim/shimtest"
	"github.com/temporalio/doordash-sandbox-poc/service/inventory"
	"github.com/temporalio/doordash-sandbox-poc/service/order"
)

type env struct {
	c       *shim.Client
	acts    *order.Activities
	workers *shim.ProductionWorkers

	mu       sync.Mutex
	consumed []order.Hop
}

var (
	envOnce sync.Once
	theEnv  *env
	envErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if theEnv != nil {
		// Graceful stop releases sticky queues (e.g. the registry's) at once.
		theEnv.workers.Stop()
	}
	os.Exit(code)
}

func setup(t *testing.T) *env {
	t.Helper()
	envOnce.Do(func() { theEnv, envErr = newEnv() })
	if envErr != nil {
		t.Skipf("dev server not available (run scripts/dev-server.sh): %v", envErr)
	}
	return theEnv
}

func newEnv() (*env, error) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	c, err := shim.Dial(shim.ConfigFromEnv(order.Service))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Namespace("production").CheckHealth(ctx, nil); err != nil {
		return nil, err
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	inventory.Serve(lis)
	inv, err := inventory.Dial(lis.Addr().String())
	if err != nil {
		return nil, err
	}
	broker := shimkafka.NewMemoryBroker()
	e := &env{c: c, acts: &order.Activities{Inventory: inv, Events: broker}}
	go func() {
		for msg := range broker.Subscribe(order.EventsTopic) {
			h, _ := order.ConsumerHop(msg)
			e.mu.Lock()
			e.consumed = append(e.consumed, h)
			e.mu.Unlock()
		}
	}()

	// The production deployment of order-service (both namespaces).
	e.workers = c.NewProductionWorkers(order.Register(e.acts), worker.Options{})
	if err := e.workers.Start(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *env) consumedFor(r shim.Routing) []order.Hop {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []order.Hop
	for _, h := range e.consumed {
		if h.Routing == r {
			out = append(out, h)
		}
	}
	return out
}

// TestConcurrentDevelopersAndProduction runs production traffic, two
// developers' sandboxes and unrouted test traffic at the same time, all using
// the same business order ID, and checks every hop of every flow.
func TestConcurrentDevelopersAndProduction(t *testing.T) {
	e := setup(t)
	alice := shimtest.StartTestRun(t, e.c, shim.TestRunOptions{App: "web", SandboxName: "alice", Register: order.Register(e.acts)})
	bob := shimtest.StartTestRun(t, e.c, shim.TestRunOptions{App: "web", SandboxName: "bob", Register: order.Register(e.acts)})

	carol := shim.Routing{PTID: shim.TenantTest, SBR: shim.SBR(order.Service, "web", "carol"), RID: shim.NewRunID()}
	cases := []struct {
		name string
		ctx  context.Context
		exp  order.Expectation
	}{
		{"production", shim.ProdContext(context.Background()), order.Expectation{
			Routing: shim.Routing{PTID: shim.TenantProd}, Namespace: "production", TaskQueue: order.Service,
			WorkerPrefix: "prod/production/" + order.Service}},
		{"alice", alice.Context(context.Background()), order.Expectation{
			Routing: alice.Routing(), Namespace: "sandbox", TaskQueue: alice.TaskQueue(), WorkerPrefix: alice.WorkerIdentity()}},
		{"bob", bob.Context(context.Background()), order.Expectation{
			Routing: bob.Routing(), Namespace: "sandbox", TaskQueue: bob.TaskQueue(), WorkerPrefix: bob.WorkerIdentity()}},
		{"carol-no-sandbox", shim.MustWithRouting(context.Background(), carol), order.Expectation{
			Routing: carol, Namespace: "sandbox", TaskQueue: order.Service, WorkerPrefix: "prod/sandbox/" + order.Service}},
		{"test-without-sbr", shim.MustWithRouting(context.Background(), shim.Routing{PTID: shim.TenantTest}), order.Expectation{
			Routing: shim.Routing{PTID: shim.TenantTest}, Namespace: "sandbox", TaskQueue: order.Service,
			WorkerPrefix: "prod/sandbox/" + order.Service}},
	}

	orderID := fmt.Sprintf("42-%d", time.Now().UnixNano())
	reports := make([]order.Report, len(cases))
	var wg sync.WaitGroup
	for i, tc := range cases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(tc.ctx, 60*time.Second)
			defer cancel()
			rep, err := order.RunScenario(ctx, e.c, orderID)
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			reports[i] = rep
		}()
	}
	wg.Wait()

	ids := map[string]string{}
	for i, tc := range cases {
		rep := reports[i]
		if testing.Verbose() {
			order.PrintHops(os.Stdout, tc.name, rep)
		}
		for _, err := range order.Verify(rep, tc.exp) {
			t.Errorf("%s: %v", tc.name, err)
		}
		// Kafka: the consumer saw the same baggage on both events.
		require.Eventually(t, func() bool { return len(e.consumedFor(tc.exp.Routing)) == 2 }, 5*time.Second, 50*time.Millisecond,
			"%s: kafka consumer should observe 2 events with baggage %s, got %v", tc.name, tc.exp.Routing, e.consumedFor(tc.exp.Routing))
		ids[tc.name] = rep.WorkflowID
	}
	// Replay every first-generation history (start, update, activities,
	// child, signal, continue-as-new) with the shim's interceptors: reading
	// routing from history headers must be deterministic.
	for i, tc := range cases {
		ns := tc.exp.Namespace
		replayer, err := worker.NewWorkflowReplayerWithOptions(e.c.ReplayerOptions())
		require.NoError(t, err)
		replayer.RegisterWorkflow(order.OrderWorkflow)
		replayer.RegisterWorkflow(order.PaymentWorkflow)
		err = replayer.ReplayWorkflowExecution(context.Background(), e.c.Namespace(ns).WorkflowService(), nil, ns,
			workflow.Execution{ID: reports[i].WorkflowID, RunID: reports[i].FirstRunID})
		require.NoError(t, err, "%s: replay", tc.name)
	}

	// Same business ID, distinct executions per run.
	require.Equal(t, "order-"+orderID+"@"+alice.RunID(), ids["alice"])
	require.Equal(t, "order-"+orderID+"@"+bob.RunID(), ids["bob"])
	require.Equal(t, "order-"+orderID, ids["production"])
}

// TestGuardRefusesMisroutedWorkflows bypasses the shim router with a raw
// client and shows the workers' guards never execute foreign traffic.
func TestGuardRefusesMisroutedWorkflows(t *testing.T) {
	e := setup(t)
	alice := shimtest.StartTestRun(t, e.c, shim.TestRunOptions{App: "web", SandboxName: "alice", Register: order.Register(e.acts)})
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	cases := []struct {
		name, ns, tq string
		routing      shim.Routing
	}{
		{"prod traffic onto alice's sandbox queue", "sandbox", alice.TaskQueue(), shim.Routing{PTID: shim.TenantProd}},
		{"bob's traffic onto alice's sandbox queue", "sandbox", alice.TaskQueue(),
			shim.Routing{PTID: shim.TenantTest, SBR: shim.SBR(order.Service, "web", "bob"), RID: shim.NewRunID()}},
		{"test traffic onto production workers", "production", order.Service, shim.Routing{PTID: shim.TenantTest}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, span := tracer.Start(shim.MustWithRouting(context.Background(), tc.routing), "raw-start")
			defer span.End()
			raw := e.c.Namespace(tc.ns)
			id := "misrouted-" + shim.NewRunID()
			run, err := raw.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: tc.tq}, order.OrderWorkflow,
				order.OrderInput{OrderID: id})
			require.NoError(t, err)
			defer func() { _ = raw.TerminateWorkflow(context.Background(), id, run.GetRunID(), "test cleanup") }()

			require.Eventually(t, func() bool {
				return hasEvent(t, raw, id, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED)
			}, 15*time.Second, 200*time.Millisecond, "guard should fail the workflow task")
			require.False(t, hasEvent(t, raw, id, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED), "no user code may run")
		})
	}
}

// TestCloseCleansUpRun: closing a run withdraws its route, terminates its
// leftover workflows and stops its worker; later traffic for that sbr falls
// back to the production workers.
func TestCloseCleansUpRun(t *testing.T) {
	e := setup(t)
	run, err := e.c.StartTestRun(context.Background(), shim.TestRunOptions{App: "web", SandboxName: "dave", Register: order.Register(e.acts)})
	require.NoError(t, err)
	ctx := run.Context(context.Background())

	// Leave a workflow blocked on its signal.
	wf, err := e.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-left-open"}, order.OrderWorkflow, order.OrderInput{OrderID: "left-open"})
	require.NoError(t, err)
	require.True(t, hasLease(t, e.c, run.RunID()))
	require.Eventually(t, func() bool {
		return hasEvent(t, e.c.Namespace("sandbox"), wf.GetID(), enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
	},
		15*time.Second, 200*time.Millisecond)

	require.NoError(t, run.Close(context.Background()))

	require.False(t, hasLease(t, e.c, run.RunID()), "lease must be withdrawn")
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, status(t, e.c.Namespace("sandbox"), wf.GetID()))
	route, _, err := e.c.Resolve(ctx)
	require.NoError(t, err)
	require.Equal(t, order.Service, route.TaskQueue, "traffic for a closed run falls back to production workers")
}

// TestCrashedRunIsReaped: a run whose process dies without cleanup loses its
// route when the lease expires, and the registry terminates its leftovers.
func TestCrashedRunIsReaped(t *testing.T) {
	e := setup(t)
	cfg := shim.ConfigFromEnv(order.Service)
	cfg.LeaseTTL = 3 * time.Second
	c, err := shim.Dial(cfg)
	require.NoError(t, err)
	defer c.Close()

	run, err := c.StartTestRun(context.Background(), shim.TestRunOptions{App: "web", SandboxName: "erin", Register: order.Register(e.acts)})
	require.NoError(t, err)
	wf, err := c.ExecuteWorkflow(run.Context(context.Background()), client.StartWorkflowOptions{ID: "order-crash"}, order.OrderWorkflow,
		order.OrderInput{OrderID: "crash"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return hasEvent(t, c.Namespace("sandbox"), wf.GetID(), enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
	},
		15*time.Second, 200*time.Millisecond)

	run.Abandon() // simulate kill -9: no deregistration, no termination

	require.Eventually(t, func() bool {
		return !hasLease(t, c, run.RunID()) &&
			status(t, c.Namespace("sandbox"), wf.GetID()) == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	}, 30*time.Second, 250*time.Millisecond, "registry should reap the crashed run")
}

// ---- helpers ----------------------------------------------------------------

func hasEvent(t *testing.T, c client.Client, id string, et enumspb.EventType) bool {
	t.Helper()
	it := c.GetWorkflowHistory(context.Background(), id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			return false
		}
		if ev.EventType == et {
			return true
		}
	}
	return false
}

func status(t *testing.T, c client.Client, id string) enumspb.WorkflowExecutionStatus {
	t.Helper()
	d, err := c.DescribeWorkflowExecution(context.Background(), id, "")
	require.NoError(t, err)
	return d.WorkflowExecutionInfo.Status
}

func hasLease(t *testing.T, c *shim.Client, rid string) bool {
	t.Helper()
	leases, err := c.Leases(context.Background())
	require.NoError(t, err)
	for _, l := range leases {
		if l.RID == rid {
			return true
		}
	}
	return false
}
