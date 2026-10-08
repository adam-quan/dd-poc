package shim

import (
	"context"
	"fmt"
	"sort"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// The sandbox registry is itself a Temporal workflow in the sandbox
// namespace. It holds one lease per active test run (sbr+rid -> task queue).
// Test runs register, renew and deregister through updates. The workflow
// mirrors its leases into its memo, which routers read with
// DescribeWorkflowExecution (no worker round trip). Expired leases (crashed
// runs) are reaped by the workflow, which terminates whatever the dead run
// left behind.
const (
	RegistryWorkflowID = "shim-sandbox-registry"
	RegistryTaskQueue  = "shim-sandbox-registry"

	registryUpdateRegister   = "register"
	registryUpdateDeregister = "deregister"
	registryQueryLeases      = "leases"
	registryMemoLeases       = "leases"
)

// Lease is a routing entry for one test run.
type Lease struct {
	Service      string        `json:"service"`
	SBR          string        `json:"sbr"`
	RID          string        `json:"rid"`
	TaskQueue    string        `json:"taskQueue"`
	Owner        string        `json:"owner"`
	TTL          time.Duration `json:"ttl"`
	RegisteredAt time.Time     `json:"registeredAt"`
	ExpiresAt    time.Time     `json:"expiresAt"`
}

func (l Lease) key() string { return l.SBR + "|" + l.RID }

// RegistryState is carried across continue-as-new.
type RegistryState struct {
	Leases map[string]Lease `json:"leases"`
}

// SandboxRegistryWorkflow is the long-running registry.
func SandboxRegistryWorkflow(ctx workflow.Context, st RegistryState) error {
	if st.Leases == nil {
		st.Leases = map[string]Lease{}
	}
	logger := workflow.GetLogger(ctx)
	version := 0
	// Leases are mirrored into the memo so routers read them with
	// DescribeWorkflowExecution: no worker round trip, and the memo is
	// persisted in the same workflow task that completes each update.
	publish := func(ctx workflow.Context) error {
		return workflow.UpsertMemo(ctx, map[string]any{registryMemoLeases: sortedLeases(st.Leases)})
	}
	if err := publish(ctx); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandler(ctx, registryUpdateRegister, func(ctx workflow.Context, l Lease) (Lease, error) {
		now := workflow.Now(ctx)
		if prev, ok := st.Leases[l.key()]; ok {
			l.RegisteredAt = prev.RegisteredAt
		} else {
			l.RegisteredAt = now
		}
		l.ExpiresAt = now.Add(l.TTL)
		st.Leases[l.key()] = l
		version++
		return l, publish(ctx)
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, registryUpdateDeregister, func(ctx workflow.Context, l Lease) (bool, error) {
		_, ok := st.Leases[l.key()]
		delete(st.Leases, l.key())
		version++
		return ok, publish(ctx)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, registryQueryLeases, func() ([]Lease, error) {
		return sortedLeases(st.Leases), nil
	}); err != nil {
		return err
	}

	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	for {
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return err
			}
			return workflow.NewContinueAsNewError(ctx, SandboxRegistryWorkflow, st)
		}

		// Sleep until the earliest lease expires or the lease set changes.
		seen := version
		changed := func() bool { return version != seen }
		next := earliestExpiry(st.Leases)
		if next.IsZero() {
			if _, err := workflow.AwaitWithTimeout(ctx, time.Hour, changed); err != nil {
				return err
			}
		} else if wait := next.Sub(workflow.Now(ctx)); wait > 0 {
			if _, err := workflow.AwaitWithTimeout(ctx, wait+time.Second, changed); err != nil {
				return err
			}
		}

		now := workflow.Now(ctx)
		var keys []string
		for k := range st.Leases {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic iteration
		for _, k := range keys {
			l := st.Leases[k]
			if l.ExpiresAt.After(now) {
				continue
			}
			delete(st.Leases, k)
			if err := publish(ctx); err != nil {
				return err
			}
			logger.Warn("sandbox lease expired; reaping run", "sbr", l.SBR, "rid", l.RID, "taskQueue", l.TaskQueue)
			if err := workflow.ExecuteActivity(actx, (*registryActivities).ReapSandboxRun, l).Get(ctx, nil); err != nil {
				logger.Error("reap failed", "err", err, "taskQueue", l.TaskQueue)
			}
		}
	}
}

// sortedLeases orders leases newest first (deterministically).
func sortedLeases(m map[string]Lease) []Lease {
	out := make([]Lease, 0, len(m))
	for _, l := range m {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].RegisteredAt.Equal(out[j].RegisteredAt) {
			return out[i].RegisteredAt.After(out[j].RegisteredAt)
		}
		return out[i].key() < out[j].key()
	})
	return out
}

func earliestExpiry(m map[string]Lease) time.Time {
	var t time.Time
	for _, l := range m {
		if t.IsZero() || l.ExpiresAt.Before(t) {
			t = l.ExpiresAt
		}
	}
	return t
}

type registryActivities struct {
	sandbox client.Client
}

// ReapSandboxRun terminates the open workflows of a run whose owner vanished.
func (a *registryActivities) ReapSandboxRun(ctx context.Context, l Lease) (int, error) {
	n, err := terminateTaskQueueWorkflows(ctx, a.sandbox, l.TaskQueue, "sandbox run lease expired (owner gone)")
	activity.GetLogger(ctx).Info("reaped sandbox run", "taskQueue", l.TaskQueue, "terminated", n)
	return n, err
}

// newRegistryWorker hosts the registry workflow. Every process that runs
// sandbox-namespace workers hosts one, so the registry is always servable.
func (c *Client) newRegistryWorker() worker.Worker {
	w := worker.New(c.sandbox, RegistryTaskQueue, worker.Options{Identity: identity("registry")})
	w.RegisterWorkflow(SandboxRegistryWorkflow)
	w.RegisterActivity(&registryActivities{sandbox: c.sandbox})
	return w
}

// registryContext gives registry calls dashtest baggage (they live in the
// sandbox namespace) and a span to carry it.
func (c *Client) registryContext(ctx context.Context) (context.Context, func()) {
	ctx = MustWithRouting(ctx, Routing{PTID: TenantTest})
	return ensureSpan(ctx, c.tracer, "shim.registry")
}

func (c *Client) upsertLease(ctx context.Context, l Lease) error {
	ctx, end := c.registryContext(ctx)
	defer end()
	start := c.sandbox.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       RegistryWorkflowID,
		TaskQueue:                RegistryTaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, SandboxRegistryWorkflow, RegistryState{})
	h, err := c.sandbox.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: client.UpdateWorkflowOptions{
			WorkflowID:   RegistryWorkflowID,
			UpdateName:   registryUpdateRegister,
			Args:         []any{l},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return fmt.Errorf("shim: register lease: %w", err)
	}
	return h.Get(ctx, nil)
}

func (c *Client) removeLease(ctx context.Context, l Lease) error {
	ctx, end := c.registryContext(ctx)
	defer end()
	h, err := c.sandbox.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   RegistryWorkflowID,
		UpdateName:   registryUpdateDeregister,
		Args:         []any{l},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("shim: deregister lease: %w", err)
	}
	return h.Get(ctx, nil)
}

// Leases lists currently valid leases (expired ones are filtered by wall
// clock even before the registry reaps them). It reads the registry's memo,
// so it works even when no registry worker is reachable.
func (c *Client) Leases(ctx context.Context) ([]Lease, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	d, err := c.sandbox.DescribeWorkflowExecution(ctx, RegistryWorkflowID, "")
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var all []Lease
	if p := d.GetWorkflowExecutionInfo().GetMemo().GetFields()[registryMemoLeases]; p != nil {
		if err := converter.GetDefaultDataConverter().FromPayload(p, &all); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	live := all[:0]
	for _, l := range all {
		if l.ExpiresAt.After(now) {
			live = append(live, l)
		}
	}
	return live, nil
}

// lookupLease finds the lease matching sbr (and rid, if present). With sbr
// only, the most recently registered run for that sandbox wins.
func (c *Client) lookupLease(ctx context.Context, r Routing) (Lease, bool, error) {
	leases, err := c.Leases(ctx)
	if err != nil {
		return Lease{}, false, err
	}
	for _, l := range leases { // newest first
		if l.Service == c.cfg.Service && l.SBR == r.SBR && (r.RID == "" || l.RID == r.RID) {
			return l, true, nil
		}
	}
	return Lease{}, false, nil
}

// terminateTaskQueueWorkflows terminates every running workflow on tq. It
// loops a few times because visibility is eventually consistent and children
// may appear after their parent is listed.
func terminateTaskQueueWorkflows(ctx context.Context, c client.Client, tq, reason string) (int, error) {
	total := 0
	query := fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", tq)
	for attempt := 0; attempt < 5; attempt++ {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query, PageSize: 1000})
		if err != nil {
			return total, err
		}
		if len(resp.Executions) == 0 && attempt > 0 {
			return total, nil
		}
		for _, e := range resp.Executions {
			err := c.TerminateWorkflow(ctx, e.Execution.WorkflowId, e.Execution.RunId, reason)
			if err != nil && !isNotFound(err) {
				return total, err
			}
			if err == nil {
				total++
			}
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return total, nil
}
