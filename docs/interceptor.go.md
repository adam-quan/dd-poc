`interceptor.go` is the worker-side half of routing. It installs one Temporal worker interceptor (`guardInterceptor`) on every shim worker. That interceptor:

1. **Admits or refuses** each workflow and activity task based on the worker's role and the task's baggage.
2. **Records the routing** from inbound headers, so workflow code can read it.
3. **Rewrites outbound calls** (activities, children, external signals) so sandbox traffic stays on the run's own queue and its own scoped IDs.

The routing always comes from `routingFromHeader` ([propagation.go:67](pkg/shim/propagation.go#L67)). That function decodes W3C baggage from the `_tracer-data` Temporal header. Headers are recorded in history, so everything here gives the same answer on replay.

## Roles and admission

[interceptor.go:14-59](pkg/shim/interceptor.go#L14-L59). `workerPolicy.admit` is the whole policy:

| Worker | Built in | Admits |
|---|---|---|
| `roleProduction` in the prod namespace | [worker.go:37](pkg/shim/worker.go#L37) | only `ptid=dashprod` |
| `roleProduction` in the sandbox namespace (fallback pool) | same | any `ptid=dashtest` |
| `roleSandbox` (one developer's test run) | [worker.go:139](pkg/shim/worker.go#L139) | `dashtest` with exactly its own `sbr` **and** `rid` |
| `roleReplay` | [worker.go:73](pkg/shim/worker.go#L73) | everything |

Task queues already keep this traffic apart. The guard is a second line of defense, for example against a start sent through a raw client that bypasses `shim.Client`. One consequence: a start with **no baggage at all** has an empty `PTID`, so production workers refuse it too.

## Inbound activities

[L89-96](pkg/shim/interceptor.go#L89-L96). A refused activity fails with a **non-retryable** `ShimRoutingViolation` error, so retrying won't run it on the wrong worker again. An admitted activity gets the worker's identity put on its context, which `WorkerIdentity(ctx)` returns. Tests use that to check which worker ran an activity.

## Inbound workflows

- **`ExecuteWorkflow`** ([L130-140](pkg/shim/interceptor.go#L130-L140)): a refused workflow **panics** before any user code runs. Under the Go SDK's default panic policy (block the workflow), the workflow task fails and keeps being retried, so the workflow stays open but makes no progress. An admitted workflow stores its routing in a per-execution `workflowState` and puts that state on the workflow context.
- **`HandleSignal`**: records the routing of the latest signal under each signal name. If the signal's `ptid` differs from the workflow's own, it logs a cross-tenant warning but still delivers the signal.
- **`HandleQuery`**: stashes the query's routing in `lastQuery`.
- **`ValidateUpdate` / `ExecuteUpdate`**: put the update's routing on the handler's context.

## Outbound calls from workflows

[L168-210](pkg/shim/interceptor.go#L168-L210).

- **Activities:** on sandbox workers, an explicit `TaskQueue` that isn't the workflow's own is rewritten to the own queue, with a warning. A developer's activities therefore never land on the shared production workers.
- **Child workflows:**
  - On sandbox workers, the child's task queue is forced to the parent's queue.
  - On every role, the child's ID goes through `ScopedWorkflowID` ([client.go:256](pkg/shim/client.go#L256)), which appends `@<rid>` for `dashtest` traffic that has a `rid`.
  - When search attributes are enabled, the child is stamped with `Ptid`/`Sbr`/`Rid`.
- **`SignalExternalWorkflow`:** the target ID is scoped the same way, so `"order-1"` resolves to this run's `"order-1@<rid>"`.

## Accessors for workflow code

[L214-241](pkg/shim/interceptor.go#L214-L241):

- `RoutingFromWorkflow(ctx)`: the baggage the workflow was started with.
- `UpdateRouting(ctx)`: the current update's baggage. You must pass the handler's own ctx.
- `SignalRouting(ctx, name)`: the baggage of the **most recent** signal with that name.
- `QueryRouting(ctx)`: the baggage of the query currently being answered.

## Gaps I noticed

- **`RequestCancelExternalWorkflow` is not intercepted.** Only signals to external workflows get scoped IDs. A sandbox workflow that cancels `"order-1"` would target the unscoped ID, which is another run's workflow or a production-style one. That contradicts CLAUDE.md's claim that external IDs are scoped. Adding an override like `SignalExternalWorkflow`'s would fix it.
- **`SignalRouting` returns the latest signal, not the one being read.** If several signals of one name are buffered on a channel before the workflow reads them, every read sees the newest signal's routing. Getting exact per-message routing would mean carrying the routing in the signal payload.
- **`stateFrom` silently returns empty routing** when called on a context the interceptor didn't set up. This happens in workflows registered on a non-shim worker, and on the bare ctx inside an update handler if you call `UpdateRouting` there. Nothing fails, the routing just reads as empty.