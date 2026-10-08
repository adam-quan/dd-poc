`registry.go` keeps track of which developer sandbox runs are alive and which task queue each one owns. The registry is itself a Temporal workflow, a single long-running execution with ID `shim-sandbox-registry` in the `sandbox` namespace.

## The data: `Lease`

[registry.go:37-48](pkg/shim/registry.go#L37-L48). A lease means "test run `rid` of sandbox `sbr` (for `service`) is alive and listening on `TaskQueue`, until `ExpiresAt`." Leases are keyed by `sbr|rid`. `RegistryState` is the lease map. It is passed into the workflow, so it survives continue-as-new.

## The workflow: `SandboxRegistryWorkflow`

[registry.go:56-147](pkg/shim/registry.go#L56-L147). It does four things:

1. **Publishes the leases to its memo** (`publish`). Every time the lease set changes, the sorted list goes into the workflow memo through `UpsertMemo`. This is how other processes read it (see "Reading" below).
2. **Two update handlers:**
   - `register` inserts or renews a lease. It keeps the original `RegisteredAt` when the lease already exists and sets `ExpiresAt = now + TTL`. Calling it again is how a run renews its lease.
   - `deregister` deletes a lease and returns whether it existed.

   Both handlers bump `version`, which the main loop uses to notice changes.
3. **A `leases` query handler.** It still exists, but routers don't use it. Per CLAUDE.md, queries stalled for about 5s when the sticky worker was gone.
4. **A reaper loop** ([L104-146](pkg/shim/registry.go#L104-L146)):
   - When the server suggests continue-as-new, it waits for in-flight handlers to finish (`AllHandlersFinished`), then continues as new with the current state.
   - Otherwise it sleeps until whichever comes first: the earliest lease expiry plus 1s, or a change to `version`. With no leases, it wakes at least once an hour, so the continue-as-new check still runs.
   - On waking, it walks the keys in sorted order (map order isn't deterministic, so sorting keeps replay safe). For each expired lease, it removes the lease, republishes the memo, and runs the `ReapSandboxRun` activity. That activity terminates the dead run's leftover workflows.

## Activities and worker

- `ReapSandboxRun` → `terminateTaskQueueWorkflows` ([L290-317](pkg/shim/registry.go#L290-L317)). It lists running workflows with the visibility query `TaskQueue = '<tq>' AND ExecutionStatus = 'Running'` and terminates each one. It repeats the list up to 5 times, 300ms apart, because visibility is eventually consistent and children can show up after their parent has been listed.
- `newRegistryWorker` registers the workflow and the activity on the `shim-sandbox-registry` queue. Every process with sandbox-namespace workers hosts one, so some worker is always available to run the registry.

## Client-side API

- **`upsertLease`** ([L201-223](pkg/shim/registry.go#L201-L223)) uses **update-with-start** with the `USE_EXISTING` conflict policy. The first registrant creates the registry workflow, and later ones send their update to the running one. If someone terminated the registry, the next registration recreates it.
- **`removeLease`** sends a plain update. If the registry doesn't exist, it treats the lease as already gone.
- **`registryContext`** attaches `ptid=dashtest` baggage and a span to every registry call. Per the propagation gotcha, without a span the baggage would be silently dropped.

## Reading: `Leases` and `lookupLease`

- **`Leases`** ([L246-270](pkg/shim/registry.go#L246-L270)) calls `DescribeWorkflowExecution` and decodes the memo. This is a server-side read only, with no worker involved, so it works even when no registry worker is running. It also drops leases that are past `ExpiresAt` by wall clock, which covers the window before the reaper wakes up.
- **`lookupLease`** returns the first matching lease for this service and `sbr`, matching `rid` too when one is given. The list is sorted newest first ([`sortedLeases`](pkg/shim/registry.go#L150-L162)), so traffic that has an `sbr` but no `rid` goes to the most recently registered run.

## Things worth knowing

- **Run lifecycle:** `StartTestRun` (in `worker.go`) calls `upsertLease` once its workers are polling, renews periodically (CLAUDE.md says within TTL/3), and calls `removeLease` on `Close`. A run that crashes stops renewing, its lease expires, and the reaper cleans up after it.
- **Possible bug in the reap loop:** `keys` is captured before the loop, and `ExecuteActivity(...).Get` yields, so update handlers can run in the middle of the loop. If a `deregister` removes a later key during that time, `st.Leases[k]` returns a zero `Lease`. Its zero `ExpiresAt` isn't after `now`, so the loop logs a "lease expired" warning and reaps a lease with `TaskQueue ""`. That's probably harmless, since the query should match nothing, but it's noise. An `if !ok { continue }` after the map lookup would fix it. Because this is workflow code, the change needs `workflow.GetVersion`, or the running registry has to be terminated first (see CLAUDE.md).
- **Renewal after being reaped:** a run that is alive but late renewing loses its lease, and its workflows are terminated. Its next renewal re-registers through `register` with a new `RegisteredAt`, but the terminated workflows don't come back.
- **`version` isn't part of `RegistryState`:** this is fine, because it only exists to wake the loop and it resets after continue-as-new.