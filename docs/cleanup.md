A crashed test run is cleaned up by the **sandbox registry workflow**, not by the run itself. Its lease expires, the registry terminates whatever is still running on the run's queue, and the empty queue is then unloaded by the server.

### What happens, step by step (default `LeaseTTL` = 30s)

1. **Renewals stop.** A live run renews its lease every TTL/3 (10s). After the crash nothing renews it, so its `ExpiresAt` stays at the time of the last successful renewal plus 30s. In practice that is about 20–30 seconds after the crash.

2. **New traffic stops reaching it.** Routers drop leases whose `ExpiresAt` has passed by their own clock (`Client.Leases` in [registry.go](pkg/shim/registry.go)). Once that time passes, `dashtest` traffic for that `sbr` goes to the production workers in the `sandbox` namespace instead of the dead queue. This happens even before the registry has done anything.

3. **The registry wakes up.** `SandboxRegistryWorkflow` sleeps on a durable timer set for the earliest lease expiry + 1s. When it fires, the workflow:
   - deletes the expired lease and rewrites its memo, so readers see it gone;
   - runs the **`ReapSandboxRun`** activity for that lease.

4. **Leftover workflows are terminated.** `ReapSandboxRun` calls `terminateTaskQueueWorkflows`, the same helper a clean `Close` uses. It lists `TaskQueue = 'sbx.<sbr>.<rid>' AND ExecutionStatus = 'Running'` in the `sandbox` namespace and terminates every match. The reason recorded is `sandbox run lease expired (owner gone)`.
   - It makes up to 5 listing passes, 300ms apart, because visibility is eventually consistent and child workflows can appear after their parent.
   - The activity has a 1-minute timeout and up to 5 attempts.
   - Terminating a workflow also discards its pending workflow and activity tasks, so nothing is left in the queue.

5. **The task queue goes idle.** It now has no pollers and no backlog. Temporal has no "delete task queue" API, because a task queue is just a name. The server unloads an idle queue by itself. `temporal task-queue describe` keeps showing the dead worker as a last-seen poller for a few minutes; that entry is only a record and ages out.

The end state is the same as a clean `TestRun.Close`: no lease, no running workflows on the queue, no pollers.

### Timing

| Event | Time after the crash |
|---|---|
| Lease expires; routers stop sending traffic to the run | ~20–30s |
| Leftover workflows terminated | about 1s after that |

`TestCrashedRunIsReaped` checks this end to end, with a 3-second TTL. `TestRun.Abandon()` simulates the crash: it stops the worker without deregistering or terminating anything.

### Things that can delay or break this

- **A registry worker must be running.** The timer and `ReapSandboxRun` both need a worker on `sandbox`/`shim-sandbox-registry`. That worker lives in `prodworker` and in every live test run. If none is running, cleanup waits; the durable timer fires as soon as one comes back.
- **If reaping fails after all retries, workflows can be left running.** The registry logs the error and moves on, and the lease is already deleted. Workflows still running on that queue then stay stuck with no poller, and nothing retries the cleanup. A periodic sweep of `sbx.*` queues with no live lease would close this gap; it isn't built.
- **A run that is alive but unreachable gets reaped too.** If a run can't reach Temporal for longer than the TTL, the registry treats it as dead and terminates its workflows. A longer `Config.LeaseTTL` protects slow networks; a shorter one cleans up crashes faster.
- **Completed workflows are not deleted.** Workflows that already finished stay in history until the namespace's retention period removes them. Only running ones are terminated.