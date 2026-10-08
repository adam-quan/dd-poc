This file wires a Temporal worker to a routing policy and then manages a sandbox lifetime with lease renewal and cleanup.

## How this code fits together

This file is the worker orchestration layer for the shim. It creates Temporal workers, attaches a guard that enforces routing, and then manages a per-developer sandbox run with leasing and cleanup.

The routing model itself is defined in `routing.go:47-83` and enforced by the worker guard in `interceptor.go:17-59`. The key idea is: every request carries baggage like `ptid`, `sbr`, and `rid`, and workers only admit traffic that matches their allowed route.

---

## 1) Production workers

In `worker.go:17-52`, `NewProductionWorkers` builds three workers:

- one in the production namespace
- one in the sandbox namespace as a fallback for test traffic
- one registry worker used for tracking sandbox leases

Each worker gets:

- a unique identity like `prod/production/myservice`
- a `workerPolicy` interceptor
- the same registration callback so workflows/activities are attached to the worker

The important bit is this pattern:

- copy `opts`
- assign `o.Identity`
- create a `workerPolicy`
- prepend `p.interceptor()` to the worker’s interceptors
- create the worker and register code

That means every production worker runs under a route guard before user code executes.

---

## 2) The route guard

The guard is not just a queue policy; it is a defense-in-depth check.

In `interceptor.go:32-59`, `workerPolicy.admit` says:

- `roleProduction` in the prod namespace accepts only `dashprod`
- `roleProduction` in the sandbox namespace accepts only `dashtest` fallback traffic
- `roleSandbox` accepts only a specific `sbr` + `rid`
- `roleReplay` accepts everything for replayer checks

So the worker is effectively saying: “I only run traffic for my tenant and my exact run.”

---

## 3) Developer sandbox runs

The more interesting part is `StartTestRun` in `worker.go:88-167`.

This creates one isolated test run:

- builds a sandbox routing key `sbr` if none was provided
- validates `sbr` and `rid`
- computes the per-run task queue with `SandboxTaskQueue(sbr, rid)`
- creates a worker for that queue
- assigns identity like `sandbox/<sbr>/<rid>`
- wraps it with the sandbox guard
- registers the test workflows/activities on that worker

Then it does:

1. start the registry worker
2. start the sandbox worker
3. publish the lease via `upsertLease`
4. start a background goroutine that renews the lease periodically

This is how the system tells the rest of the cluster: “this sandbox run is active and owns this queue.”

---

## 4) Lease renewal and lifecycle

The `TestRun` struct in `worker.go:74-86` stores:

- the routing metadata
- the queue name
- the active lease
- the worker and registry worker
- renewal cancelation and cleanup state

The `renew` method calls `upsertLease` every TTL/3 interval. If the process dies unexpectedly, the lease expires and the registry reaps the sandbox run. That is why there’s also `Abandon`, which stops the worker without cleanup and leaves expiration-driven cleanup to the registry.

---

## 5) Close sequence

`Close` in `worker.go:179-204` does the shutdown sequence in the right order:

1. stop renewal
2. remove the lease
3. terminate any leftover workflows still on that task queue
4. stop the sandbox worker
5. stop the registry worker

This matters because the system wants to avoid new traffic before draining leftovers, and then unload the queue once nothing is left.

---

## In one sentence

This file is the “route-aware worker manager” for the system: it creates production and sandbox workers, enforces that they only serve allowed traffic, and manages the lifecycle of a developer sandbox run from startup through renewal to cleanup.

If you want, I can also walk through the exact flow for a single request from `Context` creation to worker admission.