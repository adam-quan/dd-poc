`client.go` is the **router**. `shim.Client` wraps two Temporal SDK clients, one for `production` and one for `sandbox`, over a single gRPC connection. Every method reads `ptid`/`sbr`/`rid` from the ctx baggage and decides where the call goes. Callers never pick a namespace or task queue themselves.

## Setup

- **`Dial`** ([L41-77](pkg/shim/client.go#L41-L77)):
  - Builds the OTel `TracerProvider` and the Temporal tracing interceptor. The interceptor is what carries baggage in the `_tracer-data` header.
  - Dials the prod namespace, then derives the sandbox client with `NewClientFromExisting`, which shares the connection.
- **`ForService`**: a shallow copy that routes for a different service on the same connection. It is marked `owner=false`, so calling `Close` on the copy is a no-op and doesn't close the shared connection.
- **`Namespace(ns)`**: an escape hatch that returns the raw SDK client with no routing applied. Any name other than the sandbox namespace returns the **prod** client, so a typo silently gets you prod.

## `Resolve`: the routing decision

[L122-157](pkg/shim/client.go#L122-L157). It first validates the baggage. `Validate` ([routing.go:63](pkg/shim/routing.go#L63)) rejects a missing or unknown `ptid` and a malformed `sbr`/`rid`, so there is no default tenant. Then the first matching case wins:

| Baggage | Namespace | Task queue |
|---|---|---|
| `ptid=dashprod` | production | `<service>` |
| `dashtest`, no `sbr` | sandbox | `<service>` (production build, fallback pool) |
| `dashtest`, `sbr` for a **different** service | sandbox | `<service>` |
| `dashtest`, `sbr` matches a live lease | sandbox | the lease's `sbx.<sbr>.<rid>` |
| `dashtest`, `sbr` with no lease, **or the registry lookup errored** | sandbox | `<service>` |

Two details:

- **Fail open.** If the registry can't be read, test traffic still runs, on the fallback production workers. It only logs a warning.
- **`rid` fill-in.** With `sbr` but no `rid`, the newest lease wins (see `lookupLease` in `registry.go`). Its `rid` is written back into the baggage on the returned ctx, so everything downstream (headers, workflow-side guard, child IDs) sees the full `sbr`+`rid`. Without that, the sandbox worker's guard would refuse the task.

## Starting workflows: `ExecuteWorkflow`

[L161-179](pkg/shim/client.go#L161-L179). After `Resolve`, it:

1. Makes sure a span exists (`ensureSpan`). Without one, the tracing interceptor drops the baggage, and with it all routing.
2. **Overrides** `opts.TaskQueue` with the routed queue. If the caller passed something different, that only shows up as a debug log.
3. Scopes the ID with `ScopedWorkflowID`, so `order-42` becomes `order-42@<rid>` for `dashtest` traffic that carries a `rid`. Concurrent runs in the shared sandbox namespace then can't collide on business IDs. The function is idempotent: an ID that already ends in `@<rid>` is left alone.
4. Stamps the `Ptid`/`Sbr`/`Rid` search attributes, unless they are disabled.
5. Sends the call on the chosen namespace's client.

## Messages: signal, query, update, get, terminate

All five go through **`forMessage`** ([L232-251](pkg/shim/client.go#L232-L251)):

- For `sbr`-only test traffic, it runs `Resolve` to fill in `rid`, so the message reaches the same run a start would have picked.
- The namespace comes from `ptid` alone.
- It makes sure a span exists, so the message's own baggage travels with it. That's what the workflow side reads with `SignalRouting`/`UpdateRouting`/`QueryRouting`.

Each method then applies `ScopedWorkflowID` to the target ID.

## Things worth knowing

- **`sbr`-only addressing is resolved separately on every call.** A workflow started with `sbr` only and no live lease gets the unscoped ID `order-1`. If a sandbox registers before you signal it, the signal resolves a `rid` and targets `order-1@<rid>`, which gets NotFound. The reverse also happens: when a lease expires between a start and a signal, the signal loses its `rid`. Sending an explicit `rid` avoids both.
- **No lease caching.** Every start, and every `sbr`-only message, does a `DescribeWorkflowExecution` on the registry, with a 5s timeout. That's fine for a POC, but it is one extra round trip per call.
- **`GetWorkflow` throws away the resolved ctx** (`_` at [L214](pkg/shim/client.go#L214)) and ends its span right away. That's harmless today, because only the scoped ID matters there and later `run.Get` calls don't need baggage.