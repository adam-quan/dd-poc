The shim is a thin layer over the Temporal Go SDK. It lets many developers test against **one shared Temporal cluster** without their test traffic reaching production workers, and without stepping on each other. The only input is OpenTelemetry baggage that already flows with every request. The shim turns that baggage into routing decisions at each hop.

## The three baggage keys

| Key | Example | Decides |
|---|---|---|
| `ptid` | `dashprod` / `dashtest` | **Namespace**: `production` or `sandbox`. Required; a request without it is rejected. |
| `sbr` | `order-web-sandbox-alice` | **Which developer sandbox** (`<service>-<app>-sandbox-<name>`) |
| `rid` | `r20261007t171503-9f2c4e1a` | **Which test run** of that sandbox. Together with `sbr` it names the task queue `sbx.<sbr>.<rid>` and the workflow ID suffix `@<rid>`. |

## How a request flows

```
test code: run.Context(ctx)  ── adds baggage ptid=dashtest, sbr, rid
   │
   ▼
shim.Client.ExecuteWorkflow (client.go)
   ├─ Resolve: ptid → namespace; sbr(+rid) → registry lease → task queue sbx.<sbr>.<rid>
   ├─ scope the ID: order-42 → order-42@<rid>; stamp Ptid/Sbr/Rid search attributes
   └─ ensureSpan: the OTel interceptor writes baggage into the _tracer-data header
   │
   ▼
Temporal server ── task on sbx.<sbr>.<rid>, in the sandbox namespace
   │
   ▼
sandbox worker (worker.go) ── guard interceptor (interceptor.go)
   ├─ admit: dashtest with exactly this sbr+rid? Otherwise panic or fail non-retryably
   ├─ record the routing for RoutingFromWorkflow / SignalRouting / ...
   └─ outbound: activities and children pinned to the run's queue; child/external IDs scoped
   │
   ▼
activity calls gRPC / Kafka ── shimgrpc / shimkafka carry the same baggage
   └─ the downstream service uses its own shim.Client → the routing decision runs again
```

Routing is never stored centrally per request. Every hop recomputes it from the baggage. The registry only answers one question: "is a run of this `sbr` alive, and on which queue?"

## File map

| File | Role |
|---|---|
| [routing.go](pkg/shim/routing.go) | The `Routing` type: reads and writes the baggage keys, validates them (regexes), builds SBR, run ID and task queue names |
| [propagation.go](pkg/shim/propagation.go) | One W3C propagator for every transport, plus the tracer provider, `ensureSpan` and `routingFromHeader` |
| [client.go](pkg/shim/client.go) | The router: `Resolve`, `ExecuteWorkflow`, the message methods, `ScopedWorkflowID` (explained in my earlier answer) |
| [interceptor.go](pkg/shim/interceptor.go) | The worker-side guard and the workflow routing accessors (explained earlier) |
| [worker.go](pkg/shim/worker.go) | Production workers and the test-run lifecycle |
| [registry.go](pkg/shim/registry.go) | The lease registry workflow and the reaper (explained earlier) |
| [config.go](pkg/shim/config.go) | Defaults: `127.0.0.1:7233`, `production`/`sandbox`, 30s lease TTL |
| [shimgrpc](pkg/shim/shimgrpc/shimgrpc.go), [shimkafka](pkg/shim/shimkafka/shimkafka.go) | Carry baggage over gRPC metadata and Kafka headers. Kafka also has an in-memory broker. |
| [shimtest](pkg/shim/shimtest/shimtest.go) | `go test` helper: names the sandbox after `$SANDBOX_NAME` or your OS user, and closes the run in `t.Cleanup` |

## The pieces I haven't covered yet

**`propagation.go`.** The Temporal OTel interceptor only writes baggage when the ctx also holds a *valid* span. So the shim installs a real SDK `TracerProvider` with no exporters: spans are valid but go nowhere. It also calls `ensureSpan` before every client call. This is the most fragile part of the design, because removing either one makes routing silently stop working. The default provider also sets the **global** OTel propagator, a process-wide side effect.

**`routing.go`.** `WithRouting` deletes the baggage members for empty fields. For example, `registryContext` sets `ptid=dashtest` and removes `sbr`/`rid`, so a stale `rid` never leaks into registry calls.

**`worker.go`**, two deployment shapes:

- **`NewProductionWorkers`** builds three workers:
  - one on the `<service>` queue in `production` (serves `dashprod`)
  - one on the `<service>` queue in `sandbox` (the fallback for `dashtest` traffic with no live sandbox)
  - a registry worker
- **`StartTestRun`** builds one run's setup:
  - Steps: start the registry worker → start the sandbox worker on `sbx.<sbr>.<rid>` → register the lease. The route only appears once something is polling that queue.
  - A goroutine renews the lease every TTL/3.
  - `Close` reverses the order: stop renewing → withdraw the lease → terminate leftover workflows on the queue → stop the workers.
  - `Abandon` simulates a crash: it skips cleanup and leaves the work to the reaper.
- **`ReplayerOptions`** installs the same interceptors with `roleReplay`, which admits everything, so recorded histories replay under the real interceptor stack.

## Things worth knowing

New ones from these files:

- **A start can slip in during `Close`.** A router that read the memo just before the lease was withdrawn can still start a workflow on the run's queue after the leftovers were terminated. With the lease gone, the reaper never finds that workflow, so it sits on a queue nobody polls. The window is small.
- **`SBRService` only checks a prefix.** `SBRService("order-history-web-sandbox-bob", "order")` returns true. Routing is still safe, because `lookupLease` also matches on `Lease.Service`. But `StartTestRun` for service `order` would accept another service's `sbr`.
- **`shimgrpc` only has unary interceptors.** Streaming RPCs don't carry baggage.
- **`MemoryBroker.Publish` blocks** once any subscriber's buffer of 1024 messages is full.

From earlier:

- **`RequestCancelExternalWorkflow` doesn't scope its target ID** (interceptor.go). Signals to external workflows are scoped; cancels are not.
- **`SignalRouting` returns the latest signal of that name,** not the one being read.
- **The reap loop can reap a zero `Lease`** if a deregister happens mid-loop (registry.go).
- **`sbr`-only addressing can point to a different workflow ID between a start and a later message,** if a lease appears or expires in between (client.go).
- **The registry is read on every start, with no caching.**

[docs/DESIGN.md](docs/DESIGN.md) has the design rationale and diagrams behind all of this.