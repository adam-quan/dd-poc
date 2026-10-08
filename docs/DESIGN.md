# Design: baggage-routed sandboxes on a shared Temporal cluster

| | |
|---|---|
| Status | POC implemented ([adam-quan/dd-poc](https://github.com/adam-quan/dd-poc)) |
| Scope | Temporal Go SDK, one cluster, `production` + `sandbox` namespaces |
| Companion | [README](../README.md): quick start, demo walkthrough, registry operations |

This document covers the *why* and the *shape* of the system. The README covers how to run it and the
registry's operational details.

---

## 1. Problem

Developers need to test changes to a service against realistic traffic without a private cluster each.
Several developers may test **the same service at the same time**. Neither they nor production traffic may
see each other's work.

Requests already carry OpenTelemetry baggage set at the edge:

| key | values | meaning |
|---|---|---|
| `ptid` | `dashprod` / `dashtest` | tenant: production or test traffic |
| `sbr` | `<service>-<app>-sandbox-<name>` | which developer sandbox of which service |
| `rid` | generated per test run | which run of that sandbox |

## 2. Goals and non-goals

**Goals**
- G1. Production and test traffic share one cluster but run in separate namespaces.
- G2. Routing is decided only by baggage. Callers never choose a namespace or task queue.
- G3. Baggage survives every Temporal boundary (start, activity, child, signal, update, query,
  continue-as-new) and leaves through gRPC and Kafka.
- G4. Each test run gets its own task queue and workers. These are created and destroyed automatically,
  including when the developer's process crashes.
- G5. Production work never executes on sandbox workers, and sandbox work never executes on production
  workers.
- G6. Test traffic with no live sandbox still gets served, by the production build.
- G7. Concurrent developers, and concurrent runs of the same developer, are fully isolated.

**Non-goals**
- Isolating data stores, caches or third-party side effects. Baggage is forwarded so those layers *can*
  isolate; doing so is their job.
- Cross-service routing inside a workflow. This is sketched in §10 but not built.
- Multi-cluster or Temporal Cloud specifics, beyond the notes in §10.

## 3. Architecture overview

```mermaid
flowchart TB
  subgraph callers["Callers"]
    edge["Edge / upstream service<br/>(sets ptid, maybe sbr)"]
    dev["Developer test<br/>(go test / devtest)"]
  end

  subgraph shimlib["shim library (in every process)"]
    client["shim.Client<br/>Resolve → namespace + task queue"]
    tr["TestRun<br/>rid · worker · lease"]
  end

  subgraph cluster["Temporal cluster (single node)"]
    subgraph prod["namespace: production"]
      pq[["order-service"]]
    end
    subgraph sbx["namespace: sandbox"]
      fq[["order-service<br/>fallback"]]
      aq[["sbx.…-alice.&lt;rid&gt;"]]
      bq[["sbx.…-bob.&lt;rid&gt;"]]
      reg[("shim-sandbox-registry<br/>workflow + memo")]
    end
  end

  subgraph workers["Workers"]
    pw["prodworker<br/>(production build)"]
    aw["alice's sandbox worker"]
    bw["bob's sandbox worker"]
  end

  subgraph downstream["Downstream"]
    grpc["gRPC services"]
    kafka["Kafka topics"]
  end

  edge --> client
  dev --> tr --> client
  client -- dashprod --> pq
  client -- "dashtest, no live sandbox" --> fq
  client -- "dashtest + live lease" --> aq
  client -. read leases .-> reg
  tr -- "register / renew / deregister" --> reg

  pw --> pq
  pw --> fq
  pw -. hosts .-> reg
  aw --> aq
  bw --> bq

  pw & aw & bw -- "baggage in metadata / headers" --> grpc & kafka
```

There are three parts:
1. **Router (`shim.Client`):** reads baggage and resolves a start to a `(namespace, task queue)` pair.
2. **Run lifecycle (`TestRun`):** gives each test run a fresh `rid`, a queue, a guarded worker and a lease.
3. **Registry:** one durable workflow that records live runs, so the router can tell "sandbox is up" from
   "fall back to production".

## 4. Topology: namespaces, task queues, workers

```mermaid
flowchart LR
  subgraph production["namespace: production"]
    P1[["order-service"]]
  end
  subgraph sandbox["namespace: sandbox"]
    S0[["order-service"]]
    S1[["sbx.order-service-web-sandbox-alice.r…a"]]
    S2[["sbx.order-service-web-sandbox-bob.r…b"]]
    S3[["sbx.order-service-web-sandbox-alice.r…c"]]
    R[["shim-sandbox-registry"]]
  end

  PW["prodworker<br/>guard: production ns → dashprod only<br/>sandbox ns → dashtest only"]
  A1["alice run a<br/>guard: sbr=alice ∧ rid=a"]
  A2["alice run c<br/>guard: sbr=alice ∧ rid=c"]
  B1["bob run b<br/>guard: sbr=bob ∧ rid=b"]

  PW --> P1 & S0 & R
  A1 --> S1
  A1 -.-> R
  B1 --> S2
  B1 -.-> R
  A2 --> S3
  A2 -.-> R
```

| Queue | Namespace | Polled by | Receives |
|---|---|---|---|
| `<service>` | production | production build | all `dashprod` traffic |
| `<service>` | sandbox | production build | `dashtest` traffic with no live sandbox |
| `sbx.<sbr>.<rid>` | sandbox | that run's worker only | `dashtest` traffic matching that lease |
| `shim-sandbox-registry` | sandbox | production build + every test run | registry workflow and its reaper activity |

**Why one queue per run, not one per sandbox.** A developer may run two test sessions in parallel, for
example two terminals or CI next to a laptop. With one queue per `sbr`, the runs would steal each other's tasks.
Keying the queue on `sbr` + `rid` makes every run hermetic. It also makes cleanup trivial: everything on the
queue belongs to exactly one run.

## 5. Baggage propagation

### 5.1 Mechanism

The shim uses one OTel propagator everywhere: `TraceContext + Baggage`, W3C format.

| Boundary | Carrier | Component |
|---|---|---|
| Client → Temporal (start, signal, query, update) | Temporal header `_tracer-data` | `contrib/opentelemetry` tracing interceptor on the client |
| Workflow → activity / child / signal-external / continue-as-new | Temporal header `_tracer-data` | same interceptor, workflow outbound side |
| Activity → gRPC | gRPC metadata | `shimgrpc.UnaryClientInterceptor` / `UnaryServerInterceptor` |
| Activity → Kafka | record headers | `shimkafka.Inject` / `Extract` |

The Temporal interceptor only serializes baggage next to a **valid span context**. The shim therefore:
- installs an SDK `TracerProvider` by default (no exporter needed); and
- starts a span (`ensureSpan`) before every client call that has none.

### 5.2 End-to-end path for one order

```mermaid
sequenceDiagram
  autonumber
  participant C as caller (ctx baggage)
  participant S as shim.Client
  participant T as Temporal
  participant W as OrderWorkflow
  participant A as activities
  participant Ch as PaymentWorkflow (child)
  participant G as gRPC inventory
  participant K as Kafka

  C->>S: ExecuteWorkflow(ctx)
  S->>S: Resolve → ns, queue · ensureSpan
  S->>T: StartWorkflow [header: baggage]
  T->>W: run (guard admits, RoutingFromWorkflow)
  C->>S: UpdateWorkflow "add-item"
  S->>T: [header: baggage] → W (UpdateRouting)
  W->>A: ReserveInventory [header]
  A->>G: Reserve [metadata: baggage]
  W->>A: PublishOrderEvent [header]
  A->>K: record [headers: baggage]
  W->>Ch: child start [header] (same queue)
  Ch->>A: ChargePayment [header]
  C->>S: QueryWorkflow "hops"
  S->>T: [header] → W (QueryRouting)
  C->>S: SignalWorkflow "approve"
  S->>T: [header] → W (SignalRouting)
  W->>T: ContinueAsNew [header copied from current span]
  T->>W: gen 1 (RoutingFromWorkflow unchanged)
  W->>A: PublishOrderEvent "completed" [header]
  A->>K: record [headers: baggage]
```

Every arrow labelled with a header or metadata is asserted in `TestConcurrentDevelopersAndProduction`.

### 5.3 Reading baggage inside a workflow, deterministically

Workflow code must not depend on anything that can differ on replay. The shim's workflow interceptor does
**not** read baggage from the Go context. It decodes it directly from the inbound Temporal header, which is
stored in history:

```mermaid
flowchart LR
  H["History event<br/>(WorkflowExecutionStarted /<br/>Signaled / UpdateAccepted)<br/>header _tracer-data"] --> D["guard interceptor<br/>routingFromHeader()"]
  D --> St["per-execution state<br/>start routing · last signal per name · last query"]
  D --> Ux["update ctx value"]
  St --> API1["RoutingFromWorkflow(ctx)"]
  St --> API2["SignalRouting(ctx, name)"]
  St --> API3["QueryRouting(ctx)"]
  Ux --> API4["UpdateRouting(ctx)"]
```

Replay sees the same headers, so it gets the same answers. The e2e suite replays every history with
`Client.ReplayerOptions()`, which installs the same interceptor stack.

## 6. Routing decision

```mermaid
flowchart TD
  start(["ExecuteWorkflow(ctx)"]) --> v{"baggage valid?"}
  v -- no --> err["error: missing/unknown ptid,<br/>malformed sbr/rid"]
  v -- yes --> p{"ptid"}
  p -- dashprod --> prod["production / &lt;service&gt;"]
  p -- dashtest --> s{"sbr set?"}
  s -- no --> fb["sandbox / &lt;service&gt;<br/>(production build)"]
  s -- yes --> own{"sbr belongs to<br/>this service?"}
  own -- no --> fb
  own -- yes --> rd["read registry memo<br/>(DescribeWorkflowExecution)"]
  rd -- "read failed" --> warn["log warning"] --> fb
  rd --> m{"live lease with same sbr<br/>and rid (if given)?"}
  m -- no --> fb
  m -- yes --> fill{"rid in baggage?"}
  fill -- no --> set["copy lease rid into baggage<br/>(newest run wins)"] --> sq
  fill -- yes --> sq["sandbox / sbx.&lt;sbr&gt;.&lt;rid&gt;"]
```

After resolving, the shim also:
- overrides `TaskQueue` in the start options, so the caller cannot misroute;
- scopes the workflow ID for `dashtest` traffic that carries a `rid` (`order-42` → `order-42@<rid>`).
  Signals, queries, updates and child or external IDs apply the same function, so business IDs stay usable;
- stamps the `Ptid`, `Sbr` and `Rid` search attributes for visibility and cleanup queries.

**Fail-safe direction.** Every uncertain case resolves to the production build in the `sandbox` namespace,
never to `production`. The worst outcome of a registry problem is "my sandbox didn't get the traffic", not
"test traffic reached production".

## 7. Test run lifecycle

### 7.1 States

```mermaid
stateDiagram-v2
  [*] --> Starting: StartTestRun()
  Starting --> Live: worker polling, lease registered
  Live --> Live: renew every TTL/3
  Live --> Closing: Close() / t.Cleanup / SIGINT
  Closing --> Gone: deregister → terminate leftovers → stop workers
  Live --> Orphaned: process killed
  Orphaned --> Expired: TTL passes (router stops matching)
  Expired --> Gone: registry timer → ReapSandboxRun
  Gone --> [*]
```

The ordering on both paths is deliberate:
- **Start:** the worker polls *before* the lease exists, so no routed task waits on an unpolled queue.
- **Close:** the lease goes *before* the worker stops, so no new task lands on a queue that is about to die.

### 7.2 Clean close vs. crash

```mermaid
sequenceDiagram
  participant Run as TestRun
  participant Reg as registry workflow
  participant Vis as visibility
  participant T as Temporal

  rect rgba(0,128,0,0.08)
  Note over Run,T: clean close
  Run->>Reg: update deregister
  Run->>Vis: list TaskQueue=sbx… AND Running
  Run->>T: terminate each (≤5 passes)
  Run->>Run: stop sandbox + registry workers
  end

  rect rgba(200,0,0,0.08)
  Note over Run,T: crash (no Close)
  Note over Reg: renewals stop
  Reg->>Reg: timer at ExpiresAt+1s → delete lease, upsert memo
  Reg->>Vis: activity ReapSandboxRun: list TaskQueue=sbx… AND Running
  Reg->>T: terminate each
  end
```

Both paths share `terminateTaskQueueWorkflows`, so they finish in the same state: no lease, no running
workflows, no pollers. Temporal has no API to delete a task queue. An unpolled queue with an empty backlog
is unloaded by the server, so that end state *is* "deleted".

## 8. Isolation model

There are three independent layers. Any one of them alone prevents the cross-tenant execution that G5 and
G7 rule out.

```mermaid
flowchart LR
  L1["Layer 1: namespace<br/>dashprod ↔ production<br/>dashtest ↔ sandbox"] --> L2["Layer 2: task queue<br/>one queue per run;<br/>children and activities pinned to it"] --> L3["Layer 3: worker guard<br/>inbound interceptor checks<br/>baggage before user code"]
```

| Worker | Admits | On violation |
|---|---|---|
| production build, `production` ns | `ptid=dashprod` | workflow task panics: no code runs, the workflow stays parked and visible |
| production build, `sandbox` ns | `ptid=dashtest` (any or no sbr) | same |
| sandbox run worker | `ptid=dashtest` ∧ `sbr` = own ∧ `rid` = own | same; activities fail non-retryably |

`TestGuardRefusesMisroutedWorkflows` bypasses layers 1 and 2 with a raw SDK client and shows that layer 3
holds.

**Interceptor stack on a worker**

```mermaid
flowchart TB
  in(["task from server"]) --> t["OTel tracing interceptor (outer, from the client)<br/>header ⇄ span + baggage"]
  t --> g["shim guard (inner, per worker)<br/>admit · capture routing · pin queues ·<br/>scope child IDs · stamp search attributes"]
  g --> code["workflow / activity code"]
```

The SDK applies the client's interceptors before the worker's own, so tracing wraps the guard. The guard
reads headers directly, so its decisions would not change if the order did.

## 9. Key decisions and alternatives

| Decision | Chosen | Alternatives considered | Why |
|---|---|---|---|
| Where baggage rides | OTel `contrib/opentelemetry` interceptor + W3C baggage | custom `ContextPropagator` with its own header | One standard format end to end (Temporal, gRPC, Kafka); trace context comes for free; interoperates with other SDKs |
| Isolation unit | namespace per tenant + task queue per run | queue per sandbox; namespace per developer | Per-run queues isolate parallel runs; namespaces per developer are heavy to create, clean up and authorize |
| Liveness source | explicit lease registry | poll `DescribeTaskQueue` pollers | Pollers linger for minutes after a worker dies, can't map `sbr`→newest `rid`, and can't trigger cleanup |
| Registry storage | Temporal workflow | external KV (Redis/etcd), DB table | No new infrastructure; durable timers for expiry; serialized writes; reaping runs as an activity |
| Registry reads | memo via `DescribeWorkflowExecution` | workflow query | Queries need a live worker and stalled ~5s when the sticky worker had died; the memo is written atomically with each update |
| Fallback target | production build in `sandbox` namespace | production namespace; reject the request | Keeps `dashtest` out of `production`; still serves un-sandboxed test traffic |
| Business ID collisions | suffix `@<rid>`, applied by the shim | require unique IDs from tests | Developers reuse fixtures freely; the transform is applied consistently on every call |
| Misroute response | panic the workflow task | fail the workflow; drop | Never runs user code; the workflow is kept for diagnosis instead of being destroyed |

## 10. Risks and open questions

| Risk | Impact | Mitigation / next step |
|---|---|---|
| Single registry workflow serializes writes | Throughput ceiling at hundreds of runs | Shard by service, or move leases to an existing routing store |
| Registry read on every routed start | Extra RPC per `dashtest`+`sbr` start | ~1s client-side cache in front of `Client.Leases` |
| Partitioned live run outlives its TTL | Its workflows are reaped while it still runs | Tune `LeaseTTL`; consider a grace period before reaping |
| Long-running registry changes code | Non-determinism on replay | `workflow.GetVersion`, or terminate-and-restart (runs re-register within TTL/3) |
| Cross-service child workflows | Child would land on the parent's queue, not the other service's sandbox | Resolve in a local activity via `Client.ForService(x).Resolve`, then start with that queue |
| Kafka consumers ignore baggage | Sandbox records processed by production consumers | Consumer-side rule: sandbox consumers take only their sbr+rid; production consumers skip `dashtest` |
| Credentials | A developer client could address `production` directly | Temporal Cloud: per-namespace API keys/mTLS; developers get `sandbox` only |

## 11. Verification

| Test | Proves |
|---|---|
| `TestConcurrentDevelopersAndProduction` | G1–G3, G6, G7: five concurrent flows on one order ID; every hop's baggage, namespace, queue and worker; Kafka consumer view; replay determinism |
| `TestGuardRefusesMisroutedWorkflows` | G5: layer 3 holds when layers 1–2 are bypassed |
| `TestCloseCleansUpRun` | G4 (clean): lease withdrawn, leftovers terminated, later traffic falls back |
| `TestCrashedRunIsReaped` | G4 (crash): lease expiry + registry reaping |
| `pkg/shim` unit tests | baggage validation, header decoding, ID scoping, guard policy matrix |
