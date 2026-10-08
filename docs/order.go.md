`order.go` is the example service the whole POC tests. `OrderWorkflow` is built to **exercise every path baggage can travel**, and at each one it appends a `Hop` saying where it ran and what baggage it saw. A test drives the workflow, collects the hops, and checks that every one shows the expected `ptid`/`sbr`/`rid`, namespace, task queue and worker.

## The `Hop` record

[order.go:34-40](service/order/order.go#L34-L40). Each `Hop` has a step name, the `shim.Routing` seen at that point, and where the step ran (namespace, task queue, worker identity). Two helpers build hops:

- `wfHop` takes the location from `workflow.GetInfo`.
- `activityHop` takes it from `activity.GetInfo`, and the worker from `shim.WorkerIdentity`, which the guard interceptor puts on the context.

## `OrderWorkflow`: one pass over every propagation path

[L67-131](service/order/order.go#L67-L131). It runs as two **generations**.

**Generation 0** does the work:

| Step | Path exercised | How the baggage is read |
|---|---|---|
| workflow start | client → server → worker | `shim.RoutingFromWorkflow` |
| `ReserveInventory` activity | workflow → activity, then an **outbound gRPC** call to `inventory` | `activityHop`, plus the inventory server echoes what it saw (`resp.Observed`) |
| `PublishOrderEvent("reserved")` | activity → **Kafka** headers | `activityHop` (a consumer checks the headers separately, see below) |
| `PaymentWorkflow` child, ID `payment-<orderID>` | **child workflow** and its activity | the child records its own start hop and a `ChargePayment` hop |
| waits for the `approve` signal | **signal** | `shim.SignalRouting` |
| then continue-as-new to generation 1, passing the hops and items along | **continue-as-new** | — |

Throughout generation 0 there are also two handlers:

- The **`add-item` update** records its hop with `shim.UpdateRouting(uctx)`. It must use the handler's `uctx`, not the workflow `ctx`.
- The **`hops` query** returns the hops so far, plus the query's own baggage from `shim.QueryRouting(ctx)`.

Before continuing as new, the workflow waits for `AllHandlersFinished`, so an update still running doesn't get cut off.

**Generation 1** shows that the baggage survived continue-as-new. It records a new `workflow-start` hop, publishes a `"completed"` event, and returns all the hops.

**`Register`** ([L197-203](service/order/order.go#L197-L203)) is the `shim.Registrar`. The **same** function registers production and sandbox workers, so a developer's sandbox runs exactly the code they're testing.

## Where the workflow code never touches routing

The workflow doesn't choose a task queue or namespace anywhere, and it never writes `@<rid>`. All of that comes from the shim:

- The child's ID `payment-<orderID>` is scoped to `payment-<orderID>@<rid>` by the outbound interceptor. On a sandbox worker, the child and all activities are pinned to `sbx.<sbr>.<rid>`.
- `RunScenario` in [scenario.go](service/order/scenario.go) starts, updates, queries and signals using the plain business ID `order-<id>`. `shim.Client` scopes the ID and picks the namespace and queue from the ctx baggage.

So production code needs no changes to run in a sandbox. That's the point of the design.

## How it's checked

[scenario.go](service/order/scenario.go):

- **`RunScenario`** drives the full sequence: start → update → query → signal → wait for the result, following continue-as-new.
- **`ConsumerHop`** turns a Kafka message into a hop, using the baggage `shimkafka.Extract` finds in its headers.
- **`Verify`** compares every hop against an `Expectation`: routing, namespace, task queue, and the worker identity prefix for activity hops. It also checks that every kind of hop showed up at least once, so a missing path counts as a failure.

## Small things worth noticing

- **`PublishOrderEvent` isn't idempotent.** With `MaximumAttempts: 3`, a retry after a publish that actually succeeded sends a duplicate event. That's fine for a demo, but downstream consumers should expect at-least-once delivery.
- **The `approve` signal's payload is discarded** (`Receive(ctx, nil)`). Only its routing is recorded. `SignalRouting` returns the routing of the *latest* `approve` signal, which is fine here because the scenario sends exactly one.
- **Hops grow the history.** They are carried through the child's result and the continue-as-new input, so history grows with each hop. That's fine for a test workflow, but not a pattern for real ones.
- **`OrderWorkflow` is replayed in tests.** Per CLAUDE.md, `TestConcurrentDevelopersAndProduction` replays its histories, so changes to its control flow have to stay deterministic.