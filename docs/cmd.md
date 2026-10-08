The four programs in `cmd/` are the processes a real deployment would have. [scripts/demo.sh](scripts/demo.sh) (`make demo`) runs them side by side against the dev server to show the routing working.

## The four binaries

| Binary | Plays the role of | Runs workers? |
|---|---|---|
| [prodworker](cmd/prodworker/main.go) | the deployed production build of `order-service` | yes: production + fallback + registry |
| [devtest](cmd/devtest/main.go) | one developer testing their local build | yes: one sandbox run |
| [starter](cmd/starter/main.go) | an upstream caller or the edge, sending traffic with any baggage | no (client only) |
| [inventory](cmd/inventory/main.go) | a downstream gRPC service called from activities | no (gRPC server) |

All of them get their config from `shim.ConfigFromEnv` and `demo.Env`, using `TEMPORAL_ADDRESS`, `INVENTORY_ADDR` and `KAFKA_BROKERS`. Most of the logic lives in [service/order/scenario.go](service/order/scenario.go) and [internal/demo](internal/demo).

### `prodworker`

It dials the shim, builds the order activities (inventory gRPC client plus Kafka producer), and runs `NewProductionWorkers`. That gives three pollers:

- `production/order-service`
- `sandbox/order-service`: the fallback for test traffic with no live sandbox
- `sandbox/shim-sandbox-registry`

It runs until SIGINT or SIGTERM. With a real Kafka, it also starts the `order-events-audit` consumer, which logs the baggage on every event.

### `devtest`

This is the developer's side ([main.go:39-93](cmd/devtest/main.go#L39-L93)):

1. Calls `StartTestRun` with `-name` (defaults to `$SANDBOX_NAME` or your OS user) and `-app web`. That gives a fresh `rid`, a sandbox worker on `sbx.order-service-web-sandbox-<name>.<rid>`, and a lease.
2. For each of the `-orders` orders, runs `order.RunScenario` with `tr.Context(ctx)`, which carries the run's baggage. It then checks every hop against the run's own baggage, sandbox namespace, task queue and worker identity, and prints `OK` or one `FAIL` line per bad hop.
3. With `-hold`, keeps the sandbox alive after the scenario so other processes can send traffic into it.
4. On exit, `tr.Close` runs from a `defer` with its own 30s context. That covers normal exit, a scenario error, and Ctrl-C: `signal.NotifyContext` cancels `ctx`, the scenario returns early, and the cleanup still runs. Cleanup withdraws the lease, terminates leftover workflows, and stops the worker.

### `starter`

This sends one order scenario with whatever baggage the flags give it: `-ptid`, `-sbr`, `-rid`, `-order`. It prints the hops but **doesn't verify them**, because it has no expectation to compare against. It also has two inspection modes:

- `-leases`: lists live leases, read from the registry memo.
- `-resolve`: prints the routing decision and its reason, without starting anything.

### `inventory`

It serves `inventory.Serve` on `127.0.0.1:50051` and stops gracefully on a signal. It doesn't route anything itself. It echoes back the baggage it received (`resp.Observed`), which is how the `grpc:inventory.Reserve` hop shows that baggage crossed gRPC.

## What the demo shows

[scripts/demo.sh](scripts/demo.sh) builds everything, starts `inventory` and `prodworker`, writes each process's log to `.demo-logs/`, then runs three scenes.

**Scene 1: four kinds of traffic at once, all with order ID `1001`.**

| Command | Ends up as |
|---|---|
| `devtest -name alice` | `order-1001@<alice-rid>` on alice's queue |
| `devtest -name bob` | `order-1001@<bob-rid>` on bob's queue |
| `starter -ptid dashprod` | `order-1001` in `production` |
| `starter -ptid dashtest -sbr ...-carol` (no live run) | `order-1001` in `sandbox` on the fallback workers |

The four workflows have the same business ID and don't collide.

**Scene 2: `sbr`-only traffic reaches a live run.** `devtest -name dana -orders 0 -hold 60s` keeps a run up. Then `starter -sbr ...-dana` sends traffic with no `rid`. `Resolve` fills in dana's `rid` from the lease, so the order runs on dana's worker.

**Scene 3: cleanup on Ctrl-C.** The script sends `kill -INT` to dana's process. Afterwards `starter -leases` no longer lists dana, and `-resolve` for dana's `sbr` reports the fallback route.

## Gotchas

- **The last banner in the demo is misleading with the default in-memory Kafka.** It greps `prodworker.log` for `"kafka consumer received"`, under the heading "Downstream Kafka consumer (in the prod worker) saw these baggages". But each process gets its **own** `MemoryBroker` from `demo.Activities`. The prod worker's consumer therefore only sees events published by activities that ran on the prod worker: the `dashprod` and carol orders. Alice's, bob's and dana's events are logged in their own devtest logs. With `KAFKA_BROKERS` set, it would really be one shared topic.
- **`starter` doesn't verify anything.** Scenes 2 and 3 only print hops, so a routing regression there wouldn't make the demo fail. Only `devtest` (and `make e2e`) checks hops.
- **`starter` with `-sbr` and no `-rid` is exposed to the per-call resolve issue from earlier.** If dana's lease disappears between the start and the signal, the later calls target a different workflow ID. In scene 2, `-hold 60s` is long enough that this doesn't happen.
- **The demo relies on fixed sleeps** (`sleep 2` and `sleep 3`) to wait for `prodworker` and dana's run to be ready, not on a readiness check, so it can be flaky on a slow machine.