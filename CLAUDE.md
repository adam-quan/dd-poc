# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A POC of baggage-routed developer sandboxes on one Temporal cluster (Go SDK). OpenTelemetry baggage decides where work runs:
- `ptid` picks the namespace: `dashprod` → `production`, `dashtest` → `sandbox`.
- `sbr` (`<service>-<app>-sandbox-<name>`) plus `rid` (generated per test run) pick a per-run task queue, `sbx.<sbr>.<rid>`.

`pkg/shim` is the reusable library. `service/order` is the example service under test. Design rationale and diagrams are in `docs/DESIGN.md`; how to run things and the registry's operations are in `README.md`.

## Commands

```bash
make server     # Temporal dev server with production + sandbox namespaces and Ptid/Sbr/Rid search attributes (keep running)
make unit       # go test ./pkg/...  (no server needed)
make e2e        # go test ./test/ -count=1 -v  (needs `make server`)
make demo       # multi-process demo via scripts/demo.sh (needs `make server`); per-process logs land in .demo-logs/
make build      # binaries into bin/
go vet ./... && gofmt -l .
go test ./test/ -count=1 -run TestCrashedRunIsReaped -v   # single e2e test
go test ./pkg/shim -run TestWorkerPolicy                  # single unit test
```

Without a reachable dev server, the e2e tests skip rather than fail. Run with `-race` before claiming concurrency changes are safe.

## Architecture (the parts that span files)

- **Router**: `shim.Client` (`pkg/shim/client.go`) holds one client per namespace over a shared connection. Every call reads baggage from `ctx`. `Resolve` chooses the namespace and task queue and overrides whatever the caller passed in the start options. It also scopes workflow IDs for `dashtest` traffic that carries a `rid` (`id@<rid>`). Signal, query, update, get and terminate apply the same `ScopedWorkflowID`, and so does the worker-side outbound interceptor for child and external IDs.
- **Propagation** (`propagation.go`): Temporal's `contrib/opentelemetry` tracing interceptor carries W3C baggage in the `_tracer-data` header, and gRPC (`shimgrpc`) and Kafka (`shimkafka`) use the same propagator. That interceptor **only carries baggage next to a valid span context**. This is why the shim installs an SDK `TracerProvider` and calls `ensureSpan` before every client call. Removing either one silently drops all routing.
- **Workflow-side routing** (`interceptor.go`): the guard interceptor decodes routing directly from inbound Temporal headers, which are recorded in history, so it is replay-safe. Workflow code reads it through `RoutingFromWorkflow`, `SignalRouting`, `UpdateRouting` and `QueryRouting`. The same interceptor admits or refuses each task by worker role. A refused workflow task panics, so no user code runs. On sandbox workers it also pins activities and children to the run's own queue.
- **Workers** (`worker.go`): `NewProductionWorkers` polls `<service>` in both namespaces. Its `sandbox`-namespace pool is the fallback for `dashtest` traffic that has no live sandbox. `StartTestRun` sequences startup as worker polling → lease registered, and `Close` sequences shutdown as lease withdrawn → leftovers terminated → worker stopped. Keep these orderings.
- **Registry** (`registry.go`): the single long-running workflow `shim-sandbox-registry` in the `sandbox` namespace. Leases are written through update-with-start and mirrored into the workflow **memo**. Routers read the memo with `DescribeWorkflowExecution`, not with a query, because queries stalled about 5s when the sticky worker was gone. A durable timer reaps expired leases with the `ReapSandboxRun` activity. Every process that runs sandbox-namespace workers also hosts a registry worker.

## Gotchas

- `SandboxRegistryWorkflow` and `OrderWorkflow` must stay deterministic. Changing the registry's code breaks replay of the execution that is already running: use `workflow.GetVersion`, or terminate it (`temporal workflow terminate --namespace sandbox --workflow-id shim-sandbox-registry`). Runs re-register within TTL/3. `TestConcurrentDevelopersAndProduction` replays histories via `Client.ReplayerOptions()`.
- Use `127.0.0.1:7233`, not `localhost`. The dev server listens on IPv4 only, and `localhost` intermittently stalled dials past the SDK's 5s check.
- The custom search attributes `Ptid`, `Sbr` and `Rid` must exist in both namespaces; `scripts/dev-server.sh` registers them. Otherwise set `Config.DisableSearchAttributes`.
- Kafka defaults to the in-process `shimkafka.MemoryBroker`. A real broker is used only when `KAFKA_BROKERS` is set; that path compiles but has not been exercised.
- Environment variables: `TEMPORAL_ADDRESS`, `PROD_NAMESPACE`, `SANDBOX_NAMESPACE`, `INVENTORY_ADDR`, `KAFKA_BROKERS`, `SANDBOX_NAME`.
