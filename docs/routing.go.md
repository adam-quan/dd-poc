
## What this code is doing

This file defines the routing metadata used by the sandbox/prod shim. The core idea is:

- a request carries OpenTelemetry baggage
- that baggage includes:
  - `ptid` = tenant, either `dashprod` or `dashtest`
  - `sbr` = sandbox routing key
  - `rid` = run ID for a specific sandbox run
- the system uses those values to decide:
  - which Temporal namespace to hit
  - which task queue to use
  - which worker is allowed to process the request

This is a routing layer, not business logic.

---

## 1) `Routing`

At `routing.go:41-49`, `Routing` is the data structure:

- `PTID` = tenant ID
- `SBR` = sandbox route like `myservice-myapp-sandbox-dev`
- `RID` = per-run unique ID

It is the “subset of request state that matters for routing.”

Helper methods:

- `IsProd()` checks if `PTID == "dashprod"`
- `IsTest()` checks if `PTID == "dashtest"`
- `IsZero()` checks if it is empty
- `String()` formats it for logging

---

## 2) Validation

At `routing.go:51-69`, `Validate()` ensures the values are valid.

It enforces:

- `ptid` must be `dashprod` or `dashtest`
- `sbr` must match a sandbox pattern like `<service>-<app>-sandbox-<name>`
- `rid` must look like a valid run ID

This prevents bad baggage from silently misrouting work.

---

## 3) Building route keys

At `routing.go:71-83`:

- `SBR(service, app, sandboxName)` creates a routing key:
  - `myservice-myapp-sandbox-dev`
- `SBRService(sbr, service)` checks whether a sandbox key belongs to a given service
- `NewRunID()` generates a unique, queue-safe run ID
- `SandboxTaskQueue(sbr, rid)` builds a per-run queue name:
  - `sbx.<sbr>.<rid>`

This is the glue between routing metadata and Temporal task queues.

---

## 4) Injecting baggage into a context

At `routing.go:85-101`, `WithRouting` does this:

- validates the routing values
- reads the current OTel baggage from `ctx`
- replaces or deletes `ptid`, `sbr`, and `rid`
- writes the updated baggage back into the context

This is important because the baggage is what propagates through calls, activities, child workflows, and signals.

Example behavior:

- empty values remove the baggage key
- non-empty values add them back
- stale values are never left behind

---

## 5) Reading baggage back out

At `routing.go:103-117`, `RoutingFromContext` reads the baggage from the current context and turns it into a `Routing`.

This is how code later decides:

- “am I in production or sandbox?”
- “which sandbox run am I in?”
- “is this a valid developer test route?”

---

## 6) Why this matters

The package comment at the top explains the big idea:

- OpenTelemetry baggage travels across Temporal work
- `ptid` selects namespace
- `sbr` + `rid` identifies the developer sandbox run and task queue

So the system is essentially using standard OTel baggage as the routing control plane.

---

## Mental model

Think of it like this:

- `ptid` = “which environment?”
- `sbr` = “which sandbox?”
- `rid` = “which run of that sandbox?”

Then the rest of the shim translates that into actual Temporal namespace and task queue routing.

If you want, I can next explain the matching worker guard in `interceptor.go`, which is the part that enforces these values at runtime.