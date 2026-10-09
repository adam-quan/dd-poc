## `pkg/shim/propagation.go`

This file connects the shim's routing baggage (`ptid`, `sbr`, `rid`) to the way Temporal carries data between processes. It has one shared value, one constant and four small functions.

### `Propagator`
A single OpenTelemetry propagator used everywhere: Temporal headers, gRPC metadata (`shimgrpc`) and Kafka headers (`shimkafka`). It combines two W3C standards:
- **`TraceContext`** writes `traceparent`, which identifies the trace and span.
- **`Baggage`** writes `baggage`, the key/value pairs.

Over the wire, one hop's data looks like:

```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
baggage:     ptid=dashtest,sbr=order-service-web-sandbox-alice,rid=r20261007t224905-b36ae4a2
```

Using one propagator means the same two headers appear at every hop, in the same format.

### `tracerHeaderKey = "_tracer-data"`
The name of the Temporal header where the tracing interceptor stores the data above. Every Temporal SDK uses this name. `routingFromHeader` reads it directly.

### `tracerProvider(cfg)`: why a real tracer is required
This is the least obvious part of the file. The Temporal OTel interceptor (`contrib/opentelemetry`) treats baggage as an attachment to a span, not as something it carries on its own:
- **Sending side:** it takes the parent span from the caller's `ctx` and copies that span's baggage. If `ctx` has baggage but no valid span, there is no parent, and the baggage is dropped.
- **Receiving side:** if a header has no `traceparent`, it reports "no span" and ignores the `baggage` beside it.

So routing only works if spans are real. OpenTelemetry's default global tracer is a no-op whose spans are invalid. The function therefore:
- uses `cfg.TracerProvider` if the caller set one (for example, one that exports to Jaeger or Datadog);
- otherwise creates a single SDK `TracerProvider`, once per process via `sync.Once`. It has no exporter, so spans are valid but go nowhere. In the same step it sets `Propagator` as the global OTel propagator, so other OTel-instrumented code in the process uses the same format.

The global is only set in that default branch; with a custom provider it's left alone. The shim itself never relies on the global, because it passes `Propagator` explicitly everywhere.

### `newTracingInterceptor(tp)`
Builds Temporal's OTel tracing interceptor with this tracer and `Propagator`. `Dial` installs it on both namespace clients. Workers created from those clients inherit it, so the same interceptor:
- writes `_tracer-data` on starts, signals, queries and updates;
- writes it on activity calls, child starts, external signals and continue-as-new;
- restores baggage into each activity's `context.Context`.

### `ensureSpan(ctx, tracer, name)`
The safeguard for the requirement above. Every `shim.Client` call runs `ctx` through it before calling the SDK:
- If `ctx` already has a valid span (the caller is traced), it returns `ctx` unchanged with a no-op end function.
- Otherwise it starts a client span named, for example, `shim.ExecuteWorkflow`. The interceptor then has a valid parent and carries the baggage. The caller ends the span with the returned function.

Without this, code that set baggage but never started a span would route correctly at start time, since `Resolve` reads the baggage straight from `ctx`. The baggage, however, would never reach the workflow. The guard would then see empty routing and refuse the task.

### `routingFromHeader(h)`
This is how workflow code reads routing without breaking determinism.
1. Look up `_tracer-data` in a Temporal header map. If it's missing, return zero routing.
2. Decode the payload into `map[string]string` with the default data converter. If that fails, return zero routing.
3. Run **only** the `Baggage` propagator over that map, then convert the result into a `Routing` with `routingFromBaggage` (in `routing.go`).

It reads the header directly instead of going through the interceptor's span for two reasons:
- **Determinism:** headers are recorded in workflow history, so replay decodes the same bytes and gets the same `Routing`.
- **Independence:** it skips the `traceparent` check and doesn't depend on where the guard sits relative to the tracing interceptor.

`interceptor.go` calls it for workflow starts, signals, updates and queries, and for activity tasks. Its results back `RoutingFromWorkflow`, `SignalRouting`, `UpdateRouting`, `QueryRouting`, and the worker guard's admit/refuse decision. `TestRoutingFromHeader` in `routing_test.go` checks that a round trip through this path preserves the routing.