package shim

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
)

// Propagator is the single OTel propagator used on every hop: Temporal
// headers, gRPC metadata and Kafka headers. W3C trace context + W3C baggage.
var Propagator propagation.TextMapPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// tracerHeaderKey is the Temporal header the OTel tracing interceptor writes
// (shared by all SDKs).
const tracerHeaderKey = "_tracer-data"

var defaultTP struct {
	once sync.Once
	tp   trace.TracerProvider
}

func tracerProvider(cfg Config) trace.TracerProvider {
	if cfg.TracerProvider != nil {
		return cfg.TracerProvider
	}
	defaultTP.once.Do(func() {
		// A real SDK provider is required: the Temporal OTel interceptor only
		// carries baggage alongside a *valid* span context.
		defaultTP.tp = sdktrace.NewTracerProvider()
		otel.SetTextMapPropagator(Propagator)
	})
	return defaultTP.tp
}

func newTracingInterceptor(tp trace.TracerProvider) (interceptor.Interceptor, error) {
	return temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{
		Tracer:            tp.Tracer("github.com/adam-quan/dd-poc/shim"),
		TextMapPropagator: Propagator,
	})
}

// ensureSpan makes sure ctx carries a valid span so the Temporal tracing
// interceptor picks up the baggage on ctx as its parent.
func ensureSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, func()) {
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, func() {}
	}
	ctx, span := tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient))
	return ctx, func() { span.End() }
}

// routingFromHeader decodes routing from a Temporal header written by the OTel
// tracing interceptor. Header contents are part of history, so this is
// deterministic inside workflows (including on replay).
func routingFromHeader(h map[string]*commonpb.Payload) Routing {
	p := h[tracerHeaderKey]
	if p == nil {
		return Routing{}
	}
	var m map[string]string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &m); err != nil {
		return Routing{}
	}
	ctx := propagation.Baggage{}.Extract(context.Background(), propagation.MapCarrier(m))
	return routingFromBaggage(baggage.FromContext(ctx))
}
