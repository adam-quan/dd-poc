package shim

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func TestValidate(t *testing.T) {
	ok := []Routing{
		{PTID: TenantProd},
		{PTID: TenantTest},
		{PTID: TenantTest, SBR: "order-service-web-sandbox-alice", RID: NewRunID()},
	}
	for _, r := range ok {
		require.NoError(t, r.Validate(), r.String())
	}
	bad := []Routing{
		{},
		{PTID: "dashstaging"},
		{PTID: TenantTest, SBR: "alice"},
		{PTID: TenantTest, SBR: "order-service-web-sandbox-alice", RID: "has space"},
	}
	for _, r := range bad {
		require.Error(t, r.Validate(), r.String())
	}
}

func TestWithRoutingRoundTripAndClears(t *testing.T) {
	r := Routing{PTID: TenantTest, SBR: SBR("order-service", "web", "alice"), RID: NewRunID()}
	ctx := MustWithRouting(context.Background(), r)
	require.Equal(t, r, RoutingFromContext(ctx))

	// Switching to prod must drop sbr/rid rather than leak them.
	ctx = ProdContext(ctx)
	require.Equal(t, Routing{PTID: TenantProd}, RoutingFromContext(ctx))
}

func TestSBRService(t *testing.T) {
	require.True(t, SBRService("order-service-web-sandbox-alice", "order-service"))
	require.False(t, SBRService("payment-service-web-sandbox-alice", "order-service"))
	require.False(t, SBRService("order-service-web-alice", "order-service"))
}

func TestScopedWorkflowID(t *testing.T) {
	r := Routing{PTID: TenantTest, SBR: "order-service-web-sandbox-alice", RID: "r1234"}
	require.Equal(t, "order-1@r1234", ScopedWorkflowID(r, "order-1"))
	require.Equal(t, "order-1@r1234", ScopedWorkflowID(r, ScopedWorkflowID(r, "order-1")), "idempotent")
	require.Equal(t, "order-1", ScopedWorkflowID(Routing{PTID: TenantProd}, "order-1"))
	require.Equal(t, "order-1", ScopedWorkflowID(Routing{PTID: TenantTest}, "order-1"))
	require.Equal(t, "", ScopedWorkflowID(r, ""))
}

// The Temporal header written by the OTel tracing interceptor decodes back to
// the same routing (this is what workflow code sees, including on replay).
func TestRoutingFromHeader(t *testing.T) {
	r := Routing{PTID: TenantTest, SBR: "order-service-web-sandbox-bob", RID: "r20261007t000000-abcd"}
	ctx, span := sdktrace.NewTracerProvider().Tracer("t").Start(MustWithRouting(context.Background(), r), "x")
	defer span.End()
	m := propagation.MapCarrier{}
	Propagator.Inject(ctx, m)
	p, err := converter.GetDefaultDataConverter().ToPayload(map[string]string(m))
	require.NoError(t, err)
	require.Equal(t, r, routingFromHeader(map[string]*commonpb.Payload{tracerHeaderKey: p}))
	require.Equal(t, Routing{}, routingFromHeader(nil))
}

func TestWorkerPolicy(t *testing.T) {
	alice := Routing{PTID: TenantTest, SBR: "order-service-web-sandbox-alice", RID: "r-alice-1"}
	bob := Routing{PTID: TenantTest, SBR: "order-service-web-sandbox-bob", RID: "r-bob-1"}
	prod := Routing{PTID: TenantProd}

	prodWorker := &workerPolicy{role: roleProduction, namespace: "production", prodNS: "production"}
	baseline := &workerPolicy{role: roleProduction, namespace: "sandbox", prodNS: "production"}
	aliceWorker := &workerPolicy{role: roleSandbox, namespace: "sandbox", prodNS: "production", expect: alice}

	require.NoError(t, prodWorker.admit(prod))
	require.Error(t, prodWorker.admit(alice))
	require.Error(t, prodWorker.admit(Routing{}))

	require.NoError(t, baseline.admit(alice), "unmatched test traffic may land on production workers")
	require.NoError(t, baseline.admit(Routing{PTID: TenantTest}))
	require.Error(t, baseline.admit(prod))

	require.NoError(t, aliceWorker.admit(alice))
	require.Error(t, aliceWorker.admit(bob))
	require.Error(t, aliceWorker.admit(prod))
	require.Error(t, aliceWorker.admit(Routing{PTID: TenantTest, SBR: alice.SBR, RID: "r-alice-2"}), "other run of same sandbox")
}
