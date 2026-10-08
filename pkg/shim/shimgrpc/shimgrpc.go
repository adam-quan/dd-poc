// Package shimgrpc carries the shim's OTel baggage (ptid/sbr/rid) and trace
// context over gRPC metadata.
package shimgrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/adam-quan/dd-poc/pkg/shim"
)

// mdCarrier adapts gRPC metadata to an OTel TextMapCarrier.
type mdCarrier metadata.MD

func (m mdCarrier) Get(k string) string {
	if v := metadata.MD(m).Get(k); len(v) > 0 {
		return v[0]
	}
	return ""
}
func (m mdCarrier) Set(k, v string) { metadata.MD(m).Set(k, v) }
func (m mdCarrier) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// UnaryClientInterceptor injects the ctx baggage into outgoing metadata.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		shim.Propagator.Inject(ctx, mdCarrier(md))
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	}
}

// UnaryServerInterceptor extracts baggage from incoming metadata into ctx, so
// handlers can call shim.RoutingFromContext and pass it further downstream.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = shim.Propagator.Extract(ctx, mdCarrier(md))
		}
		return handler(ctx, req)
	}
}
