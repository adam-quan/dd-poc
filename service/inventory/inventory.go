// Package inventory is a downstream gRPC service called from activities. It
// echoes back the baggage it received so propagation can be verified. It uses
// a JSON codec and a hand-written ServiceDesc to avoid a protoc step.
package inventory

import (
	"context"
	"encoding/json"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
	"github.com/temporalio/doordash-sandbox-poc/pkg/shim/shimgrpc"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (jsonCodec) Name() string                    { return "json" }

func init() { encoding.RegisterCodec(jsonCodec{}) }

type ReserveRequest struct {
	OrderID string   `json:"orderId"`
	Items   []string `json:"items"`
}

type ReserveResponse struct {
	ReservationID string       `json:"reservationId"`
	Server        string       `json:"server"`
	Observed      shim.Routing `json:"observed"`
}

type server struct{ name string }

func (s *server) Reserve(ctx context.Context, req *ReserveRequest) (*ReserveResponse, error) {
	return &ReserveResponse{
		ReservationID: "res-" + req.OrderID,
		Server:        s.name,
		Observed:      shim.RoutingFromContext(ctx),
	}, nil
}

type inventoryServer interface {
	Reserve(context.Context, *ReserveRequest) (*ReserveResponse, error)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "inventory.Inventory",
	HandlerType: (*inventoryServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Reserve",
		Handler: func(srv any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
			in := new(ReserveRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			call := func(ctx context.Context, req any) (any, error) {
				return srv.(inventoryServer).Reserve(ctx, req.(*ReserveRequest))
			}
			if ic == nil {
				return call(ctx, in)
			}
			return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/inventory.Inventory/Reserve"}, call)
		},
	}},
}

// Serve starts the inventory server on lis.
func Serve(lis net.Listener) *grpc.Server {
	host, _ := os.Hostname()
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(shimgrpc.UnaryServerInterceptor()))
	s.RegisterService(&serviceDesc, &server{name: "inventory@" + host})
	go func() { _ = s.Serve(lis) }()
	return s
}

// Client calls the inventory service, propagating baggage.
type Client struct{ cc *grpc.ClientConn }

func Dial(addr string) (*Client, error) {
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(shimgrpc.UnaryClientInterceptor()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc}, nil
}

func (c *Client) Reserve(ctx context.Context, req *ReserveRequest) (*ReserveResponse, error) {
	out := new(ReserveResponse)
	return out, c.cc.Invoke(ctx, "/inventory.Inventory/Reserve", req, out)
}

func (c *Client) Close() error { return c.cc.Close() }
