// Package order is the example service under test ("order-service"). Its
// workflow deliberately touches every propagation path: workflow start,
// activities, a child workflow, a signal, an update, a query,
// continue-as-new, and outbound gRPC and Kafka calls from activities. Each
// hop records the baggage it observed.
package order

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/adam-quan/dd-poc/pkg/shim"
	"github.com/adam-quan/dd-poc/pkg/shim/shimkafka"
	"github.com/adam-quan/dd-poc/service/inventory"
)

const (
	Service     = "order-service"
	EventsTopic = "order-events"

	SignalApprove = "approve"
	UpdateAddItem = "add-item"
	QueryHops     = "hops"
)

// Hop is one observation of the baggage at a point in the flow.
type Hop struct {
	Step      string       `json:"step"`
	Routing   shim.Routing `json:"routing"`
	Namespace string       `json:"namespace,omitempty"`
	TaskQueue string       `json:"taskQueue,omitempty"`
	Worker    string       `json:"worker,omitempty"`
}

type OrderInput struct {
	OrderID    string   `json:"orderId"`
	Items      []string `json:"items"`
	Generation int      `json:"generation"`
	Hops       []Hop    `json:"hops"`
}

type OrderResult struct {
	OrderID string   `json:"orderId"`
	Items   []string `json:"items"`
	Hops    []Hop    `json:"hops"`
}

type QueryResult struct {
	Hops  []Hop        `json:"hops"`
	Query shim.Routing `json:"query"` // baggage carried by this query
}

func wfHop(ctx workflow.Context, step string, r shim.Routing) Hop {
	info := workflow.GetInfo(ctx)
	return Hop{Step: step, Routing: r, Namespace: info.Namespace, TaskQueue: info.TaskQueueName}
}

// OrderWorkflow: generation 0 does the work and continues-as-new; generation
// 1 proves the baggage survived continue-as-new and completes.
func OrderWorkflow(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	hops := in.Hops
	items := in.Items
	step := func(s string) string { return fmt.Sprintf("gen%d:%s", in.Generation, s) }

	hops = append(hops, wfHop(ctx, step("workflow-start"), shim.RoutingFromWorkflow(ctx)))

	if err := workflow.SetQueryHandler(ctx, QueryHops, func() (QueryResult, error) {
		return QueryResult{Hops: hops, Query: shim.QueryRouting(ctx)}, nil
	}); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.SetUpdateHandler(ctx, UpdateAddItem, func(uctx workflow.Context, item string) (int, error) {
		hops = append(hops, wfHop(uctx, step("update:"+UpdateAddItem), shim.UpdateRouting(uctx)))
		items = append(items, item)
		return len(items), nil
	}); err != nil {
		return OrderResult{}, err
	}

	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var a *Activities

	if in.Generation > 0 {
		// After continue-as-new.
		var h Hop
		if err := workflow.ExecuteActivity(actx, a.PublishOrderEvent, in.OrderID, "completed").Get(ctx, &h); err != nil {
			return OrderResult{}, err
		}
		hops = append(hops, h)
		return OrderResult{OrderID: in.OrderID, Items: items, Hops: hops}, nil
	}

	var res ReserveResult
	if err := workflow.ExecuteActivity(actx, a.ReserveInventory, in.OrderID, items).Get(ctx, &res); err != nil {
		return OrderResult{}, err
	}
	hops = append(hops, res.Activity, res.Downstream)

	var pub Hop
	if err := workflow.ExecuteActivity(actx, a.PublishOrderEvent, in.OrderID, "reserved").Get(ctx, &pub); err != nil {
		return OrderResult{}, err
	}
	hops = append(hops, pub)

	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: "payment-" + in.OrderID})
	var payHops []Hop
	if err := workflow.ExecuteChildWorkflow(cctx, PaymentWorkflow, in.OrderID).Get(ctx, &payHops); err != nil {
		return OrderResult{}, err
	}
	hops = append(hops, payHops...)

	workflow.GetSignalChannel(ctx, SignalApprove).Receive(ctx, nil)
	hops = append(hops, wfHop(ctx, step("signal:"+SignalApprove), shim.SignalRouting(ctx, SignalApprove)))

	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return OrderResult{}, err
	}
	return OrderResult{}, workflow.NewContinueAsNewError(ctx, OrderWorkflow, OrderInput{
		OrderID: in.OrderID, Items: items, Generation: 1, Hops: hops,
	})
}

// PaymentWorkflow is the child workflow.
func PaymentWorkflow(ctx workflow.Context, orderID string) ([]Hop, error) {
	hops := []Hop{wfHop(ctx, "child:workflow-start", shim.RoutingFromWorkflow(ctx))}
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var a *Activities
	var h Hop
	if err := workflow.ExecuteActivity(actx, a.ChargePayment, orderID).Get(ctx, &h); err != nil {
		return nil, err
	}
	return append(hops, h), nil
}

// Activities holds the outbound dependencies.
type Activities struct {
	Inventory *inventory.Client
	Events    shimkafka.Producer
}

type ReserveResult struct {
	Activity   Hop `json:"activity"`
	Downstream Hop `json:"downstream"`
}

func activityHop(ctx context.Context, step string) Hop {
	info := activity.GetInfo(ctx)
	return Hop{
		Step:      step,
		Routing:   shim.RoutingFromContext(ctx),
		Namespace: info.WorkflowNamespace,
		TaskQueue: info.TaskQueue,
		Worker:    shim.WorkerIdentity(ctx),
	}
}

// ReserveInventory makes an outbound gRPC call; the server echoes what it saw.
func (a *Activities) ReserveInventory(ctx context.Context, orderID string, items []string) (ReserveResult, error) {
	out := ReserveResult{Activity: activityHop(ctx, "activity:ReserveInventory")}
	resp, err := a.Inventory.Reserve(ctx, &inventory.ReserveRequest{OrderID: orderID, Items: items})
	if err != nil {
		return out, err
	}
	out.Downstream = Hop{Step: "grpc:inventory.Reserve", Routing: resp.Observed, Worker: resp.Server}
	return out, nil
}

// OrderEvent is the Kafka payload.
type OrderEvent struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

// PublishOrderEvent produces a Kafka record; baggage goes in the headers.
func (a *Activities) PublishOrderEvent(ctx context.Context, orderID, status string) (Hop, error) {
	h := activityHop(ctx, "activity:PublishOrderEvent:"+status)
	body, _ := json.Marshal(OrderEvent{OrderID: orderID, Status: status})
	return h, a.Events.Publish(ctx, EventsTopic, []byte(orderID), body)
}

func (a *Activities) ChargePayment(ctx context.Context, orderID string) (Hop, error) {
	return activityHop(ctx, "child:activity:ChargePayment"), nil
}

// Register is the service's shim.Registrar: identical for production and
// sandbox workers.
func Register(acts *Activities) shim.Registrar {
	return func(r worker.Registry) {
		r.RegisterWorkflow(OrderWorkflow)
		r.RegisterWorkflow(PaymentWorkflow)
		r.RegisterActivity(acts)
	}
}
