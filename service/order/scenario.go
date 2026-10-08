package order

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/client"

	"github.com/adam-quan/dd-poc/pkg/shim"
	"github.com/adam-quan/dd-poc/pkg/shim/shimkafka"
)

// Report is everything observed while driving one order end to end.
type Report struct {
	WorkflowID string
	FirstRunID string // run before continue-as-new
	Route      shim.Route
	Hops       []Hop
}

// RunScenario drives an order through start -> update -> query -> signal ->
// continue-as-new -> completion, using only the shim client and the baggage
// on ctx.
func RunScenario(ctx context.Context, c *shim.Client, orderID string) (Report, error) {
	route, ctx, err := c.Resolve(ctx) // ctx now carries the effective baggage
	if err != nil {
		return Report{}, err
	}
	rep := Report{Route: route}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-" + orderID}, OrderWorkflow,
		OrderInput{OrderID: orderID, Items: []string{"burger"}})
	if err != nil {
		return rep, fmt.Errorf("start: %w", err)
	}
	rep.WorkflowID, rep.FirstRunID = run.GetID(), run.GetRunID()

	// Update and query use the business ID; the shim scopes it per run.
	uh, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: "order-" + orderID, UpdateName: UpdateAddItem, Args: []any{"fries"},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return rep, fmt.Errorf("update: %w", err)
	}
	if err := uh.Get(ctx, nil); err != nil {
		return rep, fmt.Errorf("update result: %w", err)
	}

	qv, err := c.QueryWorkflow(ctx, "order-"+orderID, "", QueryHops)
	if err != nil {
		return rep, fmt.Errorf("query: %w", err)
	}
	var qr QueryResult
	if err := qv.Get(&qr); err != nil {
		return rep, err
	}

	if err := c.SignalWorkflow(ctx, "order-"+orderID, "", SignalApprove, nil); err != nil {
		return rep, fmt.Errorf("signal: %w", err)
	}

	final, err := c.GetWorkflow(ctx, "order-"+orderID, "") // follows continue-as-new
	if err != nil {
		return rep, err
	}
	var res OrderResult
	if err := final.Get(ctx, &res); err != nil {
		return rep, fmt.Errorf("result: %w", err)
	}
	rep.Hops = append(res.Hops, Hop{Step: "query:" + QueryHops, Routing: qr.Query})
	return rep, nil
}

// ConsumerHop is what a Kafka consumer of order events observes.
func ConsumerHop(msg kafka.Message) (Hop, OrderEvent) {
	var ev OrderEvent
	_ = json.Unmarshal(msg.Value, &ev)
	ctx := shimkafka.Extract(context.Background(), msg)
	return Hop{Step: "kafka:consume:" + ev.Status, Routing: shim.RoutingFromContext(ctx), Worker: "order-events-consumer"}, ev
}

// Expectation describes where every hop of a scenario must have run.
type Expectation struct {
	Routing      shim.Routing
	Namespace    string
	TaskQueue    string
	WorkerPrefix string // activity hops must run on a worker with this identity prefix
}

// Verify returns one error per hop that saw the wrong baggage or ran in the
// wrong place.
func Verify(rep Report, exp Expectation) []error {
	var errs []error
	if len(rep.Hops) == 0 {
		return []error{fmt.Errorf("no hops recorded")}
	}
	seen := map[string]bool{}
	for _, h := range rep.Hops {
		seen[stepKind(h.Step)] = true
		if h.Routing != exp.Routing {
			errs = append(errs, fmt.Errorf("%s: baggage %s, want %s", h.Step, h.Routing, exp.Routing))
		}
		if h.Namespace != "" && h.Namespace != exp.Namespace {
			errs = append(errs, fmt.Errorf("%s: namespace %s, want %s", h.Step, h.Namespace, exp.Namespace))
		}
		if h.TaskQueue != "" && h.TaskQueue != exp.TaskQueue {
			errs = append(errs, fmt.Errorf("%s: task queue %s, want %s", h.Step, h.TaskQueue, exp.TaskQueue))
		}
		if strings.Contains(h.Step, "activity:") && !strings.HasPrefix(h.Worker, exp.WorkerPrefix) {
			errs = append(errs, fmt.Errorf("%s: ran on worker %q, want prefix %q", h.Step, h.Worker, exp.WorkerPrefix))
		}
	}
	for _, k := range []string{"workflow-start", "activity", "grpc", "child", "update", "query", "signal", "after-can"} {
		if !seen[k] {
			errs = append(errs, fmt.Errorf("missing %s hop", k))
		}
	}
	return errs
}

func stepKind(step string) string {
	switch {
	case strings.HasPrefix(step, "gen1:workflow-start"):
		return "after-can"
	case strings.HasPrefix(step, "child:"):
		return "child"
	case strings.HasPrefix(step, "grpc:"):
		return "grpc"
	case strings.HasPrefix(step, "query:"):
		return "query"
	case strings.Contains(step, "update:"):
		return "update"
	case strings.Contains(step, "signal:"):
		return "signal"
	case strings.Contains(step, "activity:"):
		return "activity"
	case strings.Contains(step, "workflow-start"):
		return "workflow-start"
	}
	return step
}

// PrintHops renders a report as a table.
func PrintHops(w io.Writer, title string, rep Report) {
	fmt.Fprintf(w, "\n== %s ==\n   workflow %s  ->  namespace=%s taskQueue=%s\n   (%s)\n",
		title, rep.WorkflowID, rep.Route.Namespace, rep.Route.TaskQueue, rep.Route.Reason)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "   STEP\tPTID\tSBR\tRID\tNAMESPACE\tTASK QUEUE\tWORKER")
	for _, h := range rep.Hops {
		fmt.Fprintf(tw, "   %s\t%s\t%s\t%s\t%s\t%s\t%s\n", h.Step, h.Routing.PTID, dash(h.Routing.SBR), dash(h.Routing.RID),
			dash(h.Namespace), dash(h.TaskQueue), dash(h.Worker))
	}
	_ = tw.Flush()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
