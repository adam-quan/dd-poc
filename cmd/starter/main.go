// Command starter sends traffic with arbitrary baggage, like an upstream
// service or the edge would, and prints where each hop ran.
//
//	starter -ptid dashprod                       # production traffic
//	starter -ptid dashtest                       # test traffic, no sandbox
//	starter -ptid dashtest -sbr order-service-web-sandbox-alice   # into alice's live run
//	starter -leases                              # list active sandbox leases
//	starter -ptid dashtest -sbr ... -resolve     # only print the routing decision
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
	"github.com/temporalio/doordash-sandbox-poc/service/order"
)

func main() {
	ptid := flag.String("ptid", shim.TenantProd, "tenant: dashprod or dashtest")
	sbr := flag.String("sbr", "", "sandbox routing key")
	rid := flag.String("rid", "", "run ID")
	orderID := flag.String("order", fmt.Sprintf("%d", time.Now().Unix()%100000), "business order ID")
	leases := flag.Bool("leases", false, "list active sandbox leases and exit")
	resolve := flag.Bool("resolve", false, "print the routing decision only")
	flag.Parse()

	c, err := shim.Dial(shim.ConfigFromEnv(order.Service))
	if err != nil {
		slog.Error("dial", "err", err)
		os.Exit(1)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if *leases {
		ls, err := c.Leases(ctx)
		if err != nil {
			slog.Error("leases", "err", err)
			os.Exit(1)
		}
		fmt.Printf("%d active sandbox lease(s)\n", len(ls))
		for _, l := range ls {
			fmt.Printf("  %s  rid=%s  queue=%s  expires=%s\n", l.SBR, l.RID, l.TaskQueue, l.ExpiresAt.Local().Format(time.TimeOnly))
		}
		return
	}

	ctx, err = shim.WithRouting(ctx, shim.Routing{PTID: *ptid, SBR: *sbr, RID: *rid})
	if err != nil {
		slog.Error("baggage", "err", err)
		os.Exit(1)
	}
	if *resolve {
		route, _, err := c.Resolve(ctx)
		if err != nil {
			slog.Error("resolve", "err", err)
			os.Exit(1)
		}
		fmt.Printf("route: namespace=%s taskQueue=%s (%s)\n", route.Namespace, route.TaskQueue, route.Reason)
		return
	}
	rep, err := order.RunScenario(ctx, c, *orderID)
	if err != nil {
		slog.Error("scenario", "err", err)
		os.Exit(1)
	}
	order.PrintHops(os.Stdout, fmt.Sprintf("ptid=%s sbr=%s rid=%s", *ptid, *sbr, *rid), rep)
}
