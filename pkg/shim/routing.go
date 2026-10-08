// Package shim is a thin layer over the Temporal Go SDK that turns
// OpenTelemetry baggage (ptid / sbr / rid) into Temporal routing decisions.
//
//	ptid  Pedregal tenant ID. "dashprod" -> production namespace,
//	      "dashtest" -> sandbox namespace.
//	sbr   Sandbox routing key, "<service>-<app>-sandbox-<name>".
//	rid   Run ID, unique per developer test run. sbr+rid identify the
//	      workflows, the task queue and the workers of one test run.
//
// The baggage travels in the standard OTel W3C baggage header, carried by
// Temporal's OpenTelemetry tracing interceptor through workflow start,
// activities, child workflows, signals, updates, queries and
// continue-as-new, and by the shim's gRPC / Kafka carriers beyond Temporal.
package shim

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/baggage"
)

// Baggage member keys.
const (
	KeyPTID = "ptid"
	KeySBR  = "sbr"
	KeyRID  = "rid"
)

// Tenant values for ptid.
const (
	TenantProd = "dashprod"
	TenantTest = "dashtest"
)

var (
	sbrPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*-sandbox-[a-z0-9]+(?:-[a-z0-9]+)*$`)
	ridPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{3,62}$`)
)

// Routing is the routing-relevant subset of the request baggage.
type Routing struct {
	PTID string `json:"ptid"`
	SBR  string `json:"sbr,omitempty"`
	RID  string `json:"rid,omitempty"`
}

func (r Routing) IsProd() bool { return r.PTID == TenantProd }
func (r Routing) IsTest() bool { return r.PTID == TenantTest }
func (r Routing) IsZero() bool { return r == Routing{} }

func (r Routing) String() string {
	return fmt.Sprintf("ptid=%s sbr=%s rid=%s", r.PTID, orDash(r.SBR), orDash(r.RID))
}

// Validate checks the values are well-formed. It does not check that a
// sandbox exists for sbr.
func (r Routing) Validate() error {
	switch r.PTID {
	case TenantProd, TenantTest:
	case "":
		return fmt.Errorf("shim: missing %q baggage; every request must carry a tenant", KeyPTID)
	default:
		return fmt.Errorf("shim: unknown %s %q (want %s or %s)", KeyPTID, r.PTID, TenantProd, TenantTest)
	}
	if r.SBR != "" && !sbrPattern.MatchString(r.SBR) {
		return fmt.Errorf("shim: malformed %s %q (want <service>-<app>-sandbox-<name>)", KeySBR, r.SBR)
	}
	if r.RID != "" && !ridPattern.MatchString(r.RID) {
		return fmt.Errorf("shim: malformed %s %q", KeyRID, r.RID)
	}
	return nil
}

// SBR builds a sandbox routing key: <service>-<app>-sandbox-<sandboxName>.
func SBR(service, app, sandboxName string) string {
	return fmt.Sprintf("%s-%s-sandbox-%s", service, app, sandboxName)
}

// SBRService reports whether sbr addresses a sandbox of the given service.
func SBRService(sbr, service string) bool {
	return strings.HasPrefix(sbr, service+"-") && strings.Contains(sbr, "-sandbox-")
}

// NewRunID returns a fresh, task-queue-safe run ID such as
// "r20261007t171503-9f2c4e1a".
func NewRunID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "r" + strings.ToLower(time.Now().UTC().Format("20060102t150405")) + "-" + hex.EncodeToString(b[:])
}

// SandboxTaskQueue is the per-run task queue name for sbr+rid.
func SandboxTaskQueue(sbr, rid string) string {
	return "sbx." + sbr + "." + rid
}

// WithRouting returns ctx with r merged into its OTel baggage. Empty fields
// remove the corresponding member so stale values never leak.
func WithRouting(ctx context.Context, r Routing) (context.Context, error) {
	if err := r.Validate(); err != nil {
		return ctx, err
	}
	bag := baggage.FromContext(ctx)
	for k, v := range map[string]string{KeyPTID: r.PTID, KeySBR: r.SBR, KeyRID: r.RID} {
		if v == "" {
			bag = bag.DeleteMember(k)
			continue
		}
		m, err := baggage.NewMember(k, v)
		if err != nil {
			return ctx, err
		}
		if bag, err = bag.SetMember(m); err != nil {
			return ctx, err
		}
	}
	return baggage.ContextWithBaggage(ctx, bag), nil
}

// MustWithRouting is WithRouting that panics on invalid input.
func MustWithRouting(ctx context.Context, r Routing) context.Context {
	ctx, err := WithRouting(ctx, r)
	if err != nil {
		panic(err)
	}
	return ctx
}

// ProdContext marks ctx as production traffic.
func ProdContext(ctx context.Context) context.Context {
	return MustWithRouting(ctx, Routing{PTID: TenantProd})
}

// RoutingFromContext reads routing from the OTel baggage on ctx. Works in
// clients, activities, gRPC handlers and Kafka consumers.
func RoutingFromContext(ctx context.Context) Routing {
	return routingFromBaggage(baggage.FromContext(ctx))
}

func routingFromBaggage(bag baggage.Baggage) Routing {
	return Routing{
		PTID: bag.Member(KeyPTID).Value(),
		SBR:  bag.Member(KeySBR).Value(),
		RID:  bag.Member(KeyRID).Value(),
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
