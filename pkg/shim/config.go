package shim

import (
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Config describes one service's view of the shared Temporal cluster.
type Config struct {
	// HostPort of the Temporal frontend. Default 127.0.0.1:7233 (the dev
	// server listens on IPv4 only; resolving "localhost" can stall dials).
	HostPort string
	// ProdNamespace receives ptid=dashprod traffic. Default "production".
	ProdNamespace string
	// SandboxNamespace receives ptid=dashtest traffic. Default "sandbox".
	SandboxNamespace string
	// Service is the logical service name. Its production task queue (in both
	// namespaces) is the service name itself.
	Service string
	// LeaseTTL is how long a sandbox routing lease lives without renewal. A
	// crashed test run stops receiving traffic, and its leftovers are reaped,
	// once this elapses. Default 30s.
	LeaseTTL time.Duration
	// SearchAttributes stamps Ptid/Sbr/Rid keyword search attributes on
	// workflows started through the shim. They must be registered in both
	// namespaces. Default true; set DisableSearchAttributes to opt out.
	DisableSearchAttributes bool
	// TracerProvider used for the spans that carry baggage. Defaults to an SDK
	// provider without exporters: spans are valid (so baggage propagates) but
	// are not shipped anywhere.
	TracerProvider trace.TracerProvider
	// Logger for shim decisions. Default slog.Default().
	Logger *slog.Logger
	// SDKLogLevel for the Temporal SDK's own logger. Default slog.LevelWarn.
	SDKLogLevel slog.Level
}

// ConfigFromEnv fills a Config from TEMPORAL_ADDRESS, PROD_NAMESPACE and
// SANDBOX_NAMESPACE.
func ConfigFromEnv(service string) Config {
	return Config{
		HostPort:         os.Getenv("TEMPORAL_ADDRESS"),
		ProdNamespace:    os.Getenv("PROD_NAMESPACE"),
		SandboxNamespace: os.Getenv("SANDBOX_NAMESPACE"),
		Service:          service,
	}
}

func (c Config) withDefaults() Config {
	if c.HostPort == "" {
		c.HostPort = "127.0.0.1:7233"
	}
	if c.ProdNamespace == "" {
		c.ProdNamespace = "production"
	}
	if c.SandboxNamespace == "" {
		c.SandboxNamespace = "sandbox"
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.SDKLogLevel == 0 {
		c.SDKLogLevel = slog.LevelWarn
	}
	return c
}
