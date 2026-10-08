// Package shimtest wires shim test runs into `go test`.
package shimtest

import (
	"context"
	"os"
	"os/user"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/adam-quan/dd-poc/pkg/shim"
)

// SandboxName defaults to $SANDBOX_NAME, then the OS user name.
func SandboxName() string {
	if n := os.Getenv("SANDBOX_NAME"); n != "" {
		return n
	}
	if u, err := user.Current(); err == nil {
		return strings.Trim(regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(u.Username), "-"), "-")
	}
	return "dev"
}

// StartTestRun starts a sandbox test run and closes it (worker, route, task
// queue leftovers) when the test finishes, pass or fail.
func StartTestRun(t testing.TB, c *shim.Client, opts shim.TestRunOptions) *shim.TestRun {
	t.Helper()
	if opts.SBR == "" && opts.SandboxName == "" {
		opts.SandboxName = SandboxName()
	}
	run, err := c.StartTestRun(context.Background(), opts)
	if err != nil {
		t.Fatalf("start test run: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := run.Close(ctx); err != nil {
			t.Errorf("close test run %s: %v", run.RunID(), err)
		}
	})
	return run
}
