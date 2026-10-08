#!/usr/bin/env bash
# End-to-end demo against a running dev server (scripts/dev-server.sh).
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
LOG=.demo-logs
rm -rf "$LOG" && mkdir -p "$LOG"

bin/inventory  >"$LOG/inventory.log" 2>&1 & INV=$!
bin/prodworker >"$LOG/prodworker.log" 2>&1 & PW=$!
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
sleep 2

banner() { printf '\n\033[1m######## %s ########\033[0m\n' "$*"; }

banner "1. alice + bob testing order-service at once, alongside production and un-sandboxed test traffic (same order ID everywhere)"
bin/devtest -name alice                                               >"$LOG/alice.log" 2>&1 & A=$!
bin/devtest -name bob                                                 >"$LOG/bob.log"   2>&1 & B=$!
bin/starter -ptid dashprod -order 1001                                >"$LOG/prod.log"  2>&1 & P=$!
bin/starter -ptid dashtest -sbr order-service-web-sandbox-carol -order 1001 >"$LOG/carol.log" 2>&1 & C=$!
wait $A $B $P $C
for f in alice bob prod carol; do cat "$LOG/$f.log"; done

banner "2. sbr-only traffic (e.g. from a UI) reaches a live sandbox; the shim fills in its rid"
bin/devtest -name dana -orders 0 -hold 60s >"$LOG/dana.log" 2>&1 & D=$!
sleep 3
bin/starter -leases
bin/starter -ptid dashtest -sbr order-service-web-sandbox-dana -order 7

banner "3. dana's run ends -> worker, lease and task queue leftovers are cleaned up automatically"
kill -INT $D; wait $D || true
cat "$LOG/dana.log"
bin/starter -leases
bin/starter -ptid dashtest -sbr order-service-web-sandbox-dana -resolve

banner "Downstream Kafka consumer (in the prod worker) saw these baggages"
grep "kafka consumer received" "$LOG/prodworker.log" | sed 's/.*order=/  order=/' || true
