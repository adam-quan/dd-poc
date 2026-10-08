#!/usr/bin/env bash
# One single-node Temporal cluster (dev server) with two namespaces:
#   production  <- ptid=dashprod
#   sandbox     <- ptid=dashtest
# plus the routing search attributes the shim stamps on every workflow.
set -euo pipefail
exec temporal server start-dev \
  --namespace production \
  --namespace sandbox \
  --search-attribute Ptid=Keyword \
  --search-attribute Sbr=Keyword \
  --search-attribute Rid=Keyword \
  --log-level warn \
  "$@"
