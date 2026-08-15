#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_LOCAL_SCALE=false
RUN_DISTRIBUTED_RAFT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-local-scale)
      RUN_LOCAL_SCALE=true
      shift
      ;;
    --run-distributed-raft)
      RUN_DISTRIBUTED_RAFT=true
      shift
      ;;
    --help|-h)
      cat <<'USAGE'
usage: tools/run_ops_scale_readiness.sh [--run-local-scale] [--run-distributed-raft]

Runs the ops/scale readiness evidence gate. By default it validates that the
production evidence contract, docs, dashboards, harnesses, and unified corpus
are present. Optional flags run the heavier local scale and distributed Raft
process harnesses.
USAGE
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

cargo run -p temporalstore-rust --bin ops_scale_readiness_harness

if [[ "${RUN_LOCAL_SCALE}" == "true" ]]; then
  TS_SCALE_PROFILE="${TS_SCALE_PROFILE:-debug}" \
  TS_SCALE_NODES="${TS_SCALE_NODES:-3}" \
  TS_SCALE_STRING_OPS="${TS_SCALE_STRING_OPS:-40}" \
  TS_SCALE_HASH_OPS="${TS_SCALE_HASH_OPS:-10}" \
  TS_SCALE_SEQUENCE_KEYS="${TS_SCALE_SEQUENCE_KEYS:-2}" \
  TS_SCALE_SEQUENCE_LEN="${TS_SCALE_SEQUENCE_LEN:-100}" \
  TS_SCALE_EVENTS="${TS_SCALE_EVENTS:-2}" \
  TS_SCALE_FAILOVER_EVERY="${TS_SCALE_FAILOVER_EVERY:-10}" \
  TS_SCALE_READ_SAMPLE_EVERY="${TS_SCALE_READ_SAMPLE_EVERY:-10}" \
  TS_SCALE_COMPARE_SHARED_STORE="${TS_SCALE_COMPARE_SHARED_STORE:-true}" \
  TS_SCALE_SHARED_STORE_OPS="${TS_SCALE_SHARED_STORE_OPS:-50}" \
  TS_SCALE_SHARED_STORE_FLUSH_EVERY="${TS_SCALE_SHARED_STORE_FLUSH_EVERY:-10}" \
    tools/run_temporalstore_scale_harness.sh
fi

if [[ "${RUN_DISTRIBUTED_RAFT}" == "true" ]]; then
  cargo run -p temporalstore-rust --bin distributed_raft_harness -- \
    --root "${TS_DISTRIBUTED_RAFT_ROOT:-/tmp/temporalstore-distributed-raft-ops-scale}" \
    --auth-token "${TS_DISTRIBUTED_RAFT_AUTH_TOKEN:-local-raft-token}"
fi
