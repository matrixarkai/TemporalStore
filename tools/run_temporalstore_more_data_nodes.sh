#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FLAG=("--release")
CARGO_PREFIX=()

if [[ "${TS_MORE_NODES_PROFILE:-release}" == "debug" ]]; then
  PROFILE_FLAG=()
fi

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  CARGO_PREFIX=(env "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
fi

cd "$ROOT"

EXTRA_ARGS=()
if [[ -n "${TS_MORE_NODES_SHARED_STORE_ROOT:-}" ]]; then
  EXTRA_ARGS+=(--shared-store-root "${TS_MORE_NODES_SHARED_STORE_ROOT}")
fi

"${CARGO_PREFIX[@]}" cargo run "${PROFILE_FLAG[@]}" -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_MORE_NODES:-7}" \
  --string-ops "${TS_MORE_NODES_STRING_OPS:-2000}" \
  --hash-ops "${TS_MORE_NODES_HASH_OPS:-500}" \
  --sequence-keys "${TS_MORE_NODES_SEQUENCE_KEYS:-4}" \
  --sequence-len "${TS_MORE_NODES_SEQUENCE_LEN:-1000}" \
  --scale-events "${TS_MORE_NODES_SCALE_EVENTS:-6}" \
  --failover-every "${TS_MORE_NODES_FAILOVER_EVERY:-250}" \
  --read-sample-every "${TS_MORE_NODES_READ_SAMPLE_EVERY:-10}" \
  --max-log-entry-bytes "${TS_MORE_NODES_MAX_LOG_ENTRY_BYTES:-32768}" \
  --compare-shared-store "${TS_MORE_NODES_COMPARE_SHARED_STORE:-true}" \
  --shared-store-ops "${TS_MORE_NODES_SHARED_STORE_OPS:-2000}" \
  --shared-store-flush-every "${TS_MORE_NODES_SHARED_STORE_FLUSH_EVERY:-20}" \
  "${EXTRA_ARGS[@]}"
