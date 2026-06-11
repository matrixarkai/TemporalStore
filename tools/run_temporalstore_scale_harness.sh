#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FLAG=("--release")
CARGO_PREFIX=()

if [[ "${TS_SCALE_PROFILE:-release}" == "debug" ]]; then
  PROFILE_FLAG=()
fi

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  CARGO_PREFIX=(env "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
fi

cd "$ROOT"

EXTRA_ARGS=()
if [[ -n "${TS_SCALE_SHARED_STORE_ROOT:-}" ]]; then
  EXTRA_ARGS+=(--shared-store-root "${TS_SCALE_SHARED_STORE_ROOT}")
fi

"${CARGO_PREFIX[@]}" cargo run "${PROFILE_FLAG[@]}" -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_SCALE_NODES:-3}" \
  --string-ops "${TS_SCALE_STRING_OPS:-40}" \
  --hash-ops "${TS_SCALE_HASH_OPS:-10}" \
  --sequence-keys "${TS_SCALE_SEQUENCE_KEYS:-2}" \
  --sequence-len "${TS_SCALE_SEQUENCE_LEN:-100}" \
  --scale-events "${TS_SCALE_EVENTS:-2}" \
  --failover-every "${TS_SCALE_FAILOVER_EVERY:-10}" \
  --read-sample-every "${TS_SCALE_READ_SAMPLE_EVERY:-10}" \
  --max-log-entry-bytes "${TS_SCALE_MAX_LOG_ENTRY_BYTES:-32768}" \
  --compare-shared-store "${TS_SCALE_COMPARE_SHARED_STORE:-false}" \
  --shared-store-ops "${TS_SCALE_SHARED_STORE_OPS:-1000}" \
  --shared-store-flush-every "${TS_SCALE_SHARED_STORE_FLUSH_EVERY:-25}" \
  "${EXTRA_ARGS[@]}"
