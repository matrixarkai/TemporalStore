#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-rust-target}"

cd "$ROOT"

CARGO_TARGET_DIR="$TARGET_DIR" cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_SCALE_NODES:-3}" \
  --string-ops "${TS_SCALE_STRING_OPS:-1000}" \
  --hash-ops "${TS_SCALE_HASH_OPS:-250}" \
  --sequence-keys "${TS_SCALE_SEQUENCE_KEYS:-4}" \
  --sequence-len "${TS_SCALE_SEQUENCE_LEN:-500}" \
  --scale-events "${TS_SCALE_EVENTS:-2}" \
  --failover-every "${TS_SCALE_FAILOVER_EVERY:-250}" \
  --read-sample-every "${TS_SCALE_READ_SAMPLE_EVERY:-100}"
