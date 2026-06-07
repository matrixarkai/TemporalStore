#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_MODE="${AWS_MODE:-0}"

cd "${ROOT}"

echo "== local snapshot smoke =="
cargo test -p temporalstore-snapshot

if [[ "${AWS_MODE}" == "1" ]]; then
  if [[ -z "${TS_SNAPSHOT_AWS_BUCKET:-}" ]]; then
    echo "TS_SNAPSHOT_AWS_BUCKET is required when AWS_MODE=1" >&2
    exit 1
  fi
  command -v aws >/dev/null 2>&1 || {
    echo "aws CLI is required when AWS_MODE=1" >&2
    exit 1
  }
  export TS_SNAPSHOT_AWS_PREFIX="${TS_SNAPSHOT_AWS_PREFIX:-temporalstore-rust-snapshot-smoke/$(date +%Y%m%d_%H%M%S)}"
  echo "== aws snapshot smoke =="
  echo "bucket=${TS_SNAPSHOT_AWS_BUCKET}"
  echo "prefix=${TS_SNAPSHOT_AWS_PREFIX}"
  cargo test -p temporalstore-snapshot --test aws_snapshot_smoke -- --ignored --nocapture
else
  echo "AWS smoke skipped; set AWS_MODE=1 TS_SNAPSHOT_AWS_BUCKET=<bucket> to run it."
fi
