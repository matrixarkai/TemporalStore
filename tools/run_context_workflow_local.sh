#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-context-workflow-target}"
export CARGO_TARGET_DIR="${TARGET_DIR}"

echo "== context workflow: harness =="
cargo run -p temporalstore-rust --bin context_workflow_harness \
  > /tmp/temporalstore-context-workflow-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/temporalstore-context-workflow-validation.log

echo "== context workflow: unit tests =="
cargo test -p temporalstore-rust context_workflow -- --test-threads=1

echo "Context workflow local validation passed."
