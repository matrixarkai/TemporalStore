#!/usr/bin/env bash
# MatrixArk context backfill CI gate for Ubuntu 22.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RECORDS="${MATRIXARK_BACKFILL_CI_RECORDS:-128}"
BATCH_SIZES="${MATRIXARK_BACKFILL_CI_BATCH_SIZES:-32,64}"
INCREMENTAL_RECORDS="${MATRIXARK_BACKFILL_CI_INCREMENTAL_RECORDS:-32}"
REPEAT="${MATRIXARK_BACKFILL_CI_REPEAT:-2}"
JSON_OUTPUT="${MATRIXARK_BACKFILL_CI_JSON_OUTPUT:-matrixark_context_backfill_readiness.json}"

python3 -m py_compile \
  tools/matrixark_context_backfill.py \
  tools/matrixark_context_backfill_benchmark.py \
  tools/matrixark_dual_write_ingestion_benchmark.py \
  tools/validate_matrixark_context_backfill_readiness.py \
  tools/validate_open_source_readiness.py

python3 tools/test_matrixark_context_backfill.py
python3 tools/test_matrixark_context_backfill_benchmark.py
python3 tools/test_matrixark_dual_write_ingestion_benchmark.py
python3 tools/test_validate_matrixark_context_backfill_readiness.py
python3 tools/test_validate_open_source_readiness.py
python3 tools/validate_open_source_readiness.py
python3 tools/validate_matrixark_context_backfill_readiness.py \
  --records="${RECORDS}" \
  --batch-sizes="${BATCH_SIZES}" \
  --incremental-records="${INCREMENTAL_RECORDS}" \
  --repeat="${REPEAT}" \
  --json-output="${JSON_OUTPUT}"

echo "matrixark_context_backfill_ci_gate_status=ok"
echo "matrixark_context_backfill_readiness_json=${JSON_OUTPUT}"
