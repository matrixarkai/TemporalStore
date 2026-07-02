#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_ID="${JOB_ID:-$(date +%Y%m%d%H%M%S)}"
SOURCE_PREFIX="${SOURCE_PREFIX:-matrixark:mcp}"
TARGET_PREFIX="${TARGET_PREFIX:-matrixark:context_backfill:${JOB_ID}}"
MODE="${MODE:-shadow}"
DRY_RUN="${DRY_RUN:-1}"
RESUME="${RESUME:-1}"
BATCH_SIZE="${BATCH_SIZE:-256}"
METASERVER="${METASERVER:-${MATRIXARK_METASERVER:-127.0.0.1:65000}}"
NAMESPACE="${NAMESPACE:-${MATRIXARK_NAMESPACE:-matrixark}}"
TABLE="${TABLE:-${MATRIXARK_TABLE:-context}}"
PROM_OUTPUT="${PROM_OUTPUT:-/tmp/matrixark_context_backfill_${JOB_ID}.prom}"

exec python3 "${ROOT}/tools/matrixark_context_backfill.py" \
  --metaserver "${METASERVER}" \
  --namespace "${NAMESPACE}" \
  --table "${TABLE}" \
  --source-prefix "${SOURCE_PREFIX}" \
  --target-prefix "${TARGET_PREFIX}" \
  --mode "${MODE}" \
  --job-id "${JOB_ID}" \
  --batch-size "${BATCH_SIZE}" \
  --dry-run "${DRY_RUN}" \
  --resume "${RESUME}" \
  --prometheus-output "${PROM_OUTPUT}" \
  "$@"