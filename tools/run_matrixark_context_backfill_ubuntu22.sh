#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_ID="${JOB_ID:-$(date +%Y%m%d%H%M%S)}"
SOURCE_PREFIX="${SOURCE_PREFIX:-matrixark:mcp:raw_ingestion}"
RAW_BACKEND="${RAW_BACKEND:-${MATRIXARK_RAW_INGESTION_BACKEND:-temporalstore}}"
TARGET_PREFIX="${TARGET_PREFIX:-matrixark:context_backfill:${JOB_ID}}"
MODE="${MODE:-shadow}"
DRY_RUN="${DRY_RUN:-1}"
RESUME="${RESUME:-1}"
BATCH_SIZE="${BATCH_SIZE:-256}"
PARTIAL="${PARTIAL:-0}"
PARTIAL_RECORD_TYPES="${PARTIAL_RECORD_TYPES:-}"
PARTIAL_TENANT_IDS="${PARTIAL_TENANT_IDS:-}"
PARTIAL_USER_IDS="${PARTIAL_USER_IDS:-}"
PARTIAL_SESSION_IDS="${PARTIAL_SESSION_IDS:-}"
PARTIAL_FILTER_JSON="${PARTIAL_FILTER_JSON:-}"
PARTIAL_REQUIRE_BOUNDED="${PARTIAL_REQUIRE_BOUNDED:-1}"
# The documented spelling first, then the bare one, then the value the shipped config
# declares. This wrapper defaulted to 127.0.0.1:65000, which no deployment listens on.
METASERVER="${METASERVER:-${MATRIXARK_TEMPORALSTORE_METASERVER:-${MATRIXARK_METASERVER:-127.0.0.1:18000}}}"
# Same correction as the line above, which this missed. These defaulted to matrixark/context
# while the shipped config, every running process and both Python readers say deploy_ns and
# deploy_table -- so a run without NAMESPACE set backfilled a store the deployment does not read,
# and reported success, because writing into an empty namespace succeeds. mx#1056 struck exactly
# this out of two PYTHON tools; the guard that keeps it struck scans .py, so the wrapper kept it.
NAMESPACE="${NAMESPACE:-${MATRIXARK_NAMESPACE:-deploy_ns}}"
TABLE="${TABLE:-${MATRIXARK_TABLE:-deploy_table}}"
PROM_OUTPUT="${PROM_OUTPUT:-/tmp/matrixark_context_backfill_${JOB_ID}.prom}"

exec python3 "${ROOT}/tools/matrixark_context_backfill.py" \
  --metaserver "${METASERVER}" \
  --namespace "${NAMESPACE}" \
  --table "${TABLE}" \
  --source-prefix "${SOURCE_PREFIX}" \
  --raw-backend "${RAW_BACKEND}" \
  --target-prefix "${TARGET_PREFIX}" \
  --mode "${MODE}" \
  --job-id "${JOB_ID}" \
  --batch-size "${BATCH_SIZE}" \
  --partial "${PARTIAL}" \
  --partial-record-types "${PARTIAL_RECORD_TYPES}" \
  --partial-tenant-ids "${PARTIAL_TENANT_IDS}" \
  --partial-user-ids "${PARTIAL_USER_IDS}" \
  --partial-session-ids "${PARTIAL_SESSION_IDS}" \
  --partial-filter-json "${PARTIAL_FILTER_JSON}" \
  --partial-require-bounded "${PARTIAL_REQUIRE_BOUNDED}" \
  --dry-run "${DRY_RUN}" \
  --resume "${RESUME}" \
  --prometheus-output "${PROM_OUTPUT}" \
  "$@"
