#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
READER_BASE_URL="${TEMPORALSTORE_READER_BASE_URL:-}"
READER_MODEL="${TEMPORALSTORE_READER_MODEL:-google/flan-t5-small}"
READER_PROVIDER_NAME="${TEMPORALSTORE_READER_PROVIDER_NAME:-matrixark-cpp-oss-context}"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
LOC_INPUT="${TEMPORALSTORE_LOCOMO_INPUT:-/tmp/locomo10.json}"
LONGMEM_INPUT="${TEMPORALSTORE_LONGMEMEVAL_INPUT:-/tmp/longmemeval_s.json}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_DIR="${REPORT_DIR}/oss_reader_endpoint_${TIMESTAMP}"

mkdir -p "${ARCHIVE_DIR}"

write_manifest() {
  local locomo_status="$1"
  local longmem_status="$2"
  local phase="${3:-complete}"
  local error="${4:-}"
  cat >"${ARCHIVE_DIR}/manifest.json" <<JSON
{
  "archive_dir": "${ARCHIVE_DIR}",
  "reader_base_url": "${READER_BASE_URL}",
  "reader_provider_name": "${READER_PROVIDER_NAME}",
  "reader_model": "${READER_MODEL}",
  "locomo_input": "${LOC_INPUT}",
  "longmemeval_input": "${LONGMEM_INPUT}",
  "locomo_status": "${locomo_status}",
  "longmemeval_status": "${longmem_status}",
  "phase": "${phase}",
  "error": "${error}",
  "created_at_utc": "${TIMESTAMP}",
  "claim_level": "live_oss_reader_required"
}
JSON
}

if [[ -z "${READER_BASE_URL}" ]]; then
  write_manifest "not_run" "not_run" "missing_reader_base_url" \
    "set TEMPORALSTORE_READER_BASE_URL to the C++/OpenViking OpenAI-compatible /v1 endpoint"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 2
fi

if [[ ! -f "${LOC_INPUT}" && ! -f "${LONGMEM_INPUT}" ]]; then
  write_manifest "skipped_missing_input" "skipped_missing_input" "missing_inputs" \
    "mount LOCOMO or LongMemEval_s artifact and set TEMPORALSTORE_LOCOMO_INPUT/TEMPORALSTORE_LONGMEMEVAL_INPUT"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 2
fi

locomo_status="skipped_missing_input"
longmem_status="skipped_missing_input"

if [[ -f "${LOC_INPUT}" ]]; then
  python3 "${ROOT}/tools/run_locomo_90_hit_rate.py" \
    --threshold-profile oss_reader_full \
    --input "${LOC_INPUT}" \
    --reader-mode open-source \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-provider-name "${READER_PROVIDER_NAME}" \
    --reader-model "${READER_MODEL}" \
    --reader-no-fallback \
    --report "${ARCHIVE_DIR}/locomo_report.json" \
    --misses "${ARCHIVE_DIR}/locomo_misses.jsonl"
  python3 "${ROOT}/tools/archive_context_benchmark_report.py" \
    --report "${ARCHIVE_DIR}/locomo_report.json" \
    --input "${LOC_INPUT}" \
    --output "${ARCHIVE_DIR}/locomo_paper_comparable_report.json" \
    --claim-level "live_oss_reader_paper_comparable"
  locomo_status="passed"
fi

if [[ -f "${LONGMEM_INPUT}" ]]; then
  python3 "${ROOT}/tools/run_longmemeval_s_full_path.py" \
    --threshold-profile longmemeval_full \
    --input "${LONGMEM_INPUT}" \
    --reader-mode open-source \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-provider-name "${READER_PROVIDER_NAME}" \
    --reader-model "${READER_MODEL}" \
    --reader-no-fallback \
    --require-open-source-reader \
    --report "${ARCHIVE_DIR}/longmemeval_s_report.json" \
    --misses "${ARCHIVE_DIR}/longmemeval_s_misses.jsonl"
  python3 "${ROOT}/tools/archive_context_benchmark_report.py" \
    --report "${ARCHIVE_DIR}/longmemeval_s_report.json" \
    --input "${LONGMEM_INPUT}" \
    --output "${ARCHIVE_DIR}/longmemeval_s_paper_comparable_report.json" \
    --claim-level "live_oss_reader_paper_comparable"
  longmem_status="passed"
fi

write_manifest "${locomo_status}" "${longmem_status}"
echo "Archived OSS reader benchmark output under ${ARCHIVE_DIR}"
