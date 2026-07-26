#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
READER_BASE_URL="${TEMPORALSTORE_READER_BASE_URL:-http://127.0.0.1:11434/v1}"
READER_MODEL="${TEMPORALSTORE_READER_MODEL:-qwen2.5:7b}"
READER_PROVIDER_NAME="${TEMPORALSTORE_READER_PROVIDER_NAME:-qwen25-7b-ollama}"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
LOC_INPUT="${TEMPORALSTORE_LOCOMO_INPUT:-/tmp/locomo10.json}"
LONGMEM_INPUT="${TEMPORALSTORE_LONGMEMEVAL_INPUT:-/tmp/longmemeval_s.json}"
OPENVIKING_ARCHIVE="${OPENVIKING_LOCOMO_ARCHIVE:-/tmp/openviking_matrixark_oss/data/messages.jsonl}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_DIR="${REPORT_DIR}/oss_memory_ready_${TIMESTAMP}"

mkdir -p "${ARCHIVE_DIR}"

write_manifest() {
  local status="$1"
  local phase="$2"
  local error="${3:-}"
  cat >"${ARCHIVE_DIR}/manifest.json" <<JSON
{
  "status": "${status}",
  "phase": "${phase}",
  "error": "${error}",
  "archive_dir": "${ARCHIVE_DIR}",
  "reader_base_url": "${READER_BASE_URL}",
  "reader_model": "${READER_MODEL}",
  "reader_provider_name": "${READER_PROVIDER_NAME}",
  "locomo_input": "${LOC_INPUT}",
  "longmemeval_input": "${LONGMEM_INPUT}",
  "openviking_archive": "${OPENVIKING_ARCHIVE}",
  "created_at_utc": "${TIMESTAMP}",
  "claim_level": "live_oss_reader_required_fail_closed"
}
JSON
}

READINESS_REPORT="${ARCHIVE_DIR}/oss_model_readiness.json"
READER_GATE_REPORT="${ARCHIVE_DIR}/oss_reader_capability_gate.json"

if ! python3 "${ROOT}/tools/check_oss_model_readiness.py" \
  --reader-base-url "${READER_BASE_URL}" \
  --target-model "${READER_MODEL}" \
  --run-reader-gate \
  --reader-gate-report "${READER_GATE_REPORT}" \
  --report "${READINESS_REPORT}"; then
  write_manifest "not_ready" "model_readiness" "target OSS reader is not installed, unreachable, or failed the reader gate"
  cat "${READINESS_REPORT}"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 2
fi

reader_gate_status="$(python3 - "${READINESS_REPORT}" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
print(data.get("reader_gate", {}).get("status", "missing"))
PY
)"
if [[ "${reader_gate_status}" != "passed" ]]; then
  write_manifest "not_ready" "reader_capability_gate" "reader capability gate status=${reader_gate_status}"
  cat "${READINESS_REPORT}"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 3
fi

export TEMPORALSTORE_READER_BASE_URL="${READER_BASE_URL}"
export TEMPORALSTORE_READER_MODEL="${READER_MODEL}"
export TEMPORALSTORE_READER_PROVIDER_NAME="${READER_PROVIDER_NAME}"
export TEMPORALSTORE_BENCHMARK_REPORT_DIR="${ARCHIVE_DIR}"
export TEMPORALSTORE_LOCOMO_INPUT="${LOC_INPUT}"
export TEMPORALSTORE_LONGMEMEVAL_INPUT="${LONGMEM_INPUT}"

bash "${ROOT}/tools/run_context_benchmarks_oss_reader_endpoint.sh"

if [[ -f "${LOC_INPUT}" && -f "${OPENVIKING_ARCHIVE}" ]]; then
  python3 "${ROOT}/tools/run_openviking_direct_retrieval_baseline.py" \
    --input "${LOC_INPUT}" \
    --archive "${OPENVIKING_ARCHIVE}" \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-model "${READER_MODEL}" \
    --report "${ARCHIVE_DIR}/openviking_direct_locomo.json"
fi

if [[ -f "${LONGMEM_INPUT}" ]]; then
  python3 "${ROOT}/tools/run_openviking_longmem_source_retrieval_baseline.py" \
    --input "${LONGMEM_INPUT}" \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-model "${READER_MODEL}" \
    --report "${ARCHIVE_DIR}/openviking_direct_longmemeval_s.json"
fi

write_manifest "complete" "complete"
cat "${ARCHIVE_DIR}/manifest.json"
