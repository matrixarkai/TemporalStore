#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL="${TEMPORALSTORE_READER_MODEL:-google/flan-t5-small}"
HOST="${TEMPORALSTORE_HF_READER_HOST:-127.0.0.1}"
PORT="${TEMPORALSTORE_HF_READER_PORT:-8000}"
BASE_URL="http://${HOST}:${PORT}/v1"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_DIR="${REPORT_DIR}/hf_oss_reader_endpoint_${TIMESTAMP}"
SMOKE_INPUT="${TEMPORALSTORE_OSS_READER_SMOKE_INPUT:-${ROOT}/tools/fixtures/longmemeval_s_full_path_fixture.json}"
RUN_SMOKE="${TEMPORALSTORE_OSS_READER_RUN_SMOKE:-1}"
PID_FILE="${ARCHIVE_DIR}/reader.pid"
LOG_FILE="${ARCHIVE_DIR}/reader.log"

mkdir -p "${ARCHIVE_DIR}"

write_manifest() {
  local phase="$1"
  local smoke_status="${2:-not_run}"
  local error="${3:-}"
  cat >"${ARCHIVE_DIR}/manifest.json" <<JSON
{
  "archive_dir": "${ARCHIVE_DIR}",
  "reader_base_url": "${BASE_URL}",
  "reader_model": "${MODEL}",
  "reader_provider_name": "matrixark-cpp-oss-context",
  "phase": "${phase}",
  "smoke_status": "${smoke_status}",
  "smoke_input": "${SMOKE_INPUT}",
  "error": "${error}",
  "reader_open_source_calls": 0,
  "paper_comparable_claim_ready": false,
  "created_at_utc": "${TIMESTAMP}",
  "claim_level": "live_oss_reader_endpoint_smoke"
}
JSON
}

wait_for_reader() {
  local attempt
  for attempt in $(seq 1 120); do
    if python3 - "${BASE_URL}" <<'PY' >/dev/null 2>&1; then
import sys
import urllib.request

base_url = sys.argv[1].rstrip("/")
with urllib.request.urlopen(base_url + "/models", timeout=2.0) as response:
    if response.status != 200:
        raise SystemExit(1)
PY
      return 0
    fi
    sleep 2
  done
  return 1
}

cleanup() {
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

(
  cd "${ROOT}"
  exec python3 tools/openai_compatible_hf_reader.py \
    --host "${HOST}" \
    --port "${PORT}" \
    --model "${MODEL}"
) >"${LOG_FILE}" 2>&1 &
echo "$!" >"${PID_FILE}"

if ! wait_for_reader; then
  write_manifest "reader_start_failed" "not_run" "reader did not answer /v1/models; see reader.log"
  cat "${LOG_FILE}" >&2 || true
  exit 2
fi

if [[ ! "${RUN_SMOKE}" =~ ^(1|true|TRUE)$ ]]; then
  write_manifest "reader_ready" "skipped"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 0
fi

if [[ ! -f "${SMOKE_INPUT}" ]]; then
  write_manifest "reader_ready" "skipped_missing_input" "smoke input not found"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 2
fi

python3 "${ROOT}/tools/run_longmemeval_s_full_path.py" \
  --threshold-profile fixture \
  --input "${SMOKE_INPUT}" \
  --reader-mode open-source \
  --reader-base-url "${BASE_URL}" \
  --reader-provider-name matrixark-cpp-oss-context \
  --reader-model "${MODEL}" \
  --reader-no-fallback \
  --require-open-source-reader \
  --rust-temporalstore-max-cases 4 \
  --rust-temporalstore-source-limit 64 \
  --rust-temporalstore-timeout-seconds 180 \
  --report "${ARCHIVE_DIR}/longmemeval_fixture_oss_reader_report.json" \
  --misses "${ARCHIVE_DIR}/longmemeval_fixture_oss_reader_misses.jsonl"

python3 "${ROOT}/tools/archive_context_benchmark_report.py" \
  --report "${ARCHIVE_DIR}/longmemeval_fixture_oss_reader_report.json" \
  --input "${SMOKE_INPUT}" \
  --output "${ARCHIVE_DIR}/longmemeval_fixture_oss_reader_paper_comparable_report.json" \
  --claim-level live_oss_reader_endpoint_smoke

write_manifest "complete" "passed"
python3 - "${ARCHIVE_DIR}/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = json.loads(path.read_text())
report = json.loads((path.parent / "longmemeval_fixture_oss_reader_report.json").read_text())
data["reader_open_source_calls"] = int(report.get("reader_open_source_calls") or 0)
data["paper_comparable_claim_ready"] = bool(report.get("paper_comparable_claim_ready"))
path.write_text(json.dumps(data, indent=2) + "\n")
PY
echo "Archived HF OSS reader endpoint smoke under ${ARCHIVE_DIR}"
