#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT}/docker-compose.context-benchmarks.yml"
MODEL="${TEMPORALSTORE_READER_MODEL:-qwen2.5:0.5b}"
READER_IMAGE="${TEMPORALSTORE_READER_IMAGE:-ollama/ollama:0.3.14}"
INPUT_DIR="${TEMPORALSTORE_BENCHMARK_INPUT_DIR:-/tmp}"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
READER_BASE_URL="http://open-reader:11434/v1"
LOC_DEFAULT="/bench-input/locomo10.json"
LONGMEM_DEFAULT="/bench-input/longmemeval_s.json"
LOC_INPUT="${TEMPORALSTORE_LOCOMO_INPUT:-${LOC_DEFAULT}}"
LONGMEM_INPUT="${TEMPORALSTORE_LONGMEMEVAL_INPUT:-${LONGMEM_DEFAULT}}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_DIR="${REPORT_DIR}/open_model_${TIMESTAMP}"

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
  "reader_image": "${READER_IMAGE}",
  "reader_model": "${MODEL}",
  "locomo_input": "${LOC_INPUT}",
  "longmemeval_input": "${LONGMEM_INPUT}",
  "locomo_status": "${locomo_status}",
  "longmemeval_status": "${longmem_status}",
  "phase": "${phase}",
  "error": "${error}",
  "created_at_utc": "${TIMESTAMP}"
}
JSON
}

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "${COMPOSE_FILE}")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -f "${COMPOSE_FILE}")
else
  echo "docker compose or docker-compose is required" >&2
  exit 2
fi

export TEMPORALSTORE_READER_IMAGE="${READER_IMAGE}"
export TEMPORALSTORE_READER_MODEL="${MODEL}"
export TEMPORALSTORE_BENCHMARK_INPUT_DIR="${INPUT_DIR}"
export TEMPORALSTORE_BENCHMARK_REPORT_DIR="${REPORT_DIR}"

if ! "${COMPOSE[@]}" up -d open-reader >"${ARCHIVE_DIR}/docker_start.log" 2>&1; then
  write_manifest "not_run" "not_run" "docker_start_failed" "see docker_start.log"
  cat "${ARCHIVE_DIR}/docker_start.log" >&2
  exit 2
fi
if ! "${COMPOSE[@]}" exec -T open-reader ollama pull "${MODEL}" >"${ARCHIVE_DIR}/model_pull.log" 2>&1; then
  write_manifest "not_run" "not_run" "model_pull_failed" "see model_pull.log"
  cat "${ARCHIVE_DIR}/model_pull.log" >&2
  exit 2
fi

run_runner() {
  "${COMPOSE[@]}" run --rm benchmark-runner "$@"
}

locomo_status="skipped_missing_input"
longmem_status="skipped_missing_input"

if run_runner test -f "${LOC_INPUT}"; then
  run_runner \
    python3 tools/run_locomo_90_hit_rate.py \
      --threshold-profile oss_reader_full \
      --input "${LOC_INPUT}" \
      --reader-mode open-source \
      --reader-base-url "${READER_BASE_URL}" \
      --reader-provider-name matrixark-cpp-oss-context \
      --reader-model "${MODEL}" \
      --reader-no-fallback \
      --report /bench-output/"$(basename "${ARCHIVE_DIR}")"/locomo_report.json \
      --misses /bench-output/"$(basename "${ARCHIVE_DIR}")"/locomo_misses.jsonl
  locomo_status="passed"
else
  echo "Skipping LOCOMO: ${LOC_INPUT} is not present in benchmark-runner."
fi

if run_runner test -f "${LONGMEM_INPUT}"; then
  run_runner \
    python3 tools/run_longmemeval_s_full_path.py \
      --threshold-profile longmemeval_full \
      --input "${LONGMEM_INPUT}" \
      --reader-mode open-source \
      --reader-base-url "${READER_BASE_URL}" \
      --reader-provider-name matrixark-cpp-oss-context \
      --reader-model "${MODEL}" \
      --reader-no-fallback \
      --require-open-source-reader \
      --report /bench-output/"$(basename "${ARCHIVE_DIR}")"/longmemeval_s_report.json \
      --misses /bench-output/"$(basename "${ARCHIVE_DIR}")"/longmemeval_s_misses.jsonl
  longmem_status="passed"
else
  echo "Skipping LongMemEval_s: ${LONGMEM_INPUT} is not present in benchmark-runner."
fi

write_manifest "${locomo_status}" "${longmem_status}"
echo "Archived benchmark output under ${ARCHIVE_DIR}"
