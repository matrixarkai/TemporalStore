#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
READER_BASE_URL="${TEMPORALSTORE_READER_BASE_URL:-}"
READER_MODEL="${TEMPORALSTORE_READER_MODEL:-google/flan-t5-small}"
READER_PROVIDER_NAME="${TEMPORALSTORE_READER_PROVIDER_NAME:-matrixark-cpp-oss-context}"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
LOC_INPUT="${TEMPORALSTORE_LOCOMO_INPUT:-/tmp/locomo10.json}"
LONGMEM_INPUT="${TEMPORALSTORE_LONGMEMEVAL_INPUT:-/tmp/longmemeval_s.json}"
REQUIRE_RUST_TEMPORALSTORE="${TEMPORALSTORE_REQUIRE_RUST_TEMPORALSTORE:-1}"
ALLOW_PYTHON_ONLY_DIAGNOSTIC="${TEMPORALSTORE_ALLOW_PYTHON_ONLY_DIAGNOSTIC:-0}"
RUST_TEMPORALSTORE_MAX_CASES="${TEMPORALSTORE_RUST_BACKEND_MAX_CASES:-4}"
RUST_TEMPORALSTORE_SOURCE_LIMIT="${TEMPORALSTORE_RUST_BACKEND_SOURCE_LIMIT:-64}"
RUST_TEMPORALSTORE_TIMEOUT_SECONDS="${TEMPORALSTORE_RUST_BACKEND_TIMEOUT_SECONDS:-180}"
RUST_TEMPORALSTORE_SCORE_TOLERANCE="${TEMPORALSTORE_RUST_BACKEND_SCORE_TOLERANCE:-0}"
RUST_TEMPORALSTORE_BATCH_SIZE="${TEMPORALSTORE_RUST_BACKEND_BATCH_SIZE:-0}"
RUST_TEMPORALSTORE_RELEASE="${TEMPORALSTORE_RUST_BACKEND_RELEASE:-0}"
REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY="${TEMPORALSTORE_REQUIRE_FULL_RUST_REPLAY:-0}"
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
  "require_rust_temporalstore": "${REQUIRE_RUST_TEMPORALSTORE}",
  "allow_python_only_diagnostic": "${ALLOW_PYTHON_ONLY_DIAGNOSTIC}",
  "rust_temporalstore_max_cases": "${RUST_TEMPORALSTORE_MAX_CASES}",
  "rust_temporalstore_source_limit": "${RUST_TEMPORALSTORE_SOURCE_LIMIT}",
  "rust_temporalstore_batch_size": "${RUST_TEMPORALSTORE_BATCH_SIZE}",
  "rust_temporalstore_timeout_seconds": "${RUST_TEMPORALSTORE_TIMEOUT_SECONDS}",
  "rust_temporalstore_score_tolerance": "${RUST_TEMPORALSTORE_SCORE_TOLERANCE}",
  "rust_temporalstore_release": "${RUST_TEMPORALSTORE_RELEASE}",
  "require_full_rust_temporalstore_replay": "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}",
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

if [[ ! "${REQUIRE_RUST_TEMPORALSTORE}" =~ ^(1|true|TRUE)$ && ! "${ALLOW_PYTHON_ONLY_DIAGNOSTIC}" =~ ^(1|true|TRUE)$ ]]; then
  write_manifest "not_run" "not_run" "rust_temporalstore_required" \
    "Rust TemporalStore backend is required; set TEMPORALSTORE_ALLOW_PYTHON_ONLY_DIAGNOSTIC=1 only for local Python-only debugging"
  cat "${ARCHIVE_DIR}/manifest.json"
  exit 2
fi

locomo_status="skipped_missing_input"
longmem_status="skipped_missing_input"
RUST_BACKEND_ARGS=()
if [[ "${REQUIRE_RUST_TEMPORALSTORE}" == "1" || "${REQUIRE_RUST_TEMPORALSTORE}" == "true" || "${REQUIRE_RUST_TEMPORALSTORE}" == "TRUE" ]]; then
  RUST_BACKEND_ARGS=(
    --require-rust-temporalstore
    --rust-temporalstore-max-cases "${RUST_TEMPORALSTORE_MAX_CASES}"
    --rust-temporalstore-source-limit "${RUST_TEMPORALSTORE_SOURCE_LIMIT}"
    --rust-temporalstore-timeout-seconds "${RUST_TEMPORALSTORE_TIMEOUT_SECONDS}"
    --rust-temporalstore-batch-size "${RUST_TEMPORALSTORE_BATCH_SIZE}"
    --rust-temporalstore-score-tolerance "${RUST_TEMPORALSTORE_SCORE_TOLERANCE}"
  )
  if [[ "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "1" || "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "true" || "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "TRUE" ]]; then
    RUST_BACKEND_ARGS+=(--require-full-rust-temporalstore-replay)
  fi
  if [[ "${RUST_TEMPORALSTORE_RELEASE}" == "1" || "${RUST_TEMPORALSTORE_RELEASE}" == "true" || "${RUST_TEMPORALSTORE_RELEASE}" == "TRUE" ]]; then
    RUST_BACKEND_ARGS+=(--rust-temporalstore-release)
  fi
elif [[ "${ALLOW_PYTHON_ONLY_DIAGNOSTIC}" == "1" || "${ALLOW_PYTHON_ONLY_DIAGNOSTIC}" == "true" || "${ALLOW_PYTHON_ONLY_DIAGNOSTIC}" == "TRUE" ]]; then
  RUST_BACKEND_ARGS=(--skip-rust-temporalstore --allow-python-only-diagnostic)
fi

if [[ -f "${LOC_INPUT}" ]]; then
  python3 "${ROOT}/tools/run_locomo_90_hit_rate.py" \
    --threshold-profile oss_reader_full \
    --input "${LOC_INPUT}" \
    --reader-mode open-source \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-provider-name "${READER_PROVIDER_NAME}" \
    --reader-model "${READER_MODEL}" \
    --reader-no-fallback \
    "${RUST_BACKEND_ARGS[@]}" \
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
    "${RUST_BACKEND_ARGS[@]}" \
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
