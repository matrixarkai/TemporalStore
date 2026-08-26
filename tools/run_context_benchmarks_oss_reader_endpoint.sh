#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
READER_BASE_URL="${TEMPORALSTORE_READER_BASE_URL:-}"
READER_MODEL="${TEMPORALSTORE_READER_MODEL:-gpt-4o-mini}"
READER_PROVIDER_NAME="${TEMPORALSTORE_READER_PROVIDER_NAME:-external_baseline-gpt-4o-mini-reader}"
EMBEDDING_MODEL="${TEMPORALSTORE_EMBEDDING_MODEL:-sentence-transformers/all-MiniLM-L6-v2}"
BASELINE_PROVIDER_NAME="${TEMPORALSTORE_BASELINE_PROVIDER_NAME:-external_baseline-external_baseline-oss-baseline}"
BASELINE_READER_MODEL="${TEMPORALSTORE_BASELINE_READER_MODEL:-${READER_MODEL}}"
BASELINE_EMBEDDING_MODEL="${TEMPORALSTORE_BASELINE_EMBEDDING_MODEL:-${EMBEDDING_MODEL}}"
BASELINE_MAX_EVENTS="${TEMPORALSTORE_BASELINE_MAX_EVENTS:-}"
BASELINE_READER_MAX_CONTEXT_CHARS="${TEMPORALSTORE_BASELINE_READER_MAX_CONTEXT_CHARS:-}"
REPORT_DIR="${TEMPORALSTORE_BENCHMARK_REPORT_DIR:-${ROOT}/benchmark_reports}"
LOC_INPUT="${TEMPORALSTORE_LOCOMO_INPUT:-/tmp/locomo10.json}"
LONGMEM_INPUT="${TEMPORALSTORE_LONGMEMEVAL_INPUT:-/tmp/longmemeval_s.json}"
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
  "embedding_model": "${EMBEDDING_MODEL}",
  "baseline_provider_name": "${BASELINE_PROVIDER_NAME}",
  "baseline_reader_model": "${BASELINE_READER_MODEL}",
  "baseline_embedding_model": "${BASELINE_EMBEDDING_MODEL}",
  "baseline_max_events": "${BASELINE_MAX_EVENTS}",
  "baseline_reader_max_context_chars": "${BASELINE_READER_MAX_CONTEXT_CHARS}",
  "require_shared_oss_models": "1",
  "require_rust_temporalstore": "1",
  "allow_python_only_diagnostic": "0",
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
    "set TEMPORALSTORE_READER_BASE_URL to an OpenAI-compatible /v1 endpoint serving gpt-4o-mini"
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
RUST_BACKEND_ARGS=(
  --require-rust-temporalstore
  --rust-temporalstore-max-cases "${RUST_TEMPORALSTORE_MAX_CASES}"
  --rust-temporalstore-source-limit "${RUST_TEMPORALSTORE_SOURCE_LIMIT}"
  --rust-temporalstore-timeout-seconds "${RUST_TEMPORALSTORE_TIMEOUT_SECONDS}"
  --rust-temporalstore-batch-size "${RUST_TEMPORALSTORE_BATCH_SIZE}"
  --rust-temporalstore-score-tolerance "${RUST_TEMPORALSTORE_SCORE_TOLERANCE}"
)
SHARED_OSS_ARGS=(
  --embedding-model "${EMBEDDING_MODEL}"
  --baseline-provider-name "${BASELINE_PROVIDER_NAME}"
  --baseline-reader-model "${BASELINE_READER_MODEL}"
  --baseline-embedding-model "${BASELINE_EMBEDDING_MODEL}"
  --require-shared-oss-models
)
if [[ "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "1" || "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "true" || "${REQUIRE_FULL_RUST_TEMPORALSTORE_REPLAY}" == "TRUE" ]]; then
  RUST_BACKEND_ARGS+=(--require-full-rust-temporalstore-replay)
fi
if [[ "${RUST_TEMPORALSTORE_RELEASE}" == "1" || "${RUST_TEMPORALSTORE_RELEASE}" == "true" || "${RUST_TEMPORALSTORE_RELEASE}" == "TRUE" ]]; then
  RUST_BACKEND_ARGS+=(--rust-temporalstore-release)
fi

if [[ -f "${LOC_INPUT}" ]]; then
  locomo_baseline_max_events="${BASELINE_MAX_EVENTS:-128}"
  locomo_baseline_context_chars="${BASELINE_READER_MAX_CONTEXT_CHARS:-12000}"
  python3 "${ROOT}/tools/run_locomo_90_hit_rate.py" \
    --threshold-profile oss_reader_full \
    --input "${LOC_INPUT}" \
    --reader-mode open-source \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-provider-name "${READER_PROVIDER_NAME}" \
    --reader-model "${READER_MODEL}" \
    --reader-no-fallback \
    --baseline-max-events "${locomo_baseline_max_events}" \
    --baseline-reader-max-context-chars "${locomo_baseline_context_chars}" \
    "${SHARED_OSS_ARGS[@]}" \
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
  longmem_baseline_max_events="${BASELINE_MAX_EVENTS:-14}"
  longmem_baseline_context_chars="${BASELINE_READER_MAX_CONTEXT_CHARS:-12000}"
  python3 "${ROOT}/tools/run_longmemeval_s_full_path.py" \
    --threshold-profile longmemeval_full \
    --input "${LONGMEM_INPUT}" \
    --reader-mode open-source \
    --reader-base-url "${READER_BASE_URL}" \
    --reader-provider-name "${READER_PROVIDER_NAME}" \
    --reader-model "${READER_MODEL}" \
    --reader-no-fallback \
    --baseline-max-events "${longmem_baseline_max_events}" \
    --baseline-reader-max-context-chars "${longmem_baseline_context_chars}" \
    "${SHARED_OSS_ARGS[@]}" \
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
