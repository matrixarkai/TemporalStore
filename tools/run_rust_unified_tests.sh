#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${RUST_UNIFIED_CORPUS:-$(python3 "${ROOT}/tools/resolve_temporalstore_test_corpus.py")}"
RESULT_DIR="${RUST_UNIFIED_RESULT_DIR:-/tmp/temporalstore-unified-parity}"
VALIDATE_ONLY="${RUST_UNIFIED_VALIDATE_ONLY:-0}"

if [[ ! -f "${CORPUS}" ]]; then
  echo "missing unified corpus: ${CORPUS}" >&2
  exit 1
fi

args=(--corpus "${CORPUS}" --result-dir "${RESULT_DIR}")
if [[ "${VALIDATE_ONLY}" == "1" ]]; then
  args+=(--validate-only)
fi

python3 "${ROOT}/tools/run_unified_parity_tests.py" "${args[@]}"
