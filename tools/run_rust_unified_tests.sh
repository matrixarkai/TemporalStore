#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${RUST_UNIFIED_CORPUS:-${ROOT}/sdk/unified/temporalstore_unified_corpus.json}"
VALIDATE_ONLY="${RUST_UNIFIED_VALIDATE_ONLY:-0}"

if [[ ! -f "${CORPUS}" ]]; then
  echo "missing unified corpus: ${CORPUS}" >&2
  exit 1
fi

args=(--corpus "${CORPUS}")
if [[ "${VALIDATE_ONLY}" == "1" ]]; then
  args+=(--validate-only)
fi

bash "${ROOT}/tools/run_temporalstore_unified_tests.sh" "${args[@]}"

(
  cd "${ROOT}/sdk/rust/temporalstore"
  TEMPORALSTORE_UNIFIED_CORPUS="${CORPUS}" \
  cargo test --no-default-features --features proxy --test unified_corpus
)
