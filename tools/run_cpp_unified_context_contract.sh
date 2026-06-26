#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${1:-$(python3 "${ROOT}/tools/resolve_temporalstore_test_corpus.py")}"
CORPUS="$(python3 "${ROOT}/tools/resolve_temporalstore_test_corpus.py" --corpus "${CORPUS}")"
CORPUS_REPO="$(cd "$(dirname "${CORPUS}")/.." && pwd)"
RUNNER="${CORPUS_REPO}/runners/cpp/run_cpp_unified_context_contract.sh"
if [[ ! -x "${RUNNER}" ]]; then
  RUNNER="${ROOT}/third_party/TemporalStoreTestCorpus/runners/cpp/run_cpp_unified_context_contract.sh"
fi
if [[ ! -x "${RUNNER}" ]]; then
  RUNNER="${ROOT}/../TemporalStoreTestCorpus/runners/cpp/run_cpp_unified_context_contract.sh"
fi

if [[ ! -x "${RUNNER}" ]]; then
  echo "missing shared C++ context runner: ${RUNNER}" >&2
  exit 2
fi

exec "${RUNNER}" "${CORPUS}"
