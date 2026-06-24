#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${1:-$(python3 "${ROOT}/tools/resolve_temporalstore_test_corpus.py")}"
BUILD_DIR="${TMPDIR:-/tmp}/temporalstore_cpp_unified_$$"
BIN="${BUILD_DIR}/cpp_unified_context_contract"

mkdir -p "${BUILD_DIR}"
trap 'rm -rf "${BUILD_DIR}"' EXIT

g++ -std=c++17 -Wall -Wextra -Werror \
  "${ROOT}/tools/cpp_unified_context_contract.cc" \
  -o "${BIN}"

"${BIN}" "${CORPUS}"
