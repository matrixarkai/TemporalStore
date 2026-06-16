#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

ARGS=("--rust")
if [[ "${TS_RUN_CPP_UNIFIED_TESTS:-0}" == "1" ]]; then
  ARGS+=("--cpp" "--require-cpp")
elif [[ -n "${TS_CPP_UNIFIED_TEST_CMD:-}" ]]; then
  ARGS+=("--cpp")
fi

python3 tools/run_temporalstore_unified_tests.py "${ARGS[@]}" "$@"
