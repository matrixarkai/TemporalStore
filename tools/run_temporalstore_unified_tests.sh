#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

ARGS=("--rust")
if [[ -n "${TS_CPP_REPO:-}" ]]; then
  ARGS+=("--cpp-repo" "${TS_CPP_REPO}")
fi
if [[ "${TS_RUN_CPP_UNIFIED_TESTS:-0}" == "1" ]]; then
  ARGS+=("--cpp" "--require-cpp")
elif [[ -n "${TS_CPP_UNIFIED_TEST_CMD:-}" ]]; then
  ARGS+=("--cpp")
fi
if [[ "${TS_REQUIRE_CPP_NATIVE:-0}" == "1" ]]; then
  ARGS+=("--require-cpp-native")
fi

python3 tools/run_temporalstore_unified_tests.py "${ARGS[@]}" "$@"
