#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

ARGS=("--rust")
if [[ -n "${TS_NATIVE_REPO:-}" ]]; then
  ARGS+=("--native-repo" "${TS_NATIVE_REPO}")
fi
if [[ "${TS_RUN_NATIVE_UNIFIED_TESTS:-0}" == "1" ]]; then
  ARGS+=("--native" "--require-native")
elif [[ -n "${TS_NATIVE_UNIFIED_TEST_CMD:-}" ]]; then
  ARGS+=("--native")
fi
if [[ "${TS_REQUIRE_NATIVE_NATIVE:-0}" == "1" ]]; then
  ARGS+=("--require-native-native")
fi

python3 tools/run_temporalstore_unified_tests.py "${ARGS[@]}" "$@"
