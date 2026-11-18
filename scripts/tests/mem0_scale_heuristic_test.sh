#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
sandbox=$(mktemp -d "${TMPDIR:-/tmp}/mem0-heuristic-test.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -- "$sandbox/scripts"
cp -- "$repo/scripts/mem0_scale_heuristic.sh" "$sandbox/scripts/"
cp -- "$repo/scripts/tests/fixtures/mem0_heuristic_delegate.sh" "$sandbox/scripts/mem0_scale_local.sh"
launcher="$sandbox/scripts/mem0_scale_heuristic.sh"

export TEMPORALSTORE_CONTEXT_PROVIDER=ollama MATRIXARK_CONTEXT_PROVIDER=ollama
export TEMPORALSTORE_EMBEDDING_API_BASE=http://127.0.0.1:9
export MATRIXARK_EMBEDDING_API_BASE=http://127.0.0.1:9 MATRIXARK_EMBED_BASE_URL=http://127.0.0.1:9
export TEMPORALSTORE_EMBEDDING_MODEL=live-model MATRIXARK_EMBEDDING_MODEL=live-model
export MATRIXARK_REQUIRE_MODEL_SUMMARIES=true MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=true
export TS_MEM0_PROVIDER_JSON='{"provider_kind":"ollama","mock_mode":false}'

[[ $(bash "$launcher" --work-dir 'directory with spaces' --skip-build) == delegate-ok ]]
[[ $TEMPORALSTORE_CONTEXT_PROVIDER == ollama && $MATRIXARK_REQUIRE_MODEL_EMBEDDINGS == true ]]
printf '%s\n' 'ok: inherited endpoints are disabled only in the child'
[[ $(EXPECTED_INFER=true bash "$launcher" --infer true --work-dir 'directory with spaces') == delegate-ok ]]
printf '%s\n' 'ok: explicit inference mode is preserved with the mock provider'
for flag in --provider-json --provider-json=live; do
  if bash "$launcher" "$flag" '{}' --work-dir 'directory with spaces' > "$sandbox/rejected.txt" 2>&1; then
    echo 'provider override unexpectedly accepted' >&2; exit 1
  else
    [[ $? == 2 ]]
  fi
  printf '%s\n' "ok: rejected $flag before delegation"
done
if DELEGATE_EXIT=7 bash "$launcher" --work-dir 'directory with spaces' > "$sandbox/exit.txt"; then
  echo 'delegate failure was hidden' >&2; exit 1
else
  [[ $? == 7 ]]
fi
printf '%s\n' 'ok: delegate exit status is preserved'
help=$(bash "$launcher" --help)
[[ $help == *'Model-free Mem0 testing'* && $help != *delegate-ok* ]]
printf '%s\n' 'ok: help does not start the delegate'
printf '%s\n' 'heuristic launcher: 6 checks passed'
