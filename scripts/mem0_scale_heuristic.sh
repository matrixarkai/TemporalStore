#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
has_infer=0
for argument in "$@"; do
  case "$argument" in
    --provider-json|--provider-json=*)
      echo 'The heuristic launcher does not accept --provider-json; use mem0_scale_local.sh for model runs.' >&2
      exit 2 ;;
    --infer) has_infer=1 ;;
    --help)
      printf '%s\n' \
        'Usage: bash scripts/mem0_scale_heuristic.sh [mem0_scale_local.sh options]' \
        'Model-free Mem0 testing: deterministic embeddings and heuristic summaries, with no live-provider fallback.' \
        'Defaults to --infer false. Explicit --infer true exercises the mock pipeline, not model extraction quality.' \
        'Use --skip-build --bin-dir DIR to reuse validated binaries on a shared machine.'
      exit 0 ;;
  esac
done
if (( ! has_infer )); then set -- --infer false "$@"; fi

provider='{"provider_name":"mock-openai-compatible","provider_kind":"mock","mock_mode":true,"model":"mock-context-chat","embedding_model":"mock-context-embedding","base_url":"","fallback_provider":null}'
export TS_MEM0_PROVIDER_JSON="$provider"
export TEMPORALSTORE_CONTEXT_PROVIDER=mock MATRIXARK_CONTEXT_PROVIDER=mock
export TEMPORALSTORE_EMBEDDING_API_BASE='' MATRIXARK_EMBEDDING_API_BASE='' MATRIXARK_EMBED_BASE_URL=''
export TEMPORALSTORE_EMBEDDING_MODEL=mock-context-embedding MATRIXARK_EMBEDDING_MODEL=mock-context-embedding
export MATRIXARK_REQUIRE_MODEL_SUMMARIES=false MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=false

exec bash "$script_dir/mem0_scale_local.sh" --provider-json "$provider" "$@"
