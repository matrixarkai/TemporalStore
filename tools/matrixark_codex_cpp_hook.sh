#!/usr/bin/env bash
set -euo pipefail

ROOT="${MATRIXARK_REPO_ROOT:-<repo>}"
cd "$ROOT"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-direct}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:codex-hook}"
export TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-$ROOT/output-ubuntu22/release/sdk/lib/libbcache2.so}"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-5000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-5000}"
export MATRIXARK_HOOK_FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"
export MATRIXARK_HOOK_AUTOSTART_CPP="${MATRIXARK_HOOK_AUTOSTART_CPP:-1}"

# Hooks run in Codex's critical path. Default to fast deterministic providers;
# OSS/OpenAI providers can be enabled explicitly for offline/debug hook tests.
export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-hash}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-0}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-rules}"
export MATRIXARK_SEGMENT_PROVIDER="${MATRIXARK_SEGMENT_PROVIDER:-deterministic}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-0}"

if [[ "$MATRIXARK_HOOK_AUTOSTART_CPP" == "1" && "$MATRIXARK_MCP_BACKEND" == "temporalstore-direct" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    BUILD_TYPE="${BUILD_TYPE:-Release}" timeout 30 bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >/dev/null 2>&1 || true
  fi
fi

if python3 "$ROOT/tools/matrixark_codex_hook.py" "$@"; then
  exit 0
fi
status=$?
if [[ "$MATRIXARK_HOOK_FAIL_OPEN" == "1" ]]; then
  printf '{"status":"warning","component":"matrixark_codex_hook","reason":"hook_failed_fail_open","exit_code":%s}
' "$status"
  exit 0
fi
exit "$status"
