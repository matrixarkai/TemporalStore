#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
cd "$ROOT"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-direct}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:mcp:codex}"
if [[ -z "${TEMPORALSTORE_LIB:-}" ]]; then
  for candidate in \
    "$ROOT/output-ubuntu22/release/sdk/lib/libbcache2.so" \
    "$ROOT/output/sdk/lib/libbcache2.so" \
    "$ROOT/build-ubuntu22/test-release/sdk/lib/libbcache2.so"; do
    if [[ -f "$candidate" ]]; then
      export TEMPORALSTORE_LIB="$candidate"
      break
    fi
  done
fi
export TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-$ROOT/output-ubuntu22/release/sdk/lib/libbcache2.so}"
if [[ -f "$TEMPORALSTORE_LIB" ]]; then
  export LD_LIBRARY_PATH="$(dirname "$TEMPORALSTORE_LIB"):${LD_LIBRARY_PATH:-}"
fi
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
export MATRIXARK_MCP_AUTOSTART_CPP="${MATRIXARK_MCP_AUTOSTART_CPP:-1}"
export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-cluster}"

export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-oss}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-1}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-oss_encoder}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-1}"
export MATRIXARK_RETRIEVAL_TIMEOUT_MS="${MATRIXARK_RETRIEVAL_TIMEOUT_MS:-20000}"

if [[ "$MATRIXARK_LOCAL_MODE" == "no-metaserver" || "$MATRIXARK_LOCAL_MODE" == "embedded" || "$MATRIXARK_LOCAL_MODE" == "1" ]]; then
  echo "MatrixArk MCP: local JSONL/no-metaserver record-log mode is retired; start a C++/Rust TemporalStore backend instead." >&2
  exit 2
fi

if [[ "$MATRIXARK_MCP_BACKEND" == "temporalstore-direct" && "$MATRIXARK_MCP_AUTOSTART_CPP" == "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 2 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    echo "MatrixArk MCP: TemporalStore metaserver $MATRIXARK_TEMPORALSTORE_METASERVER is not listening; starting local C++ deployment..." >&2
    BUILD_TYPE="${BUILD_TYPE:-Release}" SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:---storage_async=true --server_stopping_wait_s=1}" timeout 90 bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >&2
  fi
fi

bash "$ROOT/tools/wait_temporalstore_topology_ready.sh" \
  --backend cpp \
  --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
  --namespace "$MATRIXARK_TEMPORALSTORE_NAMESPACE" \
  --table "$MATRIXARK_TEMPORALSTORE_TABLE" \
  --prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
  --temporalstore-lib "$TEMPORALSTORE_LIB" \
  --timeout-ms "${MATRIXARK_BACKEND_READINESS_TIMEOUT_MS:-30000}" >&2

exec python3 "$ROOT/tools/matrixark_mcp_server.py" \
  --backend temporalstore-direct \
  --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
  --namespace "$MATRIXARK_TEMPORALSTORE_NAMESPACE" \
  --table "$MATRIXARK_TEMPORALSTORE_TABLE" \
  --temporalstore-lib "$TEMPORALSTORE_LIB" \
  --storage-prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
  --request-timeout-ms "$MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS" \
  --io-timeout-ms "$MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS" \
  "$@"
