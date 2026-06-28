#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
cd "$ROOT"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:mcp:codex:rust}"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
export MATRIXARK_MCP_AUTOSTART_CPP="${MATRIXARK_MCP_AUTOSTART_CPP:-0}"
export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-cluster}"
if [[ -z "${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-}" && -n "${MATRIXARK_TEMPORALSTORE_RUST_CLI:-}" ]]; then
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="$MATRIXARK_TEMPORALSTORE_RUST_CLI"
fi
if [[ -z "${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-}" ]]; then
  for candidate in \
    "$ROOT/target/release/matrixark_rust_proxy" \
    "$ROOT/target/debug/matrixark_rust_proxy" \
    "$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_proxy" \
    "$ROOT/sdk/rust/temporalstore/target/debug/matrixark_rust_proxy"; do
    if [[ -x "$candidate" ]]; then
      export MATRIXARK_TEMPORALSTORE_RUST_PROXY="$candidate"
      break
    fi
  done
fi
export MATRIXARK_TEMPORALSTORE_RUST_PROXY="${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_proxy}"
if [[ -z "${MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK:-}" ]]; then
  for candidate in \
    "$ROOT/target/release/matrixark_rust_direct_sdk" \
    "$ROOT/target/debug/matrixark_rust_direct_sdk" \
    "$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_direct_sdk" \
    "$ROOT/sdk/rust/temporalstore/target/debug/matrixark_rust_direct_sdk"; do
    if [[ -x "$candidate" ]]; then
      export MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK="$candidate"
      break
    fi
  done
fi
export MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK="${MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK:-$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_direct_sdk}"
export MATRIXARK_TEMPORALSTORE_RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-$MATRIXARK_TEMPORALSTORE_RUST_PROXY}"
export LD_LIBRARY_PATH="$ROOT/output-ubuntu22/release/sdk/lib:${LD_LIBRARY_PATH:-}"

export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-oss}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-1}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-oss_encoder}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-1}"
export MATRIXARK_RETRIEVAL_TIMEOUT_MS="${MATRIXARK_RETRIEVAL_TIMEOUT_MS:-20000}"

if [[ "$MATRIXARK_LOCAL_MODE" == "no-metaserver" || "$MATRIXARK_LOCAL_MODE" == "embedded" || "$MATRIXARK_LOCAL_MODE" == "1" ]]; then
  export MATRIXARK_MCP_AUTOSTART_CPP=0
  export MATRIXARK_TEMPORALSTORE_METASERVER="local"
fi

if [[ ( "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust" || "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" ) && "$MATRIXARK_MCP_AUTOSTART_CPP" == "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 2 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    echo "MatrixArk MCP Rust: TemporalStore metaserver $MATRIXARK_TEMPORALSTORE_METASERVER is not listening; starting local deployment..." >&2
    BUILD_TYPE="${BUILD_TYPE:-Release}" SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:---storage_async=true --server_stopping_wait_s=1}" timeout 90 bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >&2
  fi
fi

if [[ ! -x "$MATRIXARK_TEMPORALSTORE_RUST_PROXY" ]]; then
  echo "MatrixArk MCP Rust: building Rust proxy at $MATRIXARK_TEMPORALSTORE_RUST_PROXY" >&2
  cargo build --release -p temporalstore-rust --bin matrixark_rust_proxy >&2
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="$ROOT/target/release/matrixark_rust_proxy"
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="$MATRIXARK_TEMPORALSTORE_RUST_PROXY"
fi
if [[ "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" && ! -x "$MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK" ]]; then
  echo "MatrixArk MCP Rust: building Rust direct SDK bridge at $MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK" >&2
  cargo build --release -p temporalstore-rust --bin matrixark_rust_direct_sdk >&2
  export MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK="$ROOT/target/release/matrixark_rust_direct_sdk"
fi

bash "$ROOT/tools/wait_temporalstore_topology_ready.sh" \
  --backend rust \
  --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
  --namespace "$MATRIXARK_TEMPORALSTORE_NAMESPACE" \
  --table "$MATRIXARK_TEMPORALSTORE_TABLE" \
  --prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
  --rust-cli "$(if [[ "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" ]]; then printf '%s' "$MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK"; else printf '%s' "$MATRIXARK_TEMPORALSTORE_RUST_PROXY"; fi)" \
  --timeout-ms "${MATRIXARK_BACKEND_READINESS_TIMEOUT_MS:-30000}" >&2

exec python3 "$ROOT/tools/matrixark_mcp_server.py" \
  --backend "$MATRIXARK_MCP_BACKEND" \
  --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
  --namespace "$MATRIXARK_TEMPORALSTORE_NAMESPACE" \
  --table "$MATRIXARK_TEMPORALSTORE_TABLE" \
  --rust-proxy "$MATRIXARK_TEMPORALSTORE_RUST_PROXY" \
  --rust-direct-sdk "$MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK" \
  --storage-prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
  --request-timeout-ms "$MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS" \
  --io-timeout-ms "$MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS" \
  "$@"
