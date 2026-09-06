#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
cd "$ROOT"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
# The default names the codex hook's namespace, which is deliberate for continuity with an
# existing store -- but this script and tools/matrixark_codex_rust_hook.sh do NOT embed the
# same way. This one asks for the in-process model (oss, below); the hook asks for token-hash
# vectors. Vectors made by one encoder are declined by the other as a different model, and
# retrieval then falls back to lexical and recency for everything the other side wrote.
#
# A prefix only collides inside ONE store, and in every documented way of starting this these
# are separate: cluster mode goes through a metaserver, and the no-metaserver instructions set
# an explicit MATRIXARK_TEMPORALSTORE_LOCAL_STORE of their own. So this is a trap rather than a
# fault -- if you ever point this and the hook at the same store, give them distinct prefixes
# or make the two encoders agree first.
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust-live-v2}"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
export MATRIXARK_MCP_AUTOSTART_NATIVE="${MATRIXARK_MCP_AUTOSTART_NATIVE:-0}"
export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-cluster}"
export MATRIXARK_TEMPORALSTORE_LOCAL_STORE="${MATRIXARK_TEMPORALSTORE_LOCAL_STORE:-$ROOT/.local/runtime/matrixark-rust-disk-fallback.jsonl}"
fallback_default=1
case "${MATRIXARK_MCP_PROFILE:-dev}" in
  prod|production|benchmark|bench|parity) fallback_default=0 ;;
esac
export MATRIXARK_TEMPORALSTORE_DISK_FALLBACK="${MATRIXARK_TEMPORALSTORE_DISK_FALLBACK:-$fallback_default}"
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
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"

export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-oss}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-1}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-oss_encoder}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-1}"
export MATRIXARK_RETRIEVAL_TIMEOUT_MS="${MATRIXARK_RETRIEVAL_TIMEOUT_MS:-20000}"

start_disk_fallback() {
  mkdir -p "$(dirname "$MATRIXARK_TEMPORALSTORE_LOCAL_STORE")"
  echo "MatrixArk MCP Rust: serving from disk-backed temporalstore-local store $MATRIXARK_TEMPORALSTORE_LOCAL_STORE" >&2
  exec python3 "$ROOT/tools/matrixark_mcp_server.py" \
    --backend temporalstore-local \
    --local-store "$MATRIXARK_TEMPORALSTORE_LOCAL_STORE" \
    "$@"
}

if [[ "$MATRIXARK_LOCAL_MODE" == "no-metaserver" || "$MATRIXARK_LOCAL_MODE" == "embedded" || "$MATRIXARK_LOCAL_MODE" == "1" ]]; then
  start_disk_fallback "$@"
fi

if [[ ( "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust" || "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" ) && "$MATRIXARK_MCP_AUTOSTART_NATIVE" == "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 2 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    echo "MatrixArk MCP Rust: TemporalStore metaserver $MATRIXARK_TEMPORALSTORE_METASERVER is not listening; starting local deployment..." >&2
    BUILD_TYPE="${BUILD_TYPE:-Release}" SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:---storage_async=true --server_stopping_wait_s=1}" timeout 90 : >&2
  fi
fi

if [[ ( "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust" || "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" ) && "$MATRIXARK_MCP_AUTOSTART_NATIVE" != "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    if [[ "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "1" || "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "true" || "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "yes" ]]; then
      echo "MatrixArk MCP Rust: TemporalStore metaserver $MATRIXARK_TEMPORALSTORE_METASERVER is not reachable; falling back to disk-backed retrieval." >&2
      start_disk_fallback "$@"
    fi
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

if ! bash "$ROOT/tools/wait_temporalstore_topology_ready.sh" \
  --backend rust \
  --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
  --namespace "$MATRIXARK_TEMPORALSTORE_NAMESPACE" \
  --table "$MATRIXARK_TEMPORALSTORE_TABLE" \
  --prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
  --rust-cli "$(if [[ "$MATRIXARK_MCP_BACKEND" == "temporalstore-rust-direct" ]]; then printf '%s' "$MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK"; else printf '%s' "$MATRIXARK_TEMPORALSTORE_RUST_PROXY"; fi)" \
  --timeout-ms "${MATRIXARK_BACKEND_READINESS_TIMEOUT_MS:-30000}" >&2; then
  if [[ "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "1" || "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "true" || "$MATRIXARK_TEMPORALSTORE_DISK_FALLBACK" == "yes" ]]; then
    echo "MatrixArk MCP Rust: TemporalStore is not ready; falling back to disk-backed retrieval." >&2
    start_disk_fallback "$@"
  fi
  exit 2
fi

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
