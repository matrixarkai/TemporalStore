#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
cd "$ROOT"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust}"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
export MATRIXARK_HOOK_FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"
export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-no-metaserver}"
export MATRIXARK_HOOK_AUTOSTART_CPP="${MATRIXARK_HOOK_AUTOSTART_CPP:-0}"
export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"

if [[ -z "${MATRIXARK_TEMPORALSTORE_RUST_CLI:-}" ]]; then
	  for candidate in \
	    "$ROOT/target/release/matrixark_rust_proxy" \
	    "$ROOT/target/debug/matrixark_rust_proxy" \
	    "$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_proxy" \
	    "$ROOT/sdk/rust/temporalstore/target/debug/matrixark_rust_proxy"; do
    if [[ -x "$candidate" ]]; then
      export MATRIXARK_TEMPORALSTORE_RUST_CLI="$candidate"
      break
    fi
  done
fi
export MATRIXARK_TEMPORALSTORE_RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-$ROOT/sdk/rust/temporalstore/target/release/matrixark_rust_proxy}"

for libdir in \
  "$ROOT/output-ubuntu22/release/sdk/lib" \
  "$ROOT/output/sdk/lib" \
  "$ROOT/sdk/lib"; do
  if [[ -d "$libdir" ]]; then
    export LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  fi
done

export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-hash}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-0}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-rules}"
export MATRIXARK_SEGMENT_PROVIDER="${MATRIXARK_SEGMENT_PROVIDER:-deterministic}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-0}"

if [[ "$MATRIXARK_LOCAL_MODE" == "no-metaserver" || "$MATRIXARK_LOCAL_MODE" == "embedded" || "$MATRIXARK_LOCAL_MODE" == "1" ]]; then
  export MATRIXARK_HOOK_AUTOSTART_CPP=0
  export MATRIXARK_TEMPORALSTORE_METASERVER="local"
fi

if [[ "$MATRIXARK_HOOK_AUTOSTART_CPP" == "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    BUILD_TYPE="${BUILD_TYPE:-Release}" timeout 30 bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >/dev/null 2>&1 || true
  fi
fi

if [[ ! -x "$MATRIXARK_TEMPORALSTORE_RUST_CLI" ]]; then
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}" cargo build --release -p temporalstore-rust --bin matrixark_rust_proxy >/dev/null
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="$ROOT/target/release/matrixark_rust_proxy"
fi

status=0
python3 "$ROOT/tools/matrixark_codex_hook.py" "$@" || status=$?
if [[ "$status" == "0" ]]; then
  exit 0
fi
if [[ "$MATRIXARK_HOOK_FAIL_OPEN" == "1" ]]; then
  printf '{"status":"warning","component":"matrixark_codex_rust_hook","reason":"hook_failed_fail_open","exit_code":%s}
' "$status"
  exit 0
fi
exit "$status"
