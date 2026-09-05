#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
cd "$ROOT"

# Persistent shared store base. /tmp is wiped on reboot and per-agent roots split
# local memory; anchor Codex and Claude Rust hooks in one durable MatrixArk root.
# Override with MATRIXARK_SHARED_HOOK_STORE_BASE or MATRIXARK_CODEX_HOOK_STORE_BASE.
CODEX_STORE_BASE="${MATRIXARK_CODEX_HOOK_STORE_BASE:-${MATRIXARK_SHARED_HOOK_STORE_BASE:-/root/.matrixark/temporalstore-hooks/shared}}"
mkdir -p "$CODEX_STORE_BASE" 2>/dev/null || true
export MATRIXARK_TEMPORALSTORE_RUST_ROOT="${MATRIXARK_TEMPORALSTORE_RUST_ROOT:-$CODEX_STORE_BASE/store}"
export TEMPORALSTORE_RUST_CODEX_HOOK_ROOT="${TEMPORALSTORE_RUST_CODEX_HOOK_ROOT:-$CODEX_STORE_BASE/rust}"
export MATRIXARK_CODEX_RUST_HOOK_ROOT="${MATRIXARK_CODEX_RUST_HOOK_ROOT:-$TEMPORALSTORE_RUST_CODEX_HOOK_ROOT}"

export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
export MATRIXARK_RUST_TEMPORALSTORE_MODE="${MATRIXARK_RUST_TEMPORALSTORE_MODE:-local}"
export MATRIXARK_RUST_SERVICE_META_ADDR="${MATRIXARK_RUST_SERVICE_META_ADDR:-127.0.0.1:17101}"
export MATRIXARK_RUST_SERVICE_DATANODE_ADDR="${MATRIXARK_RUST_SERVICE_DATANODE_ADDR:-127.0.0.1:17102}"
export MATRIXARK_RUST_SERVICE_PROXY_ADDR="${MATRIXARK_RUST_SERVICE_PROXY_ADDR:-127.0.0.1:17100}"
export MATRIXARK_RUST_SERVICE_AUTOSTART="${MATRIXARK_RUST_SERVICE_AUTOSTART:-1}"
case "$MATRIXARK_RUST_TEMPORALSTORE_MODE" in
  production|parity|service|cluster)
    export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-$MATRIXARK_RUST_SERVICE_PROXY_ADDR}"
    export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-cluster}"
    ;;
  *)
    export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
    export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-no-metaserver}"
    ;;
esac
export MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
export MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust-live-v2}"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
export MATRIXARK_HOOK_FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"
export MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"
export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
export MATRIXARK_HOOK_AUTOSTART_NATIVE="${MATRIXARK_HOOK_AUTOSTART_NATIVE:-0}"
export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"
export MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS="${MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS:-0}"
export MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES="${MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES:-0}"
export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH:-1}"
export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH:-$MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH}"
export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
export MATRIXARK_RUST_PROXY_DAEMON_AUTOSTART="${MATRIXARK_RUST_PROXY_DAEMON_AUTOSTART:-1}"
export MATRIXARK_RUST_PROXY_SOCKET="${MATRIXARK_RUST_PROXY_SOCKET:-/tmp/matrixark-rust-proxy-shared-live.sock}"
export MATRIXARK_RUST_PROXY_DAEMON_LOG="/dev/null"

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
  export MATRIXARK_HOOK_AUTOSTART_NATIVE=0
  export MATRIXARK_TEMPORALSTORE_METASERVER="local"
fi

if [[ "$MATRIXARK_HOOK_AUTOSTART_NATIVE" == "1" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    BUILD_TYPE="${BUILD_TYPE:-Release}" timeout 30 : >/dev/null 2>&1 || true
  fi
fi

_matrixark_start_rust_temporalstore_service() {
  case "$MATRIXARK_RUST_TEMPORALSTORE_MODE" in
    production|parity|service|cluster) ;;
    *) return ;;
  esac
  if [[ "$MATRIXARK_RUST_SERVICE_AUTOSTART" != "1" ]]; then
    return
  fi
  MATRIXARK_RUST_SERVICE_META_ADDR="$MATRIXARK_RUST_SERVICE_META_ADDR" \
  MATRIXARK_RUST_SERVICE_DATANODE_ADDR="$MATRIXARK_RUST_SERVICE_DATANODE_ADDR" \
  MATRIXARK_RUST_SERVICE_PROXY_ADDR="$MATRIXARK_RUST_SERVICE_PROXY_ADDR" \
    bash "$ROOT/tools/deploy_matrixark_rust_temporalstore.sh" start >/dev/null
}

if [[ ! -x "$MATRIXARK_TEMPORALSTORE_RUST_CLI" ]]; then
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}" cargo build --release -p temporalstore-rust --bin matrixark_rust_proxy >/dev/null
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="$ROOT/target/release/matrixark_rust_proxy"
fi

_matrixark_start_rust_temporalstore_service

_matrixark_start_rust_proxy_daemon() {
  case "$MATRIXARK_RUST_TEMPORALSTORE_MODE" in
    production|parity|service|cluster)
      # In service mode the Rust topology is long-lived. The MatrixArk hook
      # should use the Rust SDK/proxy client path rather than the local embedded
      # warm-proxy daemon shortcut.
      unset MATRIXARK_RUST_PROXY_SOCKET
      return
      ;;
  esac
  if [[ "$MATRIXARK_RUST_PROXY_DAEMON_AUTOSTART" != "1" ]]; then
    return
  fi
  if python3 "$ROOT/tools/matrixark_rust_proxy_daemon.py" \
      --proxy "$MATRIXARK_TEMPORALSTORE_RUST_CLI" \
      --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
      --log "$MATRIXARK_RUST_PROXY_DAEMON_LOG" \
      --ping >/dev/null 2>&1; then
    return
  fi
  (
    flock -n 8 || exit 0
    if python3 "$ROOT/tools/matrixark_rust_proxy_daemon.py" \
        --proxy "$MATRIXARK_TEMPORALSTORE_RUST_CLI" \
        --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
        --log "$MATRIXARK_RUST_PROXY_DAEMON_LOG" \
        --ping >/dev/null 2>&1; then
      exit 0
    fi
    nohup python3 "$ROOT/tools/matrixark_rust_proxy_daemon.py" \
      --proxy "$MATRIXARK_TEMPORALSTORE_RUST_CLI" \
      --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
      --log "$MATRIXARK_RUST_PROXY_DAEMON_LOG" \
      >/dev/null 2>&1 &
  ) 8>"$MATRIXARK_RUST_PROXY_SOCKET.start.lock"
  for _ in {1..40}; do
    if python3 "$ROOT/tools/matrixark_rust_proxy_daemon.py" \
        --proxy "$MATRIXARK_TEMPORALSTORE_RUST_CLI" \
        --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
        --log "$MATRIXARK_RUST_PROXY_DAEMON_LOG" \
        --ping >/dev/null 2>&1; then
      return
    fi
    sleep 0.05
  done
}

_matrixark_start_rust_proxy_daemon

if [[ "${MATRIXARK_BACKFILL_ON_START:-auto}" != "0" && "${MATRIXARK_BACKFILL_ON_START:-auto}" != "false" && "${MATRIXARK_BACKFILL_ON_START:-auto}" != "no" ]]; then
  setsid bash "$ROOT/tools/matrixark_backfill_daemon.sh" >/dev/null 2>&1 </dev/null &
  disown 2>/dev/null || true
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
