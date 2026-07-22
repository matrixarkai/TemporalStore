#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-${SCRIPT_ROOT}}"
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
export MATRIXARK_HOOK_TOOL_CALL_TIMEOUT_MS="${MATRIXARK_HOOK_TOOL_CALL_TIMEOUT_MS:-8000}"
export MATRIXARK_HOOK_RETRIEVE_TIMEOUT_MS="${MATRIXARK_HOOK_RETRIEVE_TIMEOUT_MS:-5000}"
export MATRIXARK_HOOK_TRACE_APPEND_TIMEOUT_MS="${MATRIXARK_HOOK_TRACE_APPEND_TIMEOUT_MS:-750}"
export MATRIXARK_HOOK_CLOSE_TIMEOUT_MS="${MATRIXARK_HOOK_CLOSE_TIMEOUT_MS:-750}"
export MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-0}"
export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
export MATRIXARK_HOOK_AUTOSTART_CPP="${MATRIXARK_HOOK_AUTOSTART_CPP:-1}"
export MATRIXARK_CPP_DEPLOY_DIR="${MATRIXARK_CPP_DEPLOY_DIR:-$ROOT/.local/runtime/matrixark-cpp-live}"
export MATRIXARK_CPP_METASERVER_EXTRA_FLAGS="${MATRIXARK_CPP_METASERVER_EXTRA_FLAGS:---metaserver_bthread_concurrency=4 --metaserver_service_thread_num=2 --metaserver_raft_worker_num=2 --metaserver_raft_reader_num=1 --metaserver_raft_flusher_num=1 --metaserver_raft_applier_num=2 --metaserver_raft_executor_num=2 --metaserver_raft_snapshot_num=1 --metaserver_meta_check_routine_interval_sec=30 --metaserver_balance_routine_interval_ms=60000 --metaserver_convict_routine_interval_ms=30000 --metaserver_proxy_calibrate_interval_ms=60000}"
export MATRIXARK_CPP_SERVER_EXTRA_FLAGS="${MATRIXARK_CPP_SERVER_EXTRA_FLAGS:---bthread_concurrency=4 --worker_num=2 --data_raft_worker_num=2 --storage_async=true}"
export MATRIXARK_CPP_METASERVER_CPU_AFFINITY="${MATRIXARK_CPP_METASERVER_CPU_AFFINITY:-0}"
export MATRIXARK_CPP_SERVER_CPU_AFFINITY="${MATRIXARK_CPP_SERVER_CPU_AFFINITY:-1}"

# Hooks run in Codex's critical path. Default to fast deterministic providers;
# OSS/OpenAI providers can be enabled explicitly for offline/debug hook tests.
export MATRIXARK_EMBEDDING_PROVIDER="${MATRIXARK_EMBEDDING_PROVIDER:-hash}"
export MATRIXARK_REQUIRE_OSS_EMBEDDINGS="${MATRIXARK_REQUIRE_OSS_EMBEDDINGS:-0}"
export MATRIXARK_EMBEDDING_MODEL_PATH="${MATRIXARK_EMBEDDING_MODEL_PATH:-$ROOT/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2}"
export MATRIXARK_UNDERSTANDING_PROVIDER="${MATRIXARK_UNDERSTANDING_PROVIDER:-rules}"
export MATRIXARK_SEGMENT_PROVIDER="${MATRIXARK_SEGMENT_PROVIDER:-deterministic}"
export MATRIXARK_REQUIRE_OSS_UNDERSTANDING="${MATRIXARK_REQUIRE_OSS_UNDERSTANDING:-0}"

# Codex hooks run in the editor critical path. C++ MatrixArk context ingest can
# fan out into nodes, indexes, summaries, raw records, idempotency, and audit.
# Keep hook writes accepted quickly and let the adapter's async queue/native
# append path do the heavier persistence work outside the hook boundary.
export MATRIXARK_AUDIT_MODE="${MATRIXARK_AUDIT_MODE:-async}"
export MATRIXARK_DIRECT_AUDIT_MODE="${MATRIXARK_DIRECT_AUDIT_MODE:-buffered}"
export MATRIXARK_DIRECT_WRITE_QUEUE="${MATRIXARK_DIRECT_WRITE_QUEUE:-1}"
export MATRIXARK_DIRECT_WRITE_QUEUE_MODE="${MATRIXARK_DIRECT_WRITE_QUEUE_MODE:-temporalstore}"
export MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT="${MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT:-1}"
export MATRIXARK_DIRECT_RAW_INGESTION_QUEUE="${MATRIXARK_DIRECT_RAW_INGESTION_QUEUE:-1}"

_matrixark_apply_local_cpu_bounds() {
  if command -v taskset >/dev/null 2>&1; then
    local meta_pid server_pid
    meta_pid="$(pgrep -f "$ROOT/output-ubuntu22/release/bcache2-metaserver" | head -1 || true)"
    server_pid="$(pgrep -f "$ROOT/output-ubuntu22/release/bcache2-server" | head -1 || true)"
    if [[ -n "$meta_pid" && -n "$MATRIXARK_CPP_METASERVER_CPU_AFFINITY" ]]; then
      taskset -acp "$MATRIXARK_CPP_METASERVER_CPU_AFFINITY" "$meta_pid" >/dev/null 2>&1 || true
    fi
    if [[ -n "$server_pid" && -n "$MATRIXARK_CPP_SERVER_CPU_AFFINITY" ]]; then
      taskset -acp "$MATRIXARK_CPP_SERVER_CPU_AFFINITY" "$server_pid" >/dev/null 2>&1 || true
    fi
  fi
}

if [[ "$MATRIXARK_HOOK_AUTOSTART_CPP" == "1" && "$MATRIXARK_MCP_BACKEND" == "temporalstore-direct" ]]; then
  host="${MATRIXARK_TEMPORALSTORE_METASERVER%%:*}"
  port="${MATRIXARK_TEMPORALSTORE_METASERVER##*:}"
  if ! timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
    if ! BUILD_TYPE="${BUILD_TYPE:-Release}" DEPLOY_DIR="$MATRIXARK_CPP_DEPLOY_DIR" PERSIST_DEPLOY_DIR=1 METASERVER_EXTRA_FLAGS="$MATRIXARK_CPP_METASERVER_EXTRA_FLAGS" SERVER_EXTRA_FLAGS="$MATRIXARK_CPP_SERVER_EXTRA_FLAGS" bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >/dev/null 2>&1; then
      BUILD_TYPE="${BUILD_TYPE:-Release}" DEPLOY_DIR="$MATRIXARK_CPP_DEPLOY_DIR" PERSIST_DEPLOY_DIR=0 METASERVER_EXTRA_FLAGS="$MATRIXARK_CPP_METASERVER_EXTRA_FLAGS" SERVER_EXTRA_FLAGS="$MATRIXARK_CPP_SERVER_EXTRA_FLAGS" bash "$ROOT/tools/deploy_local_ubuntu22.sh" start >/dev/null 2>&1 || true
    fi
  fi
  _matrixark_apply_local_cpu_bounds
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
