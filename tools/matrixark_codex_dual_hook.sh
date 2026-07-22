#!/usr/bin/env bash
set -euo pipefail

EVENT="${1:-UserPromptSubmit}"
export EVENT
ROOT="${MATRIXARK_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

TMP_PAYLOAD="$(mktemp /tmp/matrixark-codex-dual-hook.XXXXXX.json)"
trap 'rm -f "$TMP_PAYLOAD"' EXIT
cat >"$TMP_PAYLOAD"

LOG_DIR="${MATRIXARK_CODEX_HOOK_LOG_DIR:-$ROOT/.local/runtime/matrixark-codex-dual-hook/logs}"
mkdir -p "$LOG_DIR"
IDEMPOTENCY_DIR="${MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR:-$ROOT/.local/runtime/matrixark-codex-dual-hook/idempotency}"
mkdir -p "$IDEMPOTENCY_DIR"
find "$IDEMPOTENCY_DIR" -type d -mmin +10 -name 'hook-*' -exec rm -rf {} + 2>/dev/null || true
PAYLOAD_HASH="$(sha256sum "$TMP_PAYLOAD" | awk '{print $1}')"
LOCK_KEY="$(printf '%s:%s' "$EVENT" "$PAYLOAD_HASH" | sha256sum | awk '{print $1}')"
if ! mkdir "$IDEMPOTENCY_DIR/hook-$LOCK_KEY" 2>/dev/null; then
  exit 0
fi

COMMON_ARGS=(
  --event "$EVENT"
  --namespace "${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
  --table "${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
  --account-id "${MATRIXARK_HOOK_ACCOUNT_ID:-acct_local}"
  --tenant-id "${MATRIXARK_HOOK_TENANT_ID:-tenant_codex}"
  --user-id "${MATRIXARK_HOOK_USER_ID:-deeproute}"
  --team "${MATRIXARK_HOOK_TEAM:-codex}"
  --max-context-tokens "${MATRIXARK_HOOK_MAX_CONTEXT_TOKENS:-10000}"
  --request-timeout-ms "${MATRIXARK_HOOK_REQUEST_TIMEOUT_MS:-90000}"
  --io-timeout-ms "${MATRIXARK_HOOK_IO_TIMEOUT_MS:-90000}"
  --session-commit-threshold "${MATRIXARK_HOOK_SESSION_COMMIT_THRESHOLD:-20}"
  --idle-commit-timeout-ms "${MATRIXARK_HOOK_IDLE_COMMIT_TIMEOUT_MS:-300000}"
)

run_cpp_hook() {
  (
    export MATRIXARK_REPO_ROOT="$ROOT"
    export MATRIXARK_MCP_BACKEND=temporalstore-direct
    export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_CPP_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
    export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_CPP_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:cpp-live-v2}"
    export MATRIXARK_HOOK_AUTOSTART_CPP="${MATRIXARK_HOOK_AUTOSTART_CPP:-1}"
    export MATRIXARK_CPP_DEPLOY_DIR="${MATRIXARK_CPP_DEPLOY_DIR:-$ROOT/.local/runtime/matrixark-cpp-live}"
    export MATRIXARK_HOOK_FAIL_OPEN=1
    export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
    export MATRIXARK_DIRECT_WRITE_QUEUE="${MATRIXARK_DIRECT_WRITE_QUEUE:-1}"
    export MATRIXARK_DIRECT_RAW_INGESTION_QUEUE="${MATRIXARK_DIRECT_RAW_INGESTION_QUEUE:-1}"
    export MATRIXARK_DIRECT_WRITE_QUEUE_MODE="${MATRIXARK_DIRECT_WRITE_QUEUE_MODE:-temporalstore}"
    export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
    export MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT="${MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT:-40000}"
    export TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-$ROOT/output-ubuntu22/release/sdk/lib/libbcache2.so}"
    bash tools/matrixark_codex_cpp_hook.sh \
      "${COMMON_ARGS[@]}" \
      --backend temporalstore-direct \
      --storage-prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
      --project "${MATRIXARK_CPP_HOOK_PROJECT:-codex-global-cpp}" \
      --codex-strict-output \
      <"$TMP_PAYLOAD"
  )
}

run_rust_hook() {
  export MATRIXARK_REPO_ROOT="$ROOT"
  export MATRIXARK_MCP_BACKEND=temporalstore-rust
  export MATRIXARK_RUST_TEMPORALSTORE_MODE="${MATRIXARK_RUST_TEMPORALSTORE_MODE:-service}"
  export MATRIXARK_RUST_SERVICE_AUTOSTART="${MATRIXARK_RUST_SERVICE_AUTOSTART:-1}"
  export MATRIXARK_RUST_SERVICE_META_ADDR="${MATRIXARK_RUST_SERVICE_META_ADDR:-127.0.0.1:17101}"
  export MATRIXARK_RUST_SERVICE_DATANODE_ADDR="${MATRIXARK_RUST_SERVICE_DATANODE_ADDR:-127.0.0.1:17102}"
  export MATRIXARK_RUST_SERVICE_PROXY_ADDR="${MATRIXARK_RUST_SERVICE_PROXY_ADDR:-127.0.0.1:17100}"
  export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_RUST_SERVICE_PROXY_ADDR}"
  export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_RUST_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust-live-v2}"
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-$ROOT/target/release/matrixark_rust_proxy}"
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-$ROOT/target/release/matrixark_rust_proxy}"
  export MATRIXARK_HOOK_AUTOSTART_CPP=0
  export MATRIXARK_MCP_AUTOSTART_CPP=0
  export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"
  export MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS="${MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS:-0}"
  export MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES="${MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES:-0}"
  export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH:-1}"
  export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH:-1}"
  export MATRIXARK_RUST_PROXY_ASYNC_VISIBILITY_PUBLISH_AFTER_FLUSH="${MATRIXARK_RUST_PROXY_ASYNC_VISIBILITY_PUBLISH_AFTER_FLUSH:-0}"
  export MATRIXARK_HOOK_FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"
  export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
  export MATRIXARK_DIRECT_WRITE_QUEUE="${MATRIXARK_DIRECT_WRITE_QUEUE:-1}"
  export MATRIXARK_DIRECT_RAW_INGESTION_QUEUE="${MATRIXARK_DIRECT_RAW_INGESTION_QUEUE:-1}"
  export MATRIXARK_DIRECT_WRITE_QUEUE_MODE="${MATRIXARK_DIRECT_WRITE_QUEUE_MODE:-temporalstore}"
  export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
  export MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT="${MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT:-40000}"

  bash tools/matrixark_codex_rust_hook.sh \
    "${COMMON_ARGS[@]}" \
    --backend temporalstore-rust \
    --metaserver "$MATRIXARK_TEMPORALSTORE_METASERVER" \
    --rust-proxy "$MATRIXARK_TEMPORALSTORE_RUST_CLI" \
    --storage-prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
    --project "${MATRIXARK_RUST_HOOK_PROJECT:-codex-global-rust}" \
    --codex-strict-output \
    <"$TMP_PAYLOAD"
}

publish_rust_service_records() {
  MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}" \
  MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}" \
  MATRIXARK_RUST_TEMPORALSTORE_PREFIX="${MATRIXARK_RUST_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust-live-v2}" \
  MATRIXARK_RUST_SERVICE_PROXY_ADDR="${MATRIXARK_RUST_SERVICE_PROXY_ADDR:-127.0.0.1:17100}" \
  MATRIXARK_RUST_SERVICE_META_ADDR="${MATRIXARK_RUST_SERVICE_META_ADDR:-127.0.0.1:17101}" \
  python3 - "$TMP_PAYLOAD" 2>>"$LOG_DIR/rust-service-publish.err" <<'PY'
import json
import os
import time
import urllib.request

payload_path = __import__("sys").argv[1]
try:
    with open(payload_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
except Exception:
    payload = {}

def first_text(source, keys):
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


prompt = first_text(payload, ["prompt", "message", "text", "input", "user_prompt", "userPrompt"])
if not prompt:
    for nested_key in ("hookInput", "hook_input", "payload", "event", "data"):
        prompt = first_text(
            payload.get(nested_key),
            ["prompt", "message", "text", "input", "user_prompt", "userPrompt"],
        )
        if prompt:
            break
if not isinstance(prompt, str) or not prompt.strip():
    print(f"skip empty prompt keys={sorted(payload.keys())}", file=__import__("sys").stderr)
    raise SystemExit(0)

now_ms = int(time.time() * 1000)
namespace = os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns")
table = os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table")
prefix = os.environ.get("MATRIXARK_RUST_TEMPORALSTORE_PREFIX", "matrixark:codex-hook:rust-live-v2")
base = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_PROXY_ADDR", "127.0.0.1:17100")
meta = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_META_ADDR", "127.0.0.1:17101")
session_id = (
    payload.get("session_id")
    or payload.get("conversation_id")
    or payload.get("thread_id")
    or os.environ.get("MATRIXARK_HOOK_SESSION_ID")
    or "codex-live-active-hook"
)
session_id = f"codex:{session_id}" if not str(session_id).startswith("codex:") else str(session_id)
hook_id = f"{os.environ.get('EVENT', 'UserPromptSubmit')}:{now_ms}"
synthetic_markers = (
    "probe",
    "smoke",
    "verification",
    "manual",
    "stdin check",
    "cmd stdin check",
    "service publisher",
    "hook fixed raw ingestion probe",
    "registered codex hook config verification",
)


def retention_fields(prompt_text):
    synthetic = any(marker in (prompt_text or "").lower() for marker in synthetic_markers)
    if not synthetic:
        return {
            "origin": "codex_hook",
            "record_class": "user_message",
            "synthetic": False,
            "retention_class": "normal",
            "expires_at_ms": None,
            "gc_eligible": False,
        }
    return {
        "origin": "codex_hook",
        "record_class": "probe",
        "synthetic": True,
        "retention_class": "debug",
        "expires_at_ms": now_ms,
        "gc_eligible": True,
    }


def post(url, path, obj):
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url + path, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


for url, path, obj in (
    (meta, "/namespaces", {"namespace": namespace}),
    (
        meta,
        "/tables",
        {
            "namespace": namespace,
            "table_name": table,
            "first_shard_id": 1,
            "shard_count": 1,
            "replica_count": 1,
        },
    ),
    (base, "/ProxyService/OpenTable", {"namespace": namespace, "table_name": table}),
):
    try:
        post(url, path, obj)
    except Exception as exc:
        print(f"init {path} failed: {exc}", file=__import__("sys").stderr)

raw_record = {
    "record_type": "agent_message",
    "role": "user",
    "text": prompt,
    "session_id": session_id,
    "session_id_source": "payload_field",
    "codex_api_event": os.environ.get("EVENT", "UserPromptSubmit"),
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}
serving_record = {
    "record_type": "context_event",
    "text": "user: " + prompt,
    "session_id": session_id,
    "session_id_source": "payload_field",
    "codex_api_event": os.environ.get("EVENT", "UserPromptSubmit"),
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}

for key, record in (
    (f"{prefix}:raw_ingestion:records", raw_record),
    (f"{prefix}:records", serving_record),
):
    field = f"{now_ms:020d}"
    value = list(json.dumps(record, separators=(",", ":")).encode("utf-8"))
    try:
        post(
            base,
            "/ProxyService/HSet",
            {
                "namespace": namespace,
                "table_name": table,
                "key": key,
                "field": field,
                "value": value,
            },
        )
    except Exception as exc:
        print(f"hset {key} failed: {exc}", file=__import__("sys").stderr)
PY
}

run_cpp_hook >"$LOG_DIR/cpp-$EVENT.out" 2>"$LOG_DIR/cpp-$EVENT.err" &
CPP_PID=$!

status=0
publish_rust_service_records || true
run_rust_hook || status=$?

if ! wait "$CPP_PID"; then
  true
fi

exit "$status"
