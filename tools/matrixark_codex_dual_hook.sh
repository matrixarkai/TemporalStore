#!/usr/bin/env bash
set -euo pipefail

EVENT="${1:-UserPromptSubmit}"
export EVENT
ROOT="${MATRIXARK_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

TMP_PAYLOAD="$(mktemp /tmp/matrixark-codex-dual-hook.XXXXXX.json)"
DIRECT_PAYLOAD="$(mktemp /tmp/matrixark-codex-dual-hook-direct.XXXXXX.json)"
trap 'rm -f "$TMP_PAYLOAD" "$DIRECT_PAYLOAD"' EXIT
cat >"$TMP_PAYLOAD"
cp "$TMP_PAYLOAD" "$DIRECT_PAYLOAD"
MATRIXARK_CODEX_DIRECT_PAYLOAD_B64="$(base64 -w 0 "$TMP_PAYLOAD" 2>/dev/null || base64 "$TMP_PAYLOAD" | tr -d '\n')"
export MATRIXARK_CODEX_DIRECT_PAYLOAD_B64

IDEMPOTENCY_DIR="${MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR:-$ROOT/.local/runtime/matrixark-codex-dual-hook/idempotency}"
mkdir -p "$IDEMPOTENCY_DIR"
find "$IDEMPOTENCY_DIR" -type d -mmin +10 -name 'hook-*' -exec rm -rf {} + 2>/dev/null || true
find "$IDEMPOTENCY_DIR" -type d -mmin +1440 -name 'record-*' -exec rm -rf {} + 2>/dev/null || true
PAYLOAD_HASH="$(sha256sum "$TMP_PAYLOAD" | awk '{print $1}')"
PAYLOAD_BYTES="$(wc -c <"$TMP_PAYLOAD" | tr -d '[:space:]')"
NATIVE_HOOK_STDOUT="/dev/null"
NATIVE_HOOK_STDERR="/dev/null"
RUST_PUBLISH_STDERR="/dev/null"
NATIVE_PUBLISH_STDERR="/dev/null"
export MATRIXARK_CODEX_HOOK_DIAG_LOG=""
HOOK_USER_ID="${MATRIXARK_HOOK_USER_ID:-${MATRIXARK_USER_ID:-${MATRIXARK_LOCAL_USER_ID:-${USER:-codex_user}}}}"
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
  --user-id "$HOOK_USER_ID"
  --team "${MATRIXARK_HOOK_TEAM:-codex}"
  --max-context-tokens "${MATRIXARK_HOOK_MAX_CONTEXT_TOKENS:-10000}"
  --request-timeout-ms "${MATRIXARK_HOOK_REQUEST_TIMEOUT_MS:-90000}"
  --io-timeout-ms "${MATRIXARK_HOOK_IO_TIMEOUT_MS:-90000}"
  --session-commit-threshold "${MATRIXARK_HOOK_SESSION_COMMIT_THRESHOLD:-20}"
  --idle-commit-timeout-ms "${MATRIXARK_HOOK_IDLE_COMMIT_TIMEOUT_MS:-300000}"
)

run_hook() {
  (
    export MATRIXARK_REPO_ROOT="$ROOT"
    export MATRIXARK_MCP_BACKEND=temporalstore-direct
    export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_NATIVE_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
    NATIVE_HOT_PREFIX="${MATRIXARK_NATIVE_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:native-live-v2}"
    export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_NATIVE_FULL_HOOK_PREFIX:-matrixark:mcp:codex}"
    export MATRIXARK_HOOK_AUTOSTART_NATIVE="${MATRIXARK_HOOK_AUTOSTART_NATIVE:-1}"
    export MATRIXARK_NATIVE_DEPLOY_DIR="${MATRIXARK_NATIVE_DEPLOY_DIR:-$ROOT/.local/runtime/matrixark-native-live}"
    export MATRIXARK_HOOK_FAIL_OPEN=1
    export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
    export MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"
    export MATRIXARK_DIRECT_WRITE_QUEUE="${MATRIXARK_DIRECT_WRITE_QUEUE:-1}"
    export MATRIXARK_DIRECT_RAW_INGESTION_QUEUE="${MATRIXARK_DIRECT_RAW_INGESTION_QUEUE:-1}"
    export MATRIXARK_DIRECT_WRITE_QUEUE_MODE="${MATRIXARK_DIRECT_WRITE_QUEUE_MODE:-temporalstore}"
    export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
    export MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT="${MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT:-40000}"
    export TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-}"
    bash tools/matrixark_codex_hook.sh \
      "${COMMON_ARGS[@]}" \
      --backend temporalstore-direct \
      --storage-prefix "$MATRIXARK_TEMPORALSTORE_PREFIX" \
      --project "${MATRIXARK_NATIVE_HOOK_PROJECT:-codex-global-native}" \
      --codex-strict-output \
      <"$TMP_PAYLOAD"
  )
}

run_rust_hook() {
  export MATRIXARK_REPO_ROOT="$ROOT"
  export MATRIXARK_MCP_BACKEND=temporalstore-rust
  export MATRIXARK_RUST_TEMPORALSTORE_MODE="${MATRIXARK_RUST_TEMPORALSTORE_MODE:-local}"
  export MATRIXARK_RUST_SERVICE_AUTOSTART="${MATRIXARK_RUST_SERVICE_AUTOSTART:-1}"
  export MATRIXARK_RUST_SERVICE_META_ADDR="${MATRIXARK_RUST_SERVICE_META_ADDR:-127.0.0.1:17101}"
  export MATRIXARK_RUST_SERVICE_DATANODE_ADDR="${MATRIXARK_RUST_SERVICE_DATANODE_ADDR:-127.0.0.1:17102}"
  export MATRIXARK_RUST_SERVICE_PROXY_ADDR="${MATRIXARK_RUST_SERVICE_PROXY_ADDR:-127.0.0.1:17100}"
  export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_RUST_SERVICE_PROXY_ADDR}"
  export MATRIXARK_TEMPORALSTORE_PREFIX="${MATRIXARK_RUST_FULL_HOOK_PREFIX:-matrixark:mcp:codex}"
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-$ROOT/target/release/matrixark_rust_proxy}"
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-$ROOT/target/release/matrixark_rust_proxy}"
  export MATRIXARK_HOOK_AUTOSTART_NATIVE=0
  export MATRIXARK_MCP_AUTOSTART_NATIVE=0
  export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"
  export MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS="${MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS:-0}"
  export MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES="${MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES:-0}"
  export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH:-1}"
  export MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH="${MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH:-1}"
  export MATRIXARK_HOOK_FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"
  export MATRIXARK_HOOK_FAST_ASYNC_INGEST="${MATRIXARK_HOOK_FAST_ASYNC_INGEST:-1}"
  export MATRIXARK_HOOK_AUTO_BATCH_EXTRACT="${MATRIXARK_HOOK_AUTO_BATCH_EXTRACT:-1}"
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
  python3 - "$DIRECT_PAYLOAD" 2>>"$RUST_PUBLISH_STDERR" <<'PY'
import base64
import json
import hashlib
import os
import re
import time
import urllib.request
from pathlib import Path
from tools.matrixark_codex_hook_payload import decode_payload, extract_identity, extract_prompt

payload_path = __import__("sys").argv[1]
try:
    payload_b64 = os.environ.get("MATRIXARK_CODEX_DIRECT_PAYLOAD_B64", "")
    raw_payload_bytes = base64.b64decode(payload_b64) if payload_b64 else Path(payload_path).read_bytes()
    payload = decode_payload(raw_payload_bytes)
except Exception:
    fallback_text = raw_payload_bytes.decode("utf-8-sig", "replace").strip() if "raw_payload_bytes" in locals() else ""
    payload = fallback_text if fallback_text else {}

def loose_payload_body(text):
    body = text.strip().lstrip("﻿").strip()
    if body.startswith("--"):
        body = body[2:].strip()
    if body.startswith("{"):
        body = body[1:].strip()
    if body.endswith("}"):
        body = body[:-1].strip()
    if body.startswith('"') and body.endswith('"'):
        body = body[1:-1]
    return body

def loose_payload_fields(text):
    fields = {}
    if not isinstance(text, str) or not text.strip():
        return fields
    body = loose_payload_body(text)
    matches = list(re.finditer(r'"?([A-Za-z_][A-Za-z0-9_-]*)\s*:', body))
    for index, match in enumerate(matches):
        key = match.group(1)
        value_start = match.end()
        value_end = matches[index + 1].start() - 1 if index + 1 < len(matches) else len(body)
        value = body[value_start:value_end].strip().strip(",").strip()
        if value:
            fields[key] = value
    return fields

def loose_payload_field(text, wanted_keys):
    if not isinstance(text, str) or not text.strip():
        return ""
    body = loose_payload_body(text)
    boundary = r'(?:input-messages|input_messages|inputMessages|prompt|message|text|input|input_prompt|input-prompt|user_prompt|user-prompt|userPrompt|session_id|session-id|sessionId|conversation_id|conversation-id|conversationId|thread_id|thread-id|threadId|codex_thread_id|codex-thread-id|codexThreadId|turn_id|turn-id|turnId|type|cwd|client|model|hook_event_name|hook-event-name|transcript_path|transcript-path|last_assistant_message|last-assistant-message)'
    input_message_keys = {"input-messages", "input_messages", "inputMessages"}
    for key in wanted_keys:
        match = re.search(r'"?' + re.escape(key) + r'\s*:', body)
        if not match:
            continue
        value_start = match.end()
        while value_start < len(body) and body[value_start].isspace():
            value_start += 1
        if key in input_message_keys:
            next_match = re.search(r',\s*"?' + boundary + r'"?\s*:', body[value_start:])
            value_end = value_start + next_match.start() if next_match else len(body)
            value = body[value_start:value_end].strip().strip(",").strip().strip('}"').strip()
            if value.startswith("[") and value.endswith("]"):
                value = value[1:-1].strip()
            return value
        if value_start < len(body) and body[value_start] == "[":
            depth = 0
            for pos in range(value_start, len(body)):
                ch = body[pos]
                if ch == "[":
                    depth += 1
                elif ch == "]":
                    depth -= 1
                    if depth == 0:
                        return body[value_start + 1:pos].strip()
            return body[value_start + 1:].strip()
        next_match = re.search(r',\s*"?[A-Za-z_][A-Za-z0-9_-]*\s*:', body[value_start:])
        value_end = value_start + next_match.start() if next_match else len(body)
        return body[value_start:value_end].strip().strip(",").strip()
    return ""

def prompt_from_input_messages(value):
    if not isinstance(value, str) or not value.strip():
        return ""
    stripped = value.strip().strip("[]").strip()
    if not stripped:
        return ""
    try:
        parsed = json.loads(value if value.lstrip().startswith("[") else f"[{value}]")
        if isinstance(parsed, list):
            parts = [
                str(part).strip()
                for part in parsed
                if isinstance(part, (str, int, float)) and str(part).strip()
            ]
            if parts:
                return parts[-1]
    except Exception:
        pass

    parts = [
        part.strip().strip(",").strip()
        for part in re.split(r"(?:\r?\n|\\n)\s*,|,\s*(?=<codex_delegation>)", stripped)
        if part.strip().strip(",").strip()
    ]
    if not parts:
        parts = [stripped]

    candidates = []
    for part in parts:
        matches = re.findall(r"<input>(.*?)</input>", part, re.DOTALL)
        candidate = matches[-1].strip() if matches else part.strip()
        if candidate:
            candidates.append(candidate)
    return candidates[-1] if candidates else stripped

def payload_field(source, keys):
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def first_text(source, keys):
    if isinstance(source, str) and source.strip():
        text = source.strip()
        loose = loose_payload_fields(text)
        input_messages = loose_payload_field(text, ["input-messages", "input_messages", "inputMessages"])
        if input_messages:
            return prompt_from_input_messages(input_messages)
        event_type = loose.get("type", "")
        if event_type and event_type != "user-prompt-submit":
            return payload_field(loose, keys)
        if loose:
            value = payload_field(loose, keys)
            return value if value else ""
        return text
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def nested_source(source, key):
    if not isinstance(source, dict):
        return {}
    value = source.get(key)
    return value if isinstance(value, (dict, str)) else {}

def transcript_path_from(source):
    if isinstance(source, str):
        return payload_field(loose_payload_fields(source), ["transcript_path", "transcriptPath"])
    if not isinstance(source, dict):
        return ""
    candidates = [source]
    for nested_key in ("hookInput", "hook_input", "payload", "event", "data"):
        nested = nested_source(source, nested_key)
        if isinstance(nested, dict):
            candidates.append(nested)
    for candidate in candidates:
        value = payload_field(candidate, ["transcript_path", "transcriptPath"])
        if value:
            return value
    return ""

def read_recent_transcript_text(path, max_bytes=8_000_000):
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size > max_bytes:
                fh.seek(max(0, size - max_bytes))
                fh.readline()
            data = fh.read(max_bytes)
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""

def stop_fallback_prompt(source):
    if os.environ.get("MATRIXARK_CODEX_ALLOW_TRANSCRIPT_FALLBACK", "0") != "1":
        return ""
    if os.environ.get("EVENT", "UserPromptSubmit") != "Stop":
        return ""
    transcript = transcript_path_from(source)
    if not transcript:
        return ""
    path = Path(transcript)
    if not path.exists():
        return ""
    latest = ""
    try:
        for line in read_recent_transcript_text(path).splitlines():
            try:
                row = json.loads(line.lstrip("\ufeff"))
            except Exception:
                continue
            payload_row = row.get("payload") if isinstance(row, dict) else {}
            if not isinstance(payload_row, dict):
                continue
            row_type = payload_row.get("type")
            if row.get("type") == "response_item" and row_type == "message" and payload_row.get("role") == "user":
                parts = []
                for item in payload_row.get("content") or []:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                text = "\n".join(part.strip() for part in parts if part.strip()).strip()
            elif row.get("type") == "event_msg" and row_type == "user_message":
                text = payload_row.get("message") if isinstance(payload_row.get("message"), str) else ""
            else:
                text = ""
            if text.strip():
                latest = text.strip()
    except Exception:
        return ""
    match = re.search(r"<input>(.*?)</input>", latest, re.DOTALL)
    if match:
        latest = match.group(1).strip()
    return latest.strip()


identity = extract_identity(payload, env=os.environ)
prompt = extract_prompt(payload, event=os.environ.get("EVENT", "UserPromptSubmit"))
if not prompt:
    prompt = stop_fallback_prompt(payload)
if not isinstance(prompt, str) or not prompt.strip():
    keys = sorted(payload.keys()) if isinstance(payload, dict) else []
    print(f"skip empty prompt event={os.environ.get('EVENT', 'UserPromptSubmit')} keys={keys}", file=__import__("sys").stderr)
    raise SystemExit(0)
prompt = prompt.strip()
event_name = os.environ.get("EVENT", "UserPromptSubmit")
if event_name != "UserPromptSubmit":
    print(f"skip non-user-prompt rust hot publish event={event_name}", file=__import__("sys").stderr)
    raise SystemExit(0)

now_ms = int(time.time() * 1000)
namespace = os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns")
table = os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table")
prefix = os.environ.get("MATRIXARK_RUST_TEMPORALSTORE_PREFIX", "matrixark:codex-hook:rust-live-v2")
profile_prefix = os.environ.get("MATRIXARK_CODEX_PROFILE_PREFIX", "matrixark:mcp:codex").rstrip(":")
account_id = os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_codex")
tenant_id = os.environ.get("MATRIXARK_TENANT_ID", "tenant_codex")
user_id = os.environ.get("MATRIXARK_USER_ID", os.environ.get("USER", "codex_user"))
base = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_PROXY_ADDR", "127.0.0.1:17100")
meta = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_META_ADDR", "127.0.0.1:17101")
session_id = identity.get("session_id") or os.environ.get("MATRIXARK_HOOK_SESSION_ID") or "codex-live-active-hook"
session_id = f"codex:{session_id}" if not str(session_id).startswith("codex:") else str(session_id)
event_name = os.environ.get("EVENT", "UserPromptSubmit")
payload_hash = hashlib.sha256(raw_payload_bytes).hexdigest()
turn_component = identity.get("turn_id") or payload_hash
record_hash = hashlib.sha256(f"{session_id}\n{turn_component}\n{prompt}".encode("utf-8", "replace")).hexdigest()
idempotency_dir = Path(os.environ.get("MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR", ""))
user_prompt_marker = idempotency_dir / f"user-prompt-rust-{record_hash}"
if event_name == "Stop" and user_prompt_marker.exists():
    print(f"skip stop prompt already ingested by UserPromptSubmit session={session_id} hash={record_hash[:12]}", file=__import__("sys").stderr)
    raise SystemExit(0)
record_marker = idempotency_dir / f"record-rust-{event_name}-{record_hash}"
try:
    record_marker.mkdir()
except FileExistsError:
    print(f"skip duplicate prompt session={session_id} hash={record_hash[:12]}", file=__import__("sys").stderr)
    raise SystemExit(0)
except Exception:
    pass
hook_id = f"{event_name}:{record_hash[:16]}"
synthetic_markers = (
    "matrixark synthetic",
    "synthetic probe",
    "codex-live-probe",
    "codex-native-live-probe",
    "manual validation",
    "hook verification",
    "reply ok only",
    "manual ingestion",
    "stdin check",
    "cmd stdin check",
    "service publisher",
    "hook fixed raw ingestion probe",
    "registered codex hook config verification",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
)


def is_synthetic_prompt(prompt_text):
    normalized = " ".join((prompt_text or "").lower().split())
    if normalized.startswith("user: "):
        normalized = normalized[6:].strip()
    if not normalized:
        return False
    if normalized.startswith(("probe ", "smoke ", "debug ", "test message ")):
        return True
    padded = f" {normalized} "
    if " smoke " in padded or " proof " in padded:
        return True
    if normalized.startswith("you are a helpful assistant. you will be presented with a user prompt, and your job is to provide a short title"):
        return True
    return any(marker in normalized for marker in synthetic_markers)


def retention_fields(prompt_text):
    synthetic = is_synthetic_prompt(prompt_text)
    return {"synthetic": synthetic}


def post(url, path, obj):
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url + path, data=data, headers={"content-type": "application/json"})
    last_exc = None
    for attempt in range(20):
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            last_exc = exc
            time.sleep(min(0.05 * (attempt + 1), 0.5))
    raise last_exc


def get_value(key):
    try:
        response = post(base, "/ProxyService/Get", {"namespace": namespace, "table_name": table, "key": key})
        value = ((response or {}).get("response") or {}).get("value")
        if isinstance(value, list):
            return bytes(value).decode("utf-8", "replace")
        return value
    except Exception:
        return None


def set_value(key, value):
    post(
        base,
        "/ProxyService/Set",
        {
            "namespace": namespace,
            "table_name": table,
            "key": key,
            "value": list(str(value).encode("utf-8")),
        },
    )


def hset_value(key, field, record):
    post(
        base,
        "/ProxyService/HSet",
        {
            "namespace": namespace,
            "table_name": table,
            "key": key,
            "field": field,
            "value": list(json.dumps(record, separators=(",", ":")).encode("utf-8")),
        },
    )


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
    "session_id_source": identity.get("session_id_source") or "fallback",
    "thread_id": identity.get("thread_id") or "",
    "turn_id": identity.get("turn_id") or "",
    "codex_api_event": event_name,
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}
serving_record = {
    "record_type": "context_event",
    "event_id_hash": int(hashlib.sha256(f"event\n{record_hash}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1),
    "text": "user: " + prompt,
    "session_id": session_id,
    "scope": {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "session_id": session_id},
    "session_id_source": identity.get("session_id_source") or "fallback",
    "thread_id": identity.get("thread_id") or "",
    "turn_id": identity.get("turn_id") or "",
    "codex_api_event": event_name,
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}

def append_record(count_key, records_prefix, record, hot_count_key=None):
    if count_key not in counter_cache:
        try:
            counter_cache[count_key] = int(get_value(count_key) or "0")
        except Exception:
            counter_cache[count_key] = 0
    counter_cache[count_key] += 1
    sequence = counter_cache[count_key]
    legacy_sharded_key = f"{records_prefix}:{sequence // 10000:06d}"
    legacy_field = f"{sequence:020d}"
    page_key = f"{records_prefix}:{sequence // 256:06d}"
    page_field = f"{sequence % 256:020d}"
    hset_value(page_key, page_field, record)
    hset_value(legacy_sharded_key, legacy_field, record)
    hset_value(records_prefix, legacy_field, record)
    set_value(count_key, sequence)
    if hot_count_key:
        if hot_count_key not in counter_cache:
            try:
                counter_cache[hot_count_key] = int(get_value(hot_count_key) or "0")
            except Exception:
                counter_cache[hot_count_key] = 0
        counter_cache[hot_count_key] += 1
        set_value(hot_count_key, counter_cache[hot_count_key])
    print(
        f"published {records_prefix} sequence={sequence} field={page_field} type={record.get('record_type')}",
        file=__import__("sys").stderr,
    )
    return sequence


def compact_text(value, limit=420):
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def compact_embedding(text, dim=8):
    digest = hashlib.sha256(str(text or "").encode("utf-8", "replace")).digest()
    return [round(((digest[index] / 255.0) * 2.0) - 1.0, 6) for index in range(dim)]


def live_topic_entities(prompt_text):
    lowered = (prompt_text or "").lower()
    topics = [
        ("rust_temporalstore", ("rust", "rust temporalstore", "rust hook", "rust service")),
        ("native_temporalstore", ("native", "native", "native temporalstore")),
        ("codex_hook", ("codex", "hook", "userpromptsubmit", "realtime ingestion")),
        ("context_management", ("context", "entity", "summary", "segment", "retrieval")),
        ("oss_reader_benchmark", ("qwen", "ollama", "vllm", "locomo", "longmemeval")),
        ("storage_engine", ("storage", "page", "block", "zone", "stream", "raft", "gc")),
        ("matrixobject_shared_storage", ("matrixobject", "s3", "object storage", "shared storage")),
    ]
    selected = []
    for name, needles in topics:
        if any(needle in lowered for needle in needles):
            selected.append(name)
    return selected[:4]


def rust_live_extraction_records():
    if event_name != "UserPromptSubmit" or is_synthetic_prompt(prompt):
        return []
    event_id_hash = serving_record["event_id_hash"]
    session_scope = {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "session_id": session_id}
    profile_scope = {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id}
    session_node_path = [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}", "conversation:codex_hook"]
    profile_node_path = [f"tenant:{tenant_id}", f"user:{user_id}", "profile:long_term_memory"]
    session_node_hash = int(hashlib.sha256("/".join(session_node_path).encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
    profile_node_hash = int(hashlib.sha256("/".join(profile_node_path).encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
    common_extracted = {
        "session_id": session_id,
        "source_hook_id": hook_id,
        "updated_at_ms": now_ms,
    }
    records = [
        {
            "record_type": "context_embedding",
            "embedding_type": "event_text",
            "ref_type": "event",
            "ref_hash": event_id_hash,
            "node_hash": session_node_hash,
            "node_path": session_node_path,
            "dim": 8,
            "model": "deterministic_compact_sha256_v1",
            "vector": compact_embedding("user: " + prompt),
            "scope": session_scope,
            "memory_scope": "session",
            "session_continuity": "same_session",
            **common_extracted,
        }
    ]
    topics = live_topic_entities(prompt)
    for topic in topics:
        entity_hash = int(hashlib.sha256(f"entity\n{session_id}\n{topic}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
        profile_entity_hash = int(hashlib.sha256(f"profile_entity\n{tenant_id}\n{user_id}\n{topic}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
        entity_state = compact_text(f"{topic}: {prompt}", 700)
        records.append(
            {
                "record_type": "context_entity",
                "entity_hash": entity_hash,
                "node_hash": session_node_hash,
                "node_path": session_node_path,
                "scope": session_scope,
                "access_scope": session_scope,
                "entity_type": "topic",
                "entity_name": topic,
                "state": entity_state,
                "memory_scope": "session",
                "session_continuity": "same_session",
                "source_event_ids": [event_id_hash],
                **common_extracted,
            }
        )
        records.append(
            {
                "record_type": "context_index",
                "index_name": f"entity_type:{topic}",
                "ref_type": "context_entity",
                "ref_hash": entity_hash,
                "ref_hashes": [entity_hash],
                "data_model": "context_entity",
                "node_hash": session_node_hash,
                "scope": session_scope,
                **common_extracted,
            }
        )
        records.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "entity_state",
                "ref_type": "entity",
                "ref_hash": entity_hash,
                "node_hash": session_node_hash,
                "node_path": session_node_path,
                "dim": 8,
                "model": "deterministic_compact_sha256_v1",
                "vector": compact_embedding("topic " + entity_state),
                "scope": session_scope,
                "memory_scope": "session",
                "session_continuity": "same_session",
                **common_extracted,
            }
        )
        profile_record = {
            "record_type": "context_entity",
            "entity_hash": profile_entity_hash,
            "node_hash": profile_node_hash,
            "node_path": profile_node_path,
            "scope": profile_scope,
            "access_scope": profile_scope,
            "entity_type": "topic",
            "entity_name": topic,
            "state": entity_state,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "promoted_from_memory_scope": "session",
            "profile_promotion_policy": "always_when_profile_scope_available",
            "profile_promotion_blocker": "",
            "source_session_ids": [session_id],
            "source_entity_hashes": [entity_hash],
            "source_event_ids": [event_id_hash],
            **common_extracted,
        }
        records.append(profile_record)
        records.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "entity_state",
                "ref_type": "entity",
                "ref_hash": profile_entity_hash,
                "node_hash": profile_node_hash,
                "node_path": profile_node_path,
                "dim": 8,
                "model": "deterministic_compact_sha256_v1",
                "vector": compact_embedding("profile topic " + entity_state),
                "scope": profile_scope,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                **common_extracted,
            }
        )
        for index_name in (f"entity_type:{topic}", "memory_scope:user_profile", "session_continuity:cross_session"):
            records.append(
                {
                    "record_type": "context_index",
                    "index_name": index_name,
                    "ref_type": "context_profile_entity",
                    "ref_hash": profile_entity_hash,
                    "ref_hashes": [profile_entity_hash],
                    "data_model": "context_profile_entity",
                    "node_hash": profile_node_hash,
                    "scope": profile_scope,
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    **common_extracted,
                }
            )
        records.append(
            {
                "record_type": "context_summary_dirty",
                "dirty_hash": int(hashlib.sha256(f"profile_dirty\n{profile_entity_hash}\n{record_hash}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1),
                "node_hash": profile_node_hash,
                "node_path": profile_node_path,
                "scope": profile_scope,
                "dirty_reason": "profile_entity_promoted",
                "source_ref_type": "entity",
                "source_entity_hash": profile_entity_hash,
                "source_event_hash": event_id_hash,
                "source_memory_scopes": ["session", "user_profile"],
                "source_session_continuities": ["same_session", "cross_session"],
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                **common_extracted,
            }
        )
    summary_id = hashlib.sha256(f"summary\n{session_id}\n{record_hash}".encode("utf-8")).hexdigest()[:16]
    records.append(
        {
            "record_type": "context_summary",
            "summary_id": summary_id,
            "summary_type": "session_l0",
            "scope": f"session:{session_id}",
            "text": compact_text(f"Latest Codex user prompt: {prompt}", 900),
            **common_extracted,
        }
    )
    return records


counter_cache = {}

published_raw = False
for count_key, records_prefix, record in (
    (f"{prefix}:raw_ingestion:record_count", f"{prefix}:raw_ingestion:records", raw_record),
    (f"{prefix}:record_count", f"{prefix}:records", serving_record),
):
    try:
        append_record(count_key, records_prefix, record, count_key.replace(":record_count", ":hot_record_count"))
        if record.get("record_type") == "agent_message":
            published_raw = True
    except Exception as exc:
        print(f"publish {records_prefix} failed: {exc}", file=__import__("sys").stderr)

for record in rust_live_extraction_records():
    for destination_prefix in dict.fromkeys((prefix, profile_prefix)):
        try:
            append_record(
                f"{destination_prefix}:record_count",
                f"{destination_prefix}:records",
                record,
                f"{destination_prefix}:hot_record_count",
            )
        except Exception as exc:
            print(
                f"publish extracted Rust record to {destination_prefix} failed: {exc}",
                file=__import__("sys").stderr,
            )

if event_name == "UserPromptSubmit" and published_raw:
    try:
        user_prompt_marker.mkdir()
    except Exception:
        pass
PY
}

publish_direct_records() {
  MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}" \
  MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}" \
  MATRIXARK_NATIVE_TEMPORALSTORE_PREFIX="${MATRIXARK_NATIVE_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:native-live-v2}" \
  MATRIXARK_NATIVE_TEMPORALSTORE_METASERVER="${MATRIXARK_NATIVE_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}" \
  TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-}" \
  python3 - "$DIRECT_PAYLOAD" 2>>"$NATIVE_PUBLISH_STDERR" <<'PY'
import base64
import json
import hashlib
import os
import re
import sys
import time
from pathlib import Path
from tools.matrixark_codex_hook_payload import decode_payload, extract_identity, extract_prompt

payload_path = sys.argv[1]
repo = Path(os.environ.get("MATRIXARK_REPO_ROOT") or Path.cwd())
sys.path.insert(0, str(repo / "sdk/python"))
try:
    from temporalstore.client import Client, Options
except Exception as exc:
    print(f"import temporalstore client failed: {exc}", file=sys.stderr)
    raise SystemExit(0)

try:
    payload_b64 = os.environ.get("MATRIXARK_CODEX_DIRECT_PAYLOAD_B64", "")
    raw_payload_bytes = base64.b64decode(payload_b64) if payload_b64 else Path(payload_path).read_bytes()
    payload = decode_payload(raw_payload_bytes)
except Exception:
    fallback_text = raw_payload_bytes.decode("utf-8-sig", "replace").strip() if "raw_payload_bytes" in locals() else ""
    payload = fallback_text if fallback_text else {}

def loose_payload_body(text):
    body = text.strip().lstrip("﻿").strip()
    if body.startswith("--"):
        body = body[2:].strip()
    if body.startswith("{"):
        body = body[1:].strip()
    if body.endswith("}"):
        body = body[:-1].strip()
    if body.startswith('"') and body.endswith('"'):
        body = body[1:-1]
    return body

def loose_payload_fields(text):
    fields = {}
    if not isinstance(text, str) or not text.strip():
        return fields
    body = loose_payload_body(text)
    matches = list(re.finditer(r'"?([A-Za-z_][A-Za-z0-9_-]*)\s*:', body))
    for index, match in enumerate(matches):
        key = match.group(1)
        value_start = match.end()
        value_end = matches[index + 1].start() - 1 if index + 1 < len(matches) else len(body)
        value = body[value_start:value_end].strip().strip(",").strip()
        if value:
            fields[key] = value
    return fields

def loose_payload_field(text, wanted_keys):
    if not isinstance(text, str) or not text.strip():
        return ""
    body = loose_payload_body(text)
    boundary = r'(?:input-messages|input_messages|inputMessages|prompt|message|text|input|input_prompt|input-prompt|user_prompt|user-prompt|userPrompt|session_id|session-id|sessionId|conversation_id|conversation-id|conversationId|thread_id|thread-id|threadId|codex_thread_id|codex-thread-id|codexThreadId|turn_id|turn-id|turnId|type|cwd|client|model|hook_event_name|hook-event-name|transcript_path|transcript-path|last_assistant_message|last-assistant-message)'
    input_message_keys = {"input-messages", "input_messages", "inputMessages"}
    for key in wanted_keys:
        match = re.search(r'"?' + re.escape(key) + r'\s*:', body)
        if not match:
            continue
        value_start = match.end()
        while value_start < len(body) and body[value_start].isspace():
            value_start += 1
        if key in input_message_keys:
            next_match = re.search(r',\s*"?' + boundary + r'"?\s*:', body[value_start:])
            value_end = value_start + next_match.start() if next_match else len(body)
            value = body[value_start:value_end].strip().strip(",").strip().strip('}"').strip()
            if value.startswith("[") and value.endswith("]"):
                value = value[1:-1].strip()
            return value
        if value_start < len(body) and body[value_start] == "[":
            depth = 0
            for pos in range(value_start, len(body)):
                ch = body[pos]
                if ch == "[":
                    depth += 1
                elif ch == "]":
                    depth -= 1
                    if depth == 0:
                        return body[value_start + 1:pos].strip()
            return body[value_start + 1:].strip()
        next_match = re.search(r',\s*"?[A-Za-z_][A-Za-z0-9_-]*\s*:', body[value_start:])
        value_end = value_start + next_match.start() if next_match else len(body)
        return body[value_start:value_end].strip().strip(",").strip()
    return ""

def prompt_from_input_messages(value):
    if not isinstance(value, str) or not value.strip():
        return ""
    stripped = value.strip().strip("[]").strip()
    if not stripped:
        return ""
    try:
        parsed = json.loads(value if value.lstrip().startswith("[") else f"[{value}]")
        if isinstance(parsed, list):
            parts = [
                str(part).strip()
                for part in parsed
                if isinstance(part, (str, int, float)) and str(part).strip()
            ]
            if parts:
                return parts[-1]
    except Exception:
        pass

    parts = [
        part.strip().strip(",").strip()
        for part in re.split(r"(?:\r?\n|\\n)\s*,|,\s*(?=<codex_delegation>)", stripped)
        if part.strip().strip(",").strip()
    ]
    if not parts:
        parts = [stripped]

    candidates = []
    for part in parts:
        matches = re.findall(r"<input>(.*?)</input>", part, re.DOTALL)
        candidate = matches[-1].strip() if matches else part.strip()
        if candidate:
            candidates.append(candidate)
    return candidates[-1] if candidates else stripped

def payload_field(source, keys):
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def first_text(source, keys):
    if isinstance(source, str) and source.strip():
        text = source.strip()
        loose = loose_payload_fields(text)
        input_messages = loose_payload_field(text, ["input-messages", "input_messages", "inputMessages"])
        if input_messages:
            return prompt_from_input_messages(input_messages)
        event_type = loose.get("type", "")
        if event_type and event_type != "user-prompt-submit":
            return payload_field(loose, keys)
        if loose:
            value = payload_field(loose, keys)
            return value if value else ""
        return text
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def nested_source(source, key):
    if not isinstance(source, dict):
        return {}
    value = source.get(key)
    return value if isinstance(value, (dict, str)) else {}

def transcript_path_from(source):
    if isinstance(source, str):
        return payload_field(loose_payload_fields(source), ["transcript_path", "transcriptPath"])
    if not isinstance(source, dict):
        return ""
    candidates = [source]
    for nested_key in ("hookInput", "hook_input", "payload", "event", "data"):
        nested = nested_source(source, nested_key)
        if isinstance(nested, dict):
            candidates.append(nested)
    for candidate in candidates:
        value = payload_field(candidate, ["transcript_path", "transcriptPath"])
        if value:
            return value
    return ""

def read_recent_transcript_text(path, max_bytes=8_000_000):
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size > max_bytes:
                fh.seek(max(0, size - max_bytes))
                fh.readline()
            data = fh.read(max_bytes)
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""

def stop_fallback_prompt(source):
    if os.environ.get("MATRIXARK_CODEX_ALLOW_TRANSCRIPT_FALLBACK", "0") != "1":
        return ""
    if os.environ.get("EVENT", "UserPromptSubmit") != "Stop":
        return ""
    transcript = transcript_path_from(source)
    if not transcript:
        return ""
    path = Path(transcript)
    if not path.exists():
        return ""
    latest = ""
    try:
        for line in read_recent_transcript_text(path).splitlines():
            try:
                row = json.loads(line.lstrip("\ufeff"))
            except Exception:
                continue
            payload_row = row.get("payload") if isinstance(row, dict) else {}
            if not isinstance(payload_row, dict):
                continue
            row_type = payload_row.get("type")
            if row.get("type") == "response_item" and row_type == "message" and payload_row.get("role") == "user":
                parts = []
                for item in payload_row.get("content") or []:
                    if isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                text = "\n".join(part.strip() for part in parts if part.strip()).strip()
            elif row.get("type") == "event_msg" and row_type == "user_message":
                text = payload_row.get("message") if isinstance(payload_row.get("message"), str) else ""
            else:
                text = ""
            if text.strip():
                latest = text.strip()
    except Exception:
        return ""
    match = re.search(r"<input>(.*?)</input>", latest, re.DOTALL)
    if match:
        latest = match.group(1).strip()
    return latest.strip()

identity = extract_identity(payload, env=os.environ)
prompt = extract_prompt(payload, event=os.environ.get("EVENT", "UserPromptSubmit"))
if not prompt:
    prompt = stop_fallback_prompt(payload)
if not prompt:
    keys = sorted(payload.keys()) if isinstance(payload, dict) else []
    print(f"skip empty prompt event={os.environ.get('EVENT', 'UserPromptSubmit')} keys={keys}", file=sys.stderr)
    raise SystemExit(0)
prompt = prompt.strip()
event_name = os.environ.get("EVENT", "UserPromptSubmit")
if event_name != "UserPromptSubmit":
    print(f"skip non-user-prompt native hot publish event={event_name}", file=sys.stderr)
    raise SystemExit(0)

now_ms = int(time.time() * 1000)
namespace = os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns")
table = os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table")
prefix = os.environ.get("MATRIXARK_NATIVE_TEMPORALSTORE_PREFIX", "matrixark:codex-hook:native-live-v2")
profile_prefix = os.environ.get("MATRIXARK_CODEX_PROFILE_PREFIX", "matrixark:mcp:codex").rstrip(":")
account_id = os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_codex")
tenant_id = os.environ.get("MATRIXARK_TENANT_ID", "tenant_codex")
user_id = os.environ.get("MATRIXARK_USER_ID", os.environ.get("USER", "codex_user"))
session_id = identity.get("session_id") or os.environ.get("MATRIXARK_HOOK_SESSION_ID") or "codex-live-active-hook"
session_id = f"codex:{session_id}" if not str(session_id).startswith("codex:") else str(session_id)
event_name = os.environ.get("EVENT", "UserPromptSubmit")
payload_hash = hashlib.sha256(raw_payload_bytes).hexdigest()
turn_component = identity.get("turn_id") or payload_hash
record_hash = hashlib.sha256(f"{session_id}\n{turn_component}\n{prompt}".encode("utf-8", "replace")).hexdigest()
idempotency_dir = Path(os.environ.get("MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR", ""))
user_prompt_marker = idempotency_dir / f"user-prompt-native-{record_hash}"
if event_name == "Stop" and user_prompt_marker.exists():
    print(f"skip stop prompt already ingested by UserPromptSubmit session={session_id} hash={record_hash[:12]}", file=sys.stderr)
    raise SystemExit(0)
record_marker = idempotency_dir / f"record-native-{event_name}-{record_hash}"
try:
    record_marker.mkdir()
except FileExistsError:
    print(f"skip duplicate prompt session={session_id} hash={record_hash[:12]}", file=sys.stderr)
    raise SystemExit(0)
except Exception:
    pass
hook_id = f"{event_name}:{record_hash[:16]}"
synthetic_markers = (
    "matrixark synthetic",
    "synthetic probe",
    "codex-live-probe",
    "codex-native-live-probe",
    "manual validation",
    "hook verification",
    "reply ok only",
    "manual ingestion",
    "stdin check",
    "cmd stdin check",
    "service publisher",
    "hook fixed raw ingestion probe",
    "registered codex hook config verification",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
)

def is_synthetic_prompt(prompt_text):
    normalized = " ".join((prompt_text or "").lower().split())
    if normalized.startswith("user: "):
        normalized = normalized[6:].strip()
    if not normalized:
        return False
    if normalized.startswith(("probe ", "smoke ", "debug ", "test message ")):
        return True
    padded = f" {normalized} "
    if " smoke " in padded or " proof " in padded:
        return True
    if normalized.startswith("you are a helpful assistant. you will be presented with a user prompt, and your job is to provide a short title"):
        return True
    return any(marker in normalized for marker in synthetic_markers)

def retention_fields(prompt_text):
    return {"synthetic": is_synthetic_prompt(prompt_text)}

try:
    client = Client(
        Options(
            metaserver_addr=os.environ.get("MATRIXARK_NATIVE_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=3000,
            io_timeout_ms=3000,
        ),
        library_path=os.environ.get("TEMPORALSTORE_LIB", ""),
    )
except Exception as exc:
    print(f"connect native temporalstore failed: {exc}", file=sys.stderr)
    raise SystemExit(0)

def get_value(key):
    try:
        value = client.get_string(key)
        if isinstance(value, bytes):
            return value.decode("utf-8", "replace")
        return value
    except Exception:
        return None

def set_value(key, value):
    client.put_string(key, str(value))

def hset_value(key, field, record):
    client.hset(key, field, json.dumps(record, separators=(",", ":")))

def compact_text(value, limit=420):
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."

def compact_embedding(text, dim=8):
    digest = hashlib.sha256(str(text or "").encode("utf-8", "replace")).digest()
    return [round(((digest[index] / 255.0) * 2.0) - 1.0, 6) for index in range(dim)]

def live_topic_entities(prompt_text):
    lowered = (prompt_text or "").lower()
    topics = [
        ("rust_temporalstore", ("rust", "rust temporalstore", "rust hook", "rust service")),
        ("native_temporalstore", ("native", "native", "native temporalstore")),
        ("codex_hook", ("codex", "hook", "userpromptsubmit", "realtime ingestion")),
        ("context_management", ("context", "entity", "summary", "segment", "retrieval")),
        ("oss_reader_benchmark", ("qwen", "ollama", "vllm", "locomo", "longmemeval")),
        ("storage_engine", ("storage", "page", "block", "zone", "stream", "raft", "gc")),
        ("matrixobject_shared_storage", ("matrixobject", "s3", "object storage", "shared storage")),
    ]
    selected = []
    for name, needles in topics:
        if any(needle in lowered for needle in needles):
            selected.append(name)
    return selected[:4]

def native_live_extraction_records():
    if event_name != "UserPromptSubmit" or is_synthetic_prompt(prompt):
        return []
    event_id_hash = serving_record["event_id_hash"]
    session_scope = {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "session_id": session_id}
    profile_scope = {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id}
    session_node_path = [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}", "conversation:codex_hook"]
    profile_node_path = [f"tenant:{tenant_id}", f"user:{user_id}", "profile:long_term_memory"]
    session_node_hash = int(hashlib.sha256("/".join(session_node_path).encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
    profile_node_hash = int(hashlib.sha256("/".join(profile_node_path).encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
    common_extracted = {
        "session_id": session_id,
        "source_hook_id": hook_id,
        "updated_at_ms": now_ms,
    }
    records = [
        {
            "record_type": "context_embedding",
            "embedding_type": "event_text",
            "ref_type": "event",
            "ref_hash": event_id_hash,
            "node_hash": session_node_hash,
            "node_path": session_node_path,
            "dim": 8,
            "model": "deterministic_compact_sha256_v1",
            "vector": compact_embedding("user: " + prompt),
            "scope": session_scope,
            "memory_scope": "session",
            "session_continuity": "same_session",
            **common_extracted,
        }
    ]
    for topic in live_topic_entities(prompt):
        entity_hash = int(hashlib.sha256(f"entity\n{session_id}\n{topic}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
        profile_entity_hash = int(hashlib.sha256(f"profile_entity\n{tenant_id}\n{user_id}\n{topic}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)
        entity_state = compact_text(f"{topic}: {prompt}", 700)
        records.append({
            "record_type": "context_entity",
            "entity_hash": entity_hash,
            "node_hash": session_node_hash,
            "node_path": session_node_path,
            "scope": session_scope,
            "access_scope": session_scope,
            "entity_type": "topic",
            "entity_name": topic,
            "state": entity_state,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "source_event_ids": [event_id_hash],
            **common_extracted,
        })
        records.append({
            "record_type": "context_index",
            "index_name": f"entity_type:{topic}",
            "ref_type": "context_entity",
            "ref_hash": entity_hash,
            "ref_hashes": [entity_hash],
            "data_model": "context_entity",
            "node_hash": session_node_hash,
            "scope": session_scope,
            **common_extracted,
        })
        records.append({
            "record_type": "context_embedding",
            "embedding_type": "entity_state",
            "ref_type": "entity",
            "ref_hash": entity_hash,
            "node_hash": session_node_hash,
            "node_path": session_node_path,
            "dim": 8,
            "model": "deterministic_compact_sha256_v1",
            "vector": compact_embedding("topic " + entity_state),
            "scope": session_scope,
            "memory_scope": "session",
            "session_continuity": "same_session",
            **common_extracted,
        })
        profile_record = {
            "record_type": "context_entity",
            "entity_hash": profile_entity_hash,
            "node_hash": profile_node_hash,
            "node_path": profile_node_path,
            "scope": profile_scope,
            "access_scope": profile_scope,
            "entity_type": "topic",
            "entity_name": topic,
            "state": entity_state,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "promoted_from_memory_scope": "session",
            "profile_promotion_policy": "always_when_profile_scope_available",
            "profile_promotion_blocker": "",
            "source_session_ids": [session_id],
            "source_entity_hashes": [entity_hash],
            "source_event_ids": [event_id_hash],
            **common_extracted,
        }
        records.append(profile_record)
        records.append({
            "record_type": "context_embedding",
            "embedding_type": "entity_state",
            "ref_type": "entity",
            "ref_hash": profile_entity_hash,
            "node_hash": profile_node_hash,
            "node_path": profile_node_path,
            "dim": 8,
            "model": "deterministic_compact_sha256_v1",
            "vector": compact_embedding("profile topic " + entity_state),
            "scope": profile_scope,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            **common_extracted,
        })
        for index_name in (f"entity_type:{topic}", "memory_scope:user_profile", "session_continuity:cross_session"):
            records.append({
                "record_type": "context_index",
                "index_name": index_name,
                "ref_type": "context_profile_entity",
                "ref_hash": profile_entity_hash,
                "ref_hashes": [profile_entity_hash],
                "data_model": "context_profile_entity",
                "node_hash": profile_node_hash,
                "scope": profile_scope,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                **common_extracted,
            })
        records.append({
            "record_type": "context_summary_dirty",
            "dirty_hash": int(hashlib.sha256(f"profile_dirty\n{profile_entity_hash}\n{record_hash}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1),
            "node_hash": profile_node_hash,
            "node_path": profile_node_path,
            "scope": profile_scope,
            "dirty_reason": "profile_entity_promoted",
            "source_ref_type": "entity",
            "source_entity_hash": profile_entity_hash,
            "source_event_hash": event_id_hash,
            "source_memory_scopes": ["session", "user_profile"],
            "source_session_continuities": ["same_session", "cross_session"],
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            **common_extracted,
        })
    summary_id = hashlib.sha256(f"summary\n{session_id}\n{record_hash}".encode("utf-8")).hexdigest()[:16]
    records.append({
        "record_type": "context_summary",
        "summary_id": summary_id,
        "summary_type": "session_l0",
        "scope": f"session:{session_id}",
        "text": compact_text(f"Latest Codex user prompt: {prompt}", 900),
        **common_extracted,
    })
    return records

common = {
    "role": "user",
    "session_id": session_id,
    "session_id_source": identity.get("session_id_source") or "fallback",
    "thread_id": identity.get("thread_id") or "",
    "turn_id": identity.get("turn_id") or "",
    "codex_api_event": event_name,
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}
raw_record = {"record_type": "agent_message", "text": prompt, **common}
serving_record = {
    "record_type": "context_event",
    "event_id_hash": int(hashlib.sha256(f"event\n{record_hash}".encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1),
    "text": "user: " + prompt,
    "scope": {"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "session_id": session_id},
    **common,
}

counter_cache = {}

def append_record(count_key, records_prefix, record, hot_count_key=None):
    if count_key not in counter_cache:
        try:
            counter_cache[count_key] = int(get_value(count_key) or "0")
        except Exception:
            counter_cache[count_key] = 0
    counter_cache[count_key] += 1
    sequence = counter_cache[count_key]
    legacy_field = f"{sequence:020d}"
    legacy_sharded_key = f"{records_prefix}:{sequence // 10000:06d}"
    page_key = f"{records_prefix}:{sequence // 256:06d}"
    page_field = f"{sequence % 256:020d}"
    hset_value(page_key, page_field, record)
    hset_value(legacy_sharded_key, legacy_field, record)
    hset_value(records_prefix, legacy_field, record)
    set_value(count_key, sequence)
    if hot_count_key:
        if hot_count_key not in counter_cache:
            try:
                counter_cache[hot_count_key] = int(get_value(hot_count_key) or "0")
            except Exception:
                counter_cache[hot_count_key] = 0
        counter_cache[hot_count_key] += 1
        set_value(hot_count_key, counter_cache[hot_count_key])
    print(f"published {records_prefix} sequence={sequence} field={page_field} type={record.get('record_type')}", file=sys.stderr)
    return sequence

published_raw = False
for count_key, records_prefix, record in (
    (f"{prefix}:raw_ingestion:record_count", f"{prefix}:raw_ingestion:records", raw_record),
    (f"{prefix}:record_count", f"{prefix}:records", serving_record),
):
    try:
        append_record(count_key, records_prefix, record, count_key.replace(":record_count", ":hot_record_count"))
        if record.get("record_type") == "agent_message":
            published_raw = True
    except Exception as exc:
        print(f"publish {records_prefix} failed: {exc}", file=sys.stderr)

for record in native_live_extraction_records():
    for destination_prefix in dict.fromkeys((prefix, profile_prefix)):
        try:
            append_record(
                f"{destination_prefix}:record_count",
                f"{destination_prefix}:records",
                record,
                f"{destination_prefix}:hot_record_count",
            )
        except Exception as exc:
            print(f"publish extracted record to {destination_prefix} failed: {exc}", file=sys.stderr)

if event_name == "UserPromptSubmit" and published_raw:
    try:
        user_prompt_marker.mkdir()
    except Exception:
        pass
PY
}

publish_rust_service_records || true
publish_direct_records || true

run_hook >"$NATIVE_HOOK_STDOUT" 2>"$NATIVE_HOOK_STDERR" &
NATIVE_PID=$!

status=0
run_rust_hook || status=$?

if ! wait "$NATIVE_PID"; then
  true
fi

exit "$status"
