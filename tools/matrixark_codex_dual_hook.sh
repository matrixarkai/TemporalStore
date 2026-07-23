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

LOG_DIR="${MATRIXARK_CODEX_HOOK_LOG_DIR:-$ROOT/.local/runtime/matrixark-codex-dual-hook/logs}"
mkdir -p "$LOG_DIR"
IDEMPOTENCY_DIR="${MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR:-$ROOT/.local/runtime/matrixark-codex-dual-hook/idempotency}"
mkdir -p "$IDEMPOTENCY_DIR"
find "$IDEMPOTENCY_DIR" -type d -mmin +10 -name 'hook-*' -exec rm -rf {} + 2>/dev/null || true
find "$IDEMPOTENCY_DIR" -type d -mmin +1440 -name 'record-*' -exec rm -rf {} + 2>/dev/null || true
PAYLOAD_HASH="$(sha256sum "$TMP_PAYLOAD" | awk '{print $1}')"
PAYLOAD_BYTES="$(wc -c <"$TMP_PAYLOAD" | tr -d '[:space:]')"
DIAG_LOG="$LOG_DIR/dispatch-diagnostics.jsonl"
export MATRIXARK_CODEX_HOOK_DIAG_LOG="$DIAG_LOG"
printf '{"ts_ms":%s,"event":"%s","payload_bytes":%s,"payload_hash":"%s"}\n' \
  "$(date +%s%3N)" "$EVENT" "${PAYLOAD_BYTES:-0}" "$PAYLOAD_HASH" >>"$DIAG_LOG" 2>/dev/null || true
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
  python3 - "$DIRECT_PAYLOAD" 2>>"$LOG_DIR/rust-service-publish.err" <<'PY'
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
except Exception as exc:
    try:
        with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG", ""), "a", encoding="utf-8") as diag:
            diag.write(json.dumps({
                "ts_ms": int(time.time() * 1000),
                "event": os.environ.get("EVENT", "UserPromptSubmit"),
                "payload_parse_error": str(exc),
                "payload_path": payload_path,
            }, separators=(",", ":")) + "\n")
    except Exception:
        pass
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
    for key in wanted_keys:
        match = re.search(r'"?' + re.escape(key) + r'\s*:', body)
        if not match:
            continue
        value_start = match.end()
        while value_start < len(body) and body[value_start].isspace():
            value_start += 1
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
    matches = re.findall(r"<input>(.*?)</input>", value, re.DOTALL)
    if matches:
        return matches[-1].strip()
    parts = [part.strip() for part in value.split(",<codex_delegation>") if part.strip()]
    if len(parts) > 1:
        return ("<codex_delegation>" + parts[-1]).strip()
    return value.strip()

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
        input_messages = loose_payload_field(text, ["input-messages", "input_messages"])
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
    env_lengths = {
        key: len(value)
        for key, value in os.environ.items()
        if key.startswith(("CODEX", "MATRIXARK", "OPENAI"))
    }
    try:
        with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG", ""), "a", encoding="utf-8") as diag:
            diag.write(json.dumps({
                "ts_ms": int(time.time() * 1000),
                "event": os.environ.get("EVENT", "UserPromptSubmit"),
                "empty_prompt": True,
                "payload_keys": keys,
                "env_value_lengths": env_lengths,
            }, separators=(",", ":")) + "\n")
    except Exception:
        pass
    print(f"skip empty prompt event={os.environ.get('EVENT', 'UserPromptSubmit')} keys={keys}", file=__import__("sys").stderr)
    raise SystemExit(0)

now_ms = int(time.time() * 1000)
namespace = os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns")
table = os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table")
prefix = os.environ.get("MATRIXARK_RUST_TEMPORALSTORE_PREFIX", "matrixark:codex-hook:rust-live-v2")
base = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_PROXY_ADDR", "127.0.0.1:17100")
meta = "http://" + os.environ.get("MATRIXARK_RUST_SERVICE_META_ADDR", "127.0.0.1:17101")
session_id = identity.get("session_id") or os.environ.get("MATRIXARK_HOOK_SESSION_ID") or "codex-live-active-hook"
session_id = f"codex:{session_id}" if not str(session_id).startswith("codex:") else str(session_id)
record_hash = hashlib.sha256(f"{session_id}\n{prompt}".encode("utf-8", "replace")).hexdigest()
record_marker = Path(os.environ.get("MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR", "")) / f"record-rust-{record_hash}"
try:
    record_marker.mkdir()
except FileExistsError:
    print(f"skip duplicate prompt session={session_id} hash={record_hash[:12]}", file=__import__("sys").stderr)
    raise SystemExit(0)
except Exception:
    pass
hook_id = f"{os.environ.get('EVENT', 'UserPromptSubmit')}:{record_hash[:16]}"
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
    "proof",
    "reply ok only",
    "current thread fix",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
)


def retention_fields(prompt_text):
    synthetic = any(marker in (prompt_text or "").lower() for marker in synthetic_markers)
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
    "session_id_source": identity.get("session_id_source") or "fallback",
    "thread_id": identity.get("thread_id") or "",
    "turn_id": identity.get("turn_id") or "",
    "codex_api_event": os.environ.get("EVENT", "UserPromptSubmit"),
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}

for count_key, records_prefix, record in (
    (f"{prefix}:raw_ingestion:record_count", f"{prefix}:raw_ingestion:records", raw_record),
    (f"{prefix}:record_count", f"{prefix}:records", serving_record),
):
    try:
        sequence = int(get_value(count_key) or "0") + 1
    except Exception:
        sequence = 1
    sharded_key = f"{records_prefix}:{sequence // 10000:06d}"
    field = f"{sequence:020d}"
    try:
        hset_value(sharded_key, field, record)
        hset_value(records_prefix, field, record)
        set_value(count_key, sequence)
        print(
            f"published {records_prefix} sequence={sequence} field={field}",
            file=__import__("sys").stderr,
        )
    except Exception as exc:
        print(f"publish {records_prefix} failed: {exc}", file=__import__("sys").stderr)
PY
}

publish_cpp_direct_records() {
  MATRIXARK_TEMPORALSTORE_NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}" \
  MATRIXARK_TEMPORALSTORE_TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}" \
  MATRIXARK_CPP_TEMPORALSTORE_PREFIX="${MATRIXARK_CPP_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:cpp-live-v2}" \
  MATRIXARK_CPP_TEMPORALSTORE_METASERVER="${MATRIXARK_CPP_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}" \
  TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-$ROOT/output-ubuntu22/release/sdk/lib/libbcache2.so}" \
  python3 - "$DIRECT_PAYLOAD" 2>>"$LOG_DIR/cpp-direct-publish.err" <<'PY'
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
except Exception as exc:
    try:
        with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG", ""), "a", encoding="utf-8") as diag:
            diag.write(json.dumps({
                "ts_ms": int(time.time() * 1000),
                "event": os.environ.get("EVENT", "UserPromptSubmit"),
                "payload_parse_error": str(exc),
                "payload_path": payload_path,
            }, separators=(",", ":")) + "\n")
    except Exception:
        pass
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
    for key in wanted_keys:
        match = re.search(r'"?' + re.escape(key) + r'\s*:', body)
        if not match:
            continue
        value_start = match.end()
        while value_start < len(body) and body[value_start].isspace():
            value_start += 1
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
    matches = re.findall(r"<input>(.*?)</input>", value, re.DOTALL)
    if matches:
        return matches[-1].strip()
    parts = [part.strip() for part in value.split(",<codex_delegation>") if part.strip()]
    if len(parts) > 1:
        return ("<codex_delegation>" + parts[-1]).strip()
    return value.strip()

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
        input_messages = loose_payload_field(text, ["input-messages", "input_messages"])
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
    env_lengths = {
        key: len(value)
        for key, value in os.environ.items()
        if key.startswith(("CODEX", "MATRIXARK", "OPENAI"))
    }
    try:
        with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG", ""), "a", encoding="utf-8") as diag:
            diag.write(json.dumps({
                "ts_ms": int(time.time() * 1000),
                "event": os.environ.get("EVENT", "UserPromptSubmit"),
                "empty_prompt": True,
                "payload_keys": keys,
                "env_value_lengths": env_lengths,
            }, separators=(",", ":")) + "\n")
    except Exception:
        pass
    print(f"skip empty prompt event={os.environ.get('EVENT', 'UserPromptSubmit')} keys={keys}", file=sys.stderr)
    raise SystemExit(0)

now_ms = int(time.time() * 1000)
namespace = os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns")
table = os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table")
prefix = os.environ.get("MATRIXARK_CPP_TEMPORALSTORE_PREFIX", "matrixark:codex-hook:cpp-live-v2")
session_id = identity.get("session_id") or os.environ.get("MATRIXARK_HOOK_SESSION_ID") or "codex-live-active-hook"
session_id = f"codex:{session_id}" if not str(session_id).startswith("codex:") else str(session_id)
record_hash = hashlib.sha256(f"{session_id}\n{prompt}".encode("utf-8", "replace")).hexdigest()
record_marker = Path(os.environ.get("MATRIXARK_CODEX_HOOK_IDEMPOTENCY_DIR", "")) / f"record-cpp-{record_hash}"
try:
    record_marker.mkdir()
except FileExistsError:
    print(f"skip duplicate prompt session={session_id} hash={record_hash[:12]}", file=sys.stderr)
    raise SystemExit(0)
except Exception:
    pass
hook_id = f"{os.environ.get('EVENT', 'UserPromptSubmit')}:{record_hash[:16]}"
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
    "proof",
    "reply ok only",
    "current thread fix",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
)

def retention_fields(prompt_text):
    return {"synthetic": any(marker in (prompt_text or "").lower() for marker in synthetic_markers)}

try:
    client = Client(
        Options(
            metaserver_addr=os.environ.get("MATRIXARK_CPP_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=3000,
            io_timeout_ms=3000,
        ),
        library_path=os.environ.get("TEMPORALSTORE_LIB", str(repo / "output-ubuntu22/release/sdk/lib/libbcache2.so")),
    )
except Exception as exc:
    print(f"connect cpp temporalstore failed: {exc}", file=sys.stderr)
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

common = {
    "role": "user",
    "session_id": session_id,
    "session_id_source": identity.get("session_id_source") or "fallback",
    "thread_id": identity.get("thread_id") or "",
    "turn_id": identity.get("turn_id") or "",
    "codex_api_event": os.environ.get("EVENT", "UserPromptSubmit"),
    "hook_id": hook_id,
    "hook_type": "before_llm",
    "hook_observed_at_ms": now_ms,
    **retention_fields(prompt),
}
raw_record = {"record_type": "agent_message", "text": prompt, **common}
serving_record = {"record_type": "context_event", "text": "user: " + prompt, **common}

for count_key, records_prefix, record in (
    (f"{prefix}:raw_ingestion:record_count", f"{prefix}:raw_ingestion:records", raw_record),
    (f"{prefix}:record_count", f"{prefix}:records", serving_record),
):
    try:
        sequence = int(get_value(count_key) or "0") + 1
    except Exception:
        sequence = 1
    field = f"{sequence:020d}"
    sharded_key = f"{records_prefix}:{sequence // 10000:06d}"
    try:
        hset_value(sharded_key, field, record)
        hset_value(records_prefix, field, record)
        set_value(count_key, sequence)
        print(f"published {records_prefix} sequence={sequence} field={field}", file=sys.stderr)
    except Exception as exc:
        print(f"publish {records_prefix} failed: {exc}", file=sys.stderr)
PY
}

publish_rust_service_records || true
publish_cpp_direct_records || true

run_cpp_hook >"$LOG_DIR/cpp-$EVENT.out" 2>"$LOG_DIR/cpp-$EVENT.err" &
CPP_PID=$!

status=0
run_rust_hook || status=$?

if ! wait "$CPP_PID"; then
  true
fi

exit "$status"
