#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""HTTP/JSON facade for the MatrixArk management portal."""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.request

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log, adapter_ensure_backend_ready
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log, adapter_ensure_backend_ready

# Gateway default ceiling for the retrieve token budget when a caller omits
# max_context_tokens. Historically hardcoded to 10000 (50x below the backend
# DEFAULT_MAX_CONTEXT_TOKENS=500000), which silently under-returned on deep/
# broad queries. Now env-tunable and defaulted to the backend default so the
# gateway default == backend default. This is a CEILING, not a target: the
# pack still fills only to relevant refs (min_score-gated).
try:
    from tools.matrixark_mcp_runtime_config import DEFAULT_MAX_CONTEXT_TOKENS as _BACKEND_DEFAULT_MAX_CONTEXT_TOKENS
except ModuleNotFoundError:  # Direct script execution from tools/.
    try:
        from matrixark_mcp_runtime_config import DEFAULT_MAX_CONTEXT_TOKENS as _BACKEND_DEFAULT_MAX_CONTEXT_TOKENS
    except Exception:
        _BACKEND_DEFAULT_MAX_CONTEXT_TOKENS = 500000
GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS = int(
    os.environ.get("MATRIXARK_GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS", str(_BACKEND_DEFAULT_MAX_CONTEXT_TOKENS))
)

def _coerce_http_value(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _http_query_args(parsed: Any) -> Json:
    args: Json = {}
    scope: Json = {}
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if not values:
            continue
        value: Any = _coerce_http_value(values[-1])
        if key.startswith("scope."):
            scope[key.split(".", 1)[1]] = value
        elif key in {"account_id", "tenant_id", "user_id", "session_id", "agent_name", "team", "project"}:
            scope[key] = value
            args[key] = value
        else:
            args[key] = value
    if scope:
        args["scope"] = scope
    return args


def _http_api_key(headers: Any, args: Json) -> None:
    authorization = headers.get("Authorization", "") if headers else ""
    if authorization.lower().startswith("bearer "):
        args.setdefault("api_key", authorization.split(" ", 1)[1].strip())
    header_key = headers.get("X-MatrixArk-API-Key", "") if headers else ""
    if header_key:
        args.setdefault("api_key", header_key.strip())


def _http_cloud_mode(server: Any) -> bool:
    mode = os.environ.get("MATRIXARK_HTTP_MODE", "").strip().lower()
    if mode in {"cloud", "prod", "production"}:
        return True
    return getattr(getattr(server, "access", None), "mode", "dev") == "enforced"


def _http_trusted_gateway(headers: Any) -> bool:
    if not headers:
        return False
    trusted = headers.get("X-MatrixArk-Trusted-Gateway", "") or headers.get("X-MatrixArk-Gateway-Verified", "")
    return str(trusted).strip().lower() in {"1", "true", "yes", "trusted"}


def _http_has_auth(headers: Any, args: Json) -> bool:
    _http_api_key(headers, args)
    return bool(args.get("api_key")) or _http_trusted_gateway(headers)


HTTP_TOOL_ROUTES: dict[str, str] = {
    # Enterprise real-time ingestion + retrieval. Verticals that own their API loop (no CLI hooks)
    # POST turns here as they happen and pull managed context to build their prompt. matrixark_ingest
    # already carries the async / session-buffer / idle-commit / provisional-vs-final machinery and
    # accepts a `messages` array (single or batched). /api/ingest defaults to async_processing for a
    # fast, non-blocking ack under scaled load (see do_POST). Multi-tenant via the header API key.
    "/api/ingest": "matrixark_ingest",
    "/api/retrieve": "matrixark_retrieve",
    "/api/session_commit": "matrixark_session_commit",
    "/api/management_portal": "matrixark_management_portal",
    "/api/ingestion_dashboard": "matrixark_ingestion_dashboard",
    "/api/backend_metrics": "matrixark_backend_metrics",
    "/api/audit": "matrixark_admin_audit",
    "/api/replay": "matrixark_replay",
    "/api/auth/signup": "matrixark_auth_signup",
    "/api/auth/login": "matrixark_auth_login",
    "/api/auth/sso_login": "matrixark_auth_sso_login",
    "/api/auth/sso_callback": "matrixark_auth_sso_callback",
    "/api/admin/create_account": "matrixark_admin_create_account",
    "/api/admin/create_user": "matrixark_admin_create_user",
    "/api/admin/create_api_key": "matrixark_admin_create_api_key",
    "/api/admin/list_api_keys": "matrixark_admin_list_api_keys",
    "/api/admin/rotate_api_key": "matrixark_admin_rotate_api_key",
    "/api/admin/revoke_api_key": "matrixark_admin_revoke_api_key",
    "/api/admin/audit": "matrixark_admin_audit",
}
HTTP_SPECIAL_TOOLS = {"matrixark_agent_hook"}


def _agent_hook_scope(body: Json, normalized: Json) -> Json:
    body_scope = body.get("scope") if isinstance(body.get("scope"), dict) else {}
    normalized_scope = normalized.get("scope") if isinstance(normalized.get("scope"), dict) else {}
    scope: Json = {
        "account_id": body_scope.get("account_id") or normalized_scope.get("account_id") or normalized.get("account_id") or "acct_agent",
        "tenant_id": body_scope.get("tenant_id") or normalized_scope.get("tenant_id") or normalized.get("tenant_id") or f"tenant_{normalized.get('agent') or 'agent'}",
        "user_id": body_scope.get("user_id") or normalized_scope.get("user_id") or normalized.get("user_id") or "agent_user",
        "session_id": body_scope.get("session_id") or normalized_scope.get("session_id") or normalized.get("session_id") or normalized.get("conversation_id") or "",
    }
    for key in ("team", "project"):
        value = body_scope.get(key) or normalized_scope.get(key) or normalized.get(key)
        if value:
            scope[key] = value
    return {key: str(value) for key, value in scope.items() if value not in (None, "")}


def _agent_hook_metadata(body: Json, normalized: Json, raw_payload: Json) -> Json:
    event = normalized.get("event") or body.get("event", "")
    return {
        "source": f"{normalized.get('agent') or 'agent'}_remote_hook",
        "agent": normalized.get("agent", ""),
        "agent_event": event,
        "hook_type": normalized.get("hook_type", ""),
        "lifecycle_stage": normalized.get("lifecycle_stage", ""),
        "codex_event": event,
        "session_id_source": normalized.get("session_id_source", ""),
        "normalized_event": normalized,
        "raw_hook_payload": raw_payload,
    }


def _agent_hook_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _agent_hook_auto_batch_extract(body: Json, normalized: Json, event: str, text: str) -> bool:
    explicit = normalized.get("auto_batch_extract", body.get("auto_batch_extract"))
    if explicit is not None:
        return _agent_hook_bool(explicit)
    if _agent_hook_bool(normalized.get("should_commit")):
        return False
    if not text:
        return False
    if _agent_hook_bool(normalized.get("should_retrieve")):
        return True
    return event in {
        "AssistantResponse",
        "UserPromptSubmit",
        "PostToolUse",
        "PreToolUse",
        "PermissionRequest",
    }


def _agent_hook_commit_reason(body: Json, normalized: Json, event: str) -> str:
    explicit = str(normalized.get("commit_reason") or body.get("commit_reason") or "").strip()
    if explicit:
        return explicit
    lifecycle_stage = str(normalized.get("lifecycle_stage") or "").lower()
    if event in {"IdleTimeout", "SessionIdle"} or "idle" in lifecycle_stage:
        return "idle_timeout"
    return "hook_boundary"


def _agent_hook_call(server: Any, body: Json) -> Json:
    normalized = body.get("normalized_event") if isinstance(body.get("normalized_event"), dict) else {}
    raw_payload = body.get("raw_payload") if isinstance(body.get("raw_payload"), dict) else {}
    if not normalized:
        raise MatrixArkError("body.normalized_event is required")
    event = str(normalized.get("event") or body.get("event") or "")
    text = str(normalized.get("text") or "").strip()
    role = str(normalized.get("role") or "unknown").strip() or "unknown"
    scope = _agent_hook_scope(body, normalized)
    args_common: Json = {"scope": scope}
    if body.get("api_key"):
        args_common["api_key"] = body.get("api_key")
    storage_options = body.get("storage_options") if isinstance(body.get("storage_options"), dict) else {}
    if not storage_options:
        storage_options = {"route": "shared_store_async"}
    metadata = _agent_hook_metadata(body, normalized, raw_payload)
    observed_at_ms = normalized.get("timestamp_ms") or now_ms()
    agent_hook = {
        "source": normalized.get("agent", "agent"),
        "hook_type": normalized.get("hook_type", ""),
        "hook_id": f"{normalized.get('agent') or 'agent'}:{event}:{observed_at_ms}",
        "observed_at_ms": observed_at_ms,
        "trigger": event,
        "auto_captured": True,
        "session_id_source": normalized.get("session_id_source", ""),
    }
    result: Json = {
        "status": "ok",
        "event": event,
        "scope": scope,
        "ingested": {},
        "retrieved": {},
        "committed": {},
    }
    if text:
        auto_batch_extract = _agent_hook_auto_batch_extract(body, normalized, event, text)
        ingest_args: Json = {
            **args_common,
            "messages": [{"role": role, "content": text}],
            "metadata": metadata,
            "agent_hook": agent_hook,
            "storage_options": storage_options,
            "auto_batch_extract": auto_batch_extract,
        }
        threshold = normalized.get("session_buffer_threshold", body.get("session_buffer_threshold"))
        if threshold is not None:
            ingest_args["session_buffer_threshold"] = int(threshold)
        idle_timeout = normalized.get("idle_commit_timeout_ms", body.get("idle_commit_timeout_ms"))
        if idle_timeout is not None:
            ingest_args["idle_commit_timeout_ms"] = int(idle_timeout)
        result["ingested"] = server.call_tool("matrixark_ingest", ingest_args)
    if _agent_hook_bool(normalized.get("should_retrieve")):
        retrieve_metadata = {
            "retrieval_source": "remote_agent_hook",
            "codex_event": event,
            "hook_type": normalized.get("hook_type", ""),
            "lifecycle_stage": normalized.get("lifecycle_stage", ""),
            "codex_session_id_source": normalized.get("session_id_source", ""),
            "session_id_source": normalized.get("session_id_source", ""),
        }
        retrieve_args: Json = {
            **args_common,
            "query": str(body.get("query") or text or event)[:500],
            "max_context_tokens": int(body.get("max_context_tokens") or normalized.get("max_context_tokens") or body.get("budget") or normalized.get("budget") or GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS),
            "audit_mode": str(body.get("audit_mode") or normalized.get("audit_mode") or "telemetry_only"),
            "audit_sample_rate": float(body.get("audit_sample_rate") or normalized.get("audit_sample_rate") or 0.0),
            "metadata": retrieve_metadata,
            "retrieval_request_metadata": retrieve_metadata,
        }
        local_context = normalized.get("local_context") or raw_payload.get("local_context")
        if isinstance(local_context, list) and local_context:
            retrieve_args["local_context"] = local_context
        result["retrieved"] = server.call_tool("matrixark_retrieve", retrieve_args)
    if _agent_hook_bool(normalized.get("should_commit")):
        commit_reason = _agent_hook_commit_reason(body, normalized, event)
        explicit_force = normalized.get("force", body.get("force"))
        force = _agent_hook_bool(explicit_force) if explicit_force is not None else commit_reason != "idle_timeout"
        commit_args: Json = {
            **args_common,
            "force": force,
            "commit_reason": commit_reason,
            "extraction_phase": str(normalized.get("extraction_phase") or "final"),
            "final_session_boundary": _agent_hook_bool(normalized.get("final_session_boundary", True)),
            "metadata": metadata,
            "agent_hook": {**agent_hook, "hook_type": "session_commit"},
            "storage_options": storage_options,
        }
        threshold = normalized.get("session_buffer_threshold", body.get("session_buffer_threshold"))
        if threshold is not None:
            commit_args["threshold_messages"] = int(threshold)
        idle_timeout = normalized.get("idle_commit_timeout_ms", body.get("idle_commit_timeout_ms"))
        if idle_timeout is not None:
            commit_args["idle_timeout_ms"] = int(idle_timeout)
        result["committed"] = server.call_tool("matrixark_session_commit", commit_args)
    return result


_CODEX_HOOK_SHARD_SIZE = 10000
_CODEX_HOOK_SYNTHETIC_MARKERS = {
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
    "registered codex hook config verification",
    "fixed raw ingestion probe",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
}


def _hook_text_is_synthetic(text_value: str) -> bool:
    normalized = " ".join(str(text_value or "").lower().split())
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
    return any(marker in normalized for marker in _CODEX_HOOK_SYNTHETIC_MARKERS)


def _hook_decode_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, list) and all(isinstance(item, int) for item in value):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def _namespace_and_table(args: Json) -> tuple[str, str]:
    """Which namespace and table a backend addresses: the caller's, else the environment, else the
    deployment defaults.

    All three readers below resolved this themselves, in the same two lines, and carried their own
    copy of both default names with them. Renaming a default would have had to be done in three
    places and would have been right in whichever one the author happened to open.
    """
    return (
        str(args.get("namespace") or os.environ.get("MATRIXARK_NAMESPACE") or "deploy_ns"),
        str(args.get("table") or os.environ.get("MATRIXARK_TABLE") or "deploy_table"),
    )


class _HookStoreReader:
    name = "unknown"

    def get_string(self, key: str) -> str | None:
        raise NotImplementedError

    def hget(self, key: str, field: str) -> str | None:
        raise NotImplementedError


class _HookStoreReader(_HookStoreReader):
    name = "native"

    def __init__(self, args: Json) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        sdk_path = repo_root / "sdk/python"
        if str(sdk_path) not in sys.path:
            sys.path.insert(0, str(sdk_path))
        from temporalstore.client import Client, Options  # type: ignore

        lib_path = str(
            args.get("native_library_path")
            or os.environ.get("TEMPORALSTORE_LIB")
            or repo_root / "output-ubuntu22/release/sdk/lib/libtemporalstore.so"
        )
        metaserver = str(args.get("native_metaserver") or os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER") or "127.0.0.1:18000")
        namespace, table = _namespace_and_table(args)
        self.client = Client(
            Options(
                metaserver_addr=metaserver,
                namespace_name=namespace,
                table_name=table,
                request_timeout_ms=int(args.get("request_timeout_ms") or 3000),
                io_timeout_ms=int(args.get("io_timeout_ms") or 3000),
            ),
            library_path=lib_path,
        )

    def get_string(self, key: str) -> str | None:
        try:
            return _hook_decode_value(self.client.get_string(key))
        except Exception:
            return None

    def hget(self, key: str, field: str) -> str | None:
        try:
            return _hook_decode_value(self.client.hget(key, field))
        except Exception:
            return None


class _RustServiceHookStoreReader(_HookStoreReader):
    name = "rust-service"

    def __init__(self, args: Json) -> None:
        self.addr = str(args.get("rust_service_addr") or os.environ.get("MATRIXARK_RUST_SERVICE_PROXY_ADDR") or "127.0.0.1:17100")
        self.namespace, self.table = _namespace_and_table(args)

    def _post(self, path: str, payload: Json) -> Json | None:
        try:
            request = urllib.request.Request(
                f"http://{self.addr}{path}",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=2.0) as response:
                parsed = json.loads(response.read().decode("utf-8"))
                return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def get_string(self, key: str) -> str | None:
        payload = {"namespace": self.namespace, "table_name": self.table, "key": key}
        response = self._post("/ProxyService/Get", payload)
        return _hook_decode_value((response or {}).get("response", {}).get("value"))

    def hget(self, key: str, field: str) -> str | None:
        payload = {"namespace": self.namespace, "table_name": self.table, "key": key, "field": field}
        response = self._post("/ProxyService/HGet", payload)
        return _hook_decode_value((response or {}).get("response", {}).get("value"))


class _RustLocalHookStoreReader(_HookStoreReader):
    name = "rust-local-proxy"

    def __init__(self, args: Json) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        self.proxy_bin = str(args.get("rust_proxy_bin") or os.environ.get("MATRIXARK_RUST_PROXY_BIN") or repo_root / "target/release/matrixark_rust_proxy")
        self.namespace, self.table = _namespace_and_table(args)
        self.cwd = str(repo_root)

    def _call(self, op: str, **kwargs: Any) -> Json | None:
        env = os.environ.copy()
        env["MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG"] = "1"
        env.setdefault("MATRIXARK_LOCAL_MODE", "no-metaserver")
        env.setdefault("MATRIXARK_TEMPORALSTORE_METASERVER", "local")
        request = {"op": op, "metaserver": "local", "namespace": self.namespace, "table": self.table, **kwargs}
        try:
            completed = subprocess.run(
                [self.proxy_bin, "--debug-single-shot"],
                input=json.dumps(request) + "\n",
                text=True,
                capture_output=True,
                timeout=4,
                env=env,
                cwd=self.cwd,
            )
        except Exception:
            return None
        for line in reversed(completed.stdout.splitlines()):
            stripped = line.strip()
            if stripped.startswith("{"):
                try:
                    parsed = json.loads(stripped)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    continue
        return None

    def get_string(self, key: str) -> str | None:
        return _hook_decode_value((self._call("get_string", key=key) or {}).get("value"))

    def hget(self, key: str, field: str) -> str | None:
        return _hook_decode_value((self._call("hget", key=key, field=field) or {}).get("value"))


def _hook_prefixes(args: Json, backend: str) -> list[str]:
    explicit = str(args.get("prefix") or "").strip()
    if explicit:
        return [explicit]
    if backend == "native":
        configured = os.environ.get("MATRIXARK_NATIVE_TEMPORALSTORE_PREFIX")
        return [prefix for prefix in [configured, "matrixark:codex-hook:native-live-v2", "matrixark:codex-hook"] if prefix]
    configured = os.environ.get("MATRIXARK_RUST_TEMPORALSTORE_PREFIX")
    return [prefix for prefix in [configured, "matrixark:codex-hook:rust-live-v2", "matrixark:codex-hook:rust"] if prefix]


def _hook_record_count(reader: _HookStoreReader, prefix: str) -> tuple[str | None, int]:
    for key in (f"{prefix}:raw_ingestion:record_count", f"{prefix}:record_count"):
        value = reader.get_string(key)
        try:
            return key, int(str(value))
        except Exception:
            continue
    return None, 0


def _hook_parse_record(raw: str | None) -> Json | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return {"raw": raw}
    return parsed if isinstance(parsed, dict) else {"raw": parsed}


def _hook_message_candidates(record: Json) -> list[tuple[str, str]]:
    role = record.get("role") or record.get("author_role")
    text_value = record.get("text") or record.get("content") or record.get("message") or record.get("query") or record.get("text_preview")
    messages = record.get("messages")
    if isinstance(messages, list):
        rows: list[tuple[str, str]] = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            message_role = message.get("role") or role
            message_text = message.get("content") or message.get("text") or text_value
            rows.append((str(message_role or ""), str(message_text or "")))
        if rows:
            return rows
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    if not text_value:
        text_value = envelope.get("query") or envelope.get("visible_local_context") or envelope.get("text")
    if not role:
        role = envelope.get("role") or record.get("speaker")
    return [(str(role or ""), str(text_value or ""))]


def _hook_timestamp_ms(record: Json) -> int:
    for key in ("ingestion_time_ms", "hook_observed_at_ms", "timestamp_ms", "created_at_ms", "event_time_ms", "ts_ms"):
        value = record.get(key)
        try:
            if value is not None:
                return int(value)
        except Exception:
            continue
    return 0


def _hook_session_id(record: Json) -> str:
    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    for key in ("session_id", "thread_id", "conversation_id", "codex_thread_id"):
        for source in (record, scope, envelope):
            value = source.get(key) if isinstance(source, dict) else None
            if value:
                return str(value)
    return ""


def _hook_is_real_user(record: Json, role: str, text_value: str, *, real_user_only: bool, include_synthetic: bool) -> bool:
    inferred_role = _hook_infer_role(record, role, text_value)
    if real_user_only and inferred_role.lower() != "user":
        return False
    stripped_text = text_value.strip()
    if not stripped_text:
        return False
    lowered_text = stripped_text.lower()
    if lowered_text.startswith(("<environment_context>", "<codex_internal_context>", "<developer_context>", "<system_context>")):
        return False
    if include_synthetic:
        return True
    if record.get("synthetic") is True and _hook_text_is_synthetic(text_value):
        return False
    return not _hook_text_is_synthetic(text_value)


def _hook_infer_role(record: Json, role: str, text_value: str) -> str:
    if role:
        return role
    lowered = text_value.strip().lower()
    if lowered.startswith("assistant:") or lowered in {"ok", "done"}:
        return "assistant"
    event_name = str(record.get("codex_api_event") or record.get("event") or "")
    hook_type = str(record.get("hook_type") or "")
    record_type = str(record.get("record_type") or "")
    if event_name == "UserPromptSubmit" or hook_type == "before_llm" or record_type == "agent_message":
        return "user"
    return role


def _hook_row_identity(row: Json) -> tuple[str, str, str]:
    text = " ".join(str(row.get("text", "")).lower().split())
    if text.startswith("user: "):
        text = text[6:].strip()
    text_key = text[:512]
    return (str(row.get("backend") or ""), str(row.get("session_id") or ""), text_key)


def _hook_prefer_row(candidate: Json, current: Json) -> Json:
    candidate_ts = int(candidate.get("timestamp_ms") or 0)
    current_ts = int(current.get("timestamp_ms") or 0)
    if candidate_ts != current_ts:
        return candidate if candidate_ts > current_ts else current
    candidate_projection = str(candidate.get("projection") or "")
    current_projection = str(current.get("projection") or "")
    if candidate_projection == "raw_ingestion" and current_projection != "raw_ingestion":
        return candidate
    if current_projection == "raw_ingestion" and candidate_projection != "raw_ingestion":
        return current
    return candidate if int(candidate.get("sequence") or 0) > int(current.get("sequence") or 0) else current


def _hook_dedupe_rows(rows: list[Json]) -> list[Json]:
    by_identity: dict[tuple[str, str, str], Json] = {}
    for row in rows:
        identity = _hook_row_identity(row)
        existing = by_identity.get(identity)
        by_identity[identity] = row if existing is None else _hook_prefer_row(row, existing)
    return sorted(
        by_identity.values(),
        key=lambda row: (int(row.get("timestamp_ms") or 0), int(row.get("sequence") or 0)),
        reverse=True,
    )


def _hook_collect(reader: _HookStoreReader, prefix: str, args: Json) -> Json:
    top_k = max(1, min(int(args.get("top_k") or 3), 200))
    scan_limit = max(top_k, min(int(args.get("scan_limit") or 500), 5000))
    real_user_only = bool(args.get("real_user_only", True))
    include_synthetic = bool(args.get("include_synthetic", False))
    count_key, record_count = _hook_record_count(reader, prefix)
    rows: list[Json] = []
    if record_count <= 0:
        return {"backend": reader.name, "prefix": prefix, "count_key": count_key, "record_count": record_count, "rows": rows, "status": "empty"}
    first = max(1, record_count - scan_limit + 1)
    for sequence in range(record_count, first - 1, -1):
        candidate_keys = []
        if count_key and ":raw_ingestion:" in count_key:
            candidate_keys.append((f"{prefix}:raw_ingestion:records:{sequence // _CODEX_HOOK_SHARD_SIZE:06d}", f"{sequence:020d}", "raw_ingestion"))
        candidate_keys.extend(
            [
                (f"{prefix}:records:{sequence // _CODEX_HOOK_SHARD_SIZE:06d}", f"{sequence:020d}", "records"),
                (f"{prefix}:records", f"{sequence:020d}", "records-unsharded"),
            ]
        )
        raw = None
        source_key = ""
        source_field = ""
        projection = ""
        for key, field, kind in candidate_keys:
            raw = reader.hget(key, field)
            if raw:
                source_key, source_field, projection = key, field, kind
                break
        record = _hook_parse_record(raw)
        if not record:
            continue
        for role, text_value in _hook_message_candidates(record):
            if not _hook_is_real_user(record, role, text_value, real_user_only=real_user_only, include_synthetic=include_synthetic):
                continue
            inferred_role = _hook_infer_role(record, role, text_value)
            rows.append(
                {
                    "backend": reader.name,
                    "prefix": prefix,
                    "sequence": sequence,
                    "timestamp_ms": _hook_timestamp_ms(record),
                    "session_id": _hook_session_id(record),
                    "role": inferred_role,
                    "text": text_value,
                    "projection": projection,
                    "source_key": source_key,
                    "source_field": source_field,
                    "record_type": record.get("record_type"),
                    "source_kind": record.get("source_kind"),
                    "synthetic": _hook_text_is_synthetic(text_value),
                    "record_keys": sorted(k for k in record.keys() if k not in {"messages", "text", "content", "message"})[:48],
                }
            )
    rows = _hook_dedupe_rows(rows)
    status = "ok" if len(rows) >= top_k else ("partial" if rows else "no_matching_rows")
    return {"backend": reader.name, "prefix": prefix, "count_key": count_key, "record_count": record_count, "rows": rows[:top_k], "status": status}


def query_codex_hook_messages(args: Json) -> Json:
    backend = str(args.get("backend") or "both").strip().lower()
    readers: list[tuple[str, _HookStoreReader]] = []
    errors: list[Json] = []
    if backend in {"both", "native", "native"}:
        try:
            readers.append(("native", _HookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "native", "error": str(exc)})
    if backend in {"both", "rust", "rust-service"}:
        try:
            readers.append(("rust", _RustServiceHookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "rust-service", "error": str(exc)})
    if backend in {"rust-local"}:
        try:
            readers.append(("rust", _RustLocalHookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "rust-local-proxy", "error": str(exc)})
    results: list[Json] = []
    for backend_kind, reader in readers:
        for prefix in _hook_prefixes(args, "native" if backend_kind == "native" else "rust"):
            result = _hook_collect(reader, prefix, args)
            results.append(result)
            if result.get("rows"):
                break
    normalized: dict[str, set[str]] = {}
    for result in results:
        normalized[result["backend"]] = {" ".join(str(row.get("text", "")).lower().split())[:240] for row in result.get("rows", [])}
    comparison: list[Json] = []
    for result in results:
        for row in result.get("rows", []):
            text_key = " ".join(str(row.get("text", "")).lower().split())[:240]
            comparison.append(
                {
                    "backend": row.get("backend"),
                    "sequence": row.get("sequence"),
                    "session_id": row.get("session_id"),
                    "text_preview": str(row.get("text", ""))[:220],
                    "seen_in_other_backend": any(text_key in values for name, values in normalized.items() if name != row.get("backend")),
                }
            )
    return {
        "status": "ok",
        "queried_at_ms": int(time.time() * 1000),
        "top_k": max(1, min(int(args.get("top_k") or 3), 200)),
        "real_user_only": bool(args.get("real_user_only", True)),
        "include_synthetic": bool(args.get("include_synthetic", False)),
        "results": results,
        "comparison": comparison,
        "errors": errors,
    }


def mcp_http_dispatch(server: Any, body: Json, *, api_key: Optional[str] = None) -> Json:
    """Stateless MCP-over-HTTP dispatch (Streamable HTTP transport).

    Pipes a JSON-RPC MCP message through the same server.handle() used by stdio, so MCP runs
    distributed/load-balanced for enterprise reads. Injects the header API key into tools/call
    arguments (multi-tenant), and applies the ingest fast-ack default. Stateless: any replica can
    serve any request. Returns the JSON-RPC response ({"jsonrpc":"2.0"} for notifications).
    """
    if isinstance(body, dict) and body.get("method") == "tools/call" and isinstance(body.get("params"), dict):
        name = body["params"].get("name")
        margs = body["params"].get("arguments")
        if isinstance(margs, dict):
            if api_key:
                margs.setdefault("api_key", api_key)
            if name == "matrixark_ingest":
                margs.setdefault("async_processing", True)
    response = server.handle(body)
    return response if response is not None else {"jsonrpc": "2.0"}


def apply_ingest_route_defaults(path: str, args: Json) -> Json:
    """Enterprise ingest defaults: fast, non-blocking ack for scaled real-time ingestion.

    /api/ingest defaults to async_processing=True (write the ContextEvent / session buffer and
    return; extraction/summaries/embeddings run async). Callers keep control by sending an explicit
    async_processing value. Pure + side-effect-free apart from mutating the passed args dict.
    """
    if path == "/api/ingest" and isinstance(args, dict):
        args.setdefault("async_processing", True)
    return args


def apply_retrieve_budget_alias(args: Json) -> Json:
    """Alias the client-facing ``budget`` knob onto ``max_context_tokens``.

    The documented public retrieve knob is ``budget``, but the gateway and the
    ``matrixark_retrieve`` tool read ``max_context_tokens`` -- so ``budget`` was
    silently ignored. Accept either: ``max_context_tokens`` wins when both are
    supplied, otherwise a non-empty ``budget`` populates it. Pure apart from
    mutating the passed args dict.
    """
    if not isinstance(args, dict):
        return args
    if args.get("max_context_tokens") in (None, ""):
        budget = args.get("budget")
        if budget not in (None, ""):
            args["max_context_tokens"] = budget
    return args


def make_matrixark_http_handler(server: "MatrixArkMcpServer", static_root: Path) -> type[SimpleHTTPRequestHandler]:
    static_root = static_root.resolve()

    class MatrixArkHttpHandler(SimpleHTTPRequestHandler):
        server_version = "MatrixArkPortal/0.1"
        cloud_mode = _http_cloud_mode(server)

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(static_root), **kwargs)

        def log_message(self, format: str, *args: Any) -> None:
            _mcp_debug_log("http " + format % args)

        def end_headers(self) -> None:
            if self.cloud_mode:
                self.send_header("Access-Control-Allow-Origin", os.environ.get("MATRIXARK_HTTP_ALLOWED_ORIGIN", "https://app.matrixark.ai"))
            else:
                self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type, X-MatrixArk-API-Key, X-MatrixArk-Trusted-Gateway, X-MatrixArk-Gateway-Verified",
            )
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            super().end_headers()

        def do_OPTIONS(self) -> None:  # noqa: N802 - http.server API
            self.send_response(204)
            self.end_headers()

        def _write_json(self, status: int, payload: Json) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> Json:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            parsed = json.loads(raw.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise MatrixArkError("HTTP JSON body must be an object")
            return parsed

        def _write_auth_required(self, tool_name: str = "http") -> None:
            self._write_json(
                401,
                {
                    "status": "error",
                    "tool": tool_name,
                    "error": "MatrixArk cloud HTTP API requires bearer API key or trusted gateway authentication",
                },
            )

        def _call_tool_route(self, tool_name: str, args: Json) -> None:
            try:
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required(tool_name)
                    return
                _http_api_key(self.headers, args)
                if tool_name == "matrixark_retrieve":
                    apply_retrieve_budget_alias(args)
                result = server.call_tool(tool_name, args)
                self._write_json(200, {"status": "ok", "tool": tool_name, "result": result})
            except Exception as exc:
                # Shed load at the edge under overload -> 429 so the client SDK backs off + retries,
                # rather than a 500. Matched by class name to avoid a hard import dependency.
                if exc.__class__.__name__ == "MatrixArkBackpressureError":
                    self._write_json(429, {"status": "overloaded", "tool": tool_name,
                                           "error": str(exc), "retry_after_ms": 100})
                else:
                    self._write_json(500, {"status": "error", "tool": tool_name, "error": str(exc)})

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            parsed = urlparse(self.path)
            if parsed.path == "/api/health":
                backend = "unknown"
                try:
                    backend = str(server.adapter.backend_metrics().get("backend", "unknown"))
                except Exception:
                    pass
                self._write_json(200, {"status": "ok", "service": "matrixark_portal", "backend": backend})
                return
            if parsed.path == "/api/ready":
                try:
                    ready = adapter_ensure_backend_ready(server.adapter, reason="http_ready", probe=True, timeout_ms=1000)
                    status = "ok" if ready.get("status") == "ready" else "topology_not_ready"
                    self._write_json(200 if status == "ok" else 503, {"status": status, "service": "matrixark_portal", "readiness": ready})
                except Exception as exc:
                    self._write_json(503, {"status": "topology_not_ready", "service": "matrixark_portal", "error": str(exc)})
                return
            if parsed.path in HTTP_TOOL_ROUTES:
                self._call_tool_route(HTTP_TOOL_ROUTES[parsed.path], _http_query_args(parsed))
                return
            if parsed.path == "/api/codex_hook_messages":
                args = _http_query_args(parsed)
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("codex_hook_messages")
                    return
                self._write_json(200, {"status": "ok", "result": query_codex_hook_messages(args)})
                return
            if parsed.path == "/api/tools":
                args: Json = {}
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("tools")
                    return
                self._write_json(200, {"status": "ok", "tools": sorted(set(HTTP_TOOL_ROUTES.values()) | HTTP_SPECIAL_TOOLS)})
                return
            super().do_GET()

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            parsed = urlparse(self.path)
            try:
                body = self._read_json_body()
            except Exception as exc:
                self._write_json(400, {"status": "error", "error": str(exc)})
                return
            if parsed.path == "/api/tools/call":
                tool_name = str(body.get("tool") or body.get("name") or "")
                args = body.get("arguments") or body.get("args") or {}
                if not tool_name or not isinstance(args, dict):
                    self._write_json(400, {"status": "error", "error": "body requires tool and arguments object"})
                    return
                self._call_tool_route(tool_name, args)
                return
            if parsed.path == "/mcp":
                # MCP Streamable HTTP transport (stateless). Pipe a JSON-RPC MCP message
                # (initialize / tools/list / tools/call) through the SAME dispatch as stdio, so MCP
                # runs distributed + load-balanced for enterprise reads instead of per-process stdio.
                # Stateless => any replica can serve any request. Multi-tenant: the header API key is
                # injected into tools/call arguments so the access model applies unchanged.
                if self.cloud_mode and not _http_has_auth(self.headers, body):
                    self._write_auth_required("mcp")
                    return
                _akd: Json = {}
                _http_api_key(self.headers, _akd)
                try:
                    response = mcp_http_dispatch(server, body, api_key=_akd.get("api_key"))
                    self._write_json(200, response)
                except Exception as exc:  # keep MCP errors JSON-RPC shaped
                    self._write_json(200, {"jsonrpc": "2.0", "id": body.get("id") if isinstance(body, dict) else None,
                                           "error": {"code": -32603, "message": str(exc)}})
                return
            if parsed.path == "/api/agent/hook":
                if self.cloud_mode and not _http_has_auth(self.headers, body):
                    self._write_auth_required("agent_hook")
                    return
                _http_api_key(self.headers, body)
                try:
                    result = _agent_hook_call(server, body)
                    self._write_json(200, {"status": "ok", "tool": "matrixark_agent_hook", "result": result})
                except Exception as exc:
                    self._write_json(500, {"status": "error", "tool": "matrixark_agent_hook", "error": str(exc)})
                return
            if parsed.path in HTTP_TOOL_ROUTES:
                args = body.get("arguments") if isinstance(body.get("arguments"), dict) else body
                apply_ingest_route_defaults(parsed.path, args)
                self._call_tool_route(HTTP_TOOL_ROUTES[parsed.path], args)
                return
            if parsed.path == "/api/codex_hook_messages":
                args = body.get("arguments") if isinstance(body.get("arguments"), dict) else body
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("codex_hook_messages")
                    return
                self._write_json(200, {"status": "ok", "result": query_codex_hook_messages(args)})
                return
            self._write_json(404, {"status": "error", "error": f"unknown API path {parsed.path}"})

    return MatrixArkHttpHandler
