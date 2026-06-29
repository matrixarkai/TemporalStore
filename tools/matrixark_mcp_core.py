#!/usr/bin/env python3
"""MatrixArk MCP server for LLM context ingestion and retrieval.

This is intentionally dependency-free. It implements the small JSON-RPC subset
needed by MCP clients over stdio, and keeps the storage boundary behind a local
adapter that can be replaced with TemporalStore RPC calls later.
"""

from __future__ import annotations

import argparse
import secrets
import select
import shutil
import socket
import subprocess
import hashlib
import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import urllib.error
import urllib.request
import math
import os
import re
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

try:
    from tools.matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from tools.matrixark_skill_parser import parse_skill
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import ResourceParserError, content_hash, embedding_text_for_chunk, normalize_parse_warnings, parse_resource, summarize_resource_chunks
    from matrixark_skill_parser import parse_skill


Json = dict[str, Any]


def _mcp_debug_log(message: str) -> None:
    path = os.environ.get("MATRIXARK_MCP_DEBUG_LOG")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{time.time():.3f} {message}\n")
    except Exception:
        pass
MAX_PRIOR_MESSAGES = 8
MAX_PRIOR_CHARS = 4096
EMBEDDING_DIM = 32
DIRECT_RECORD_LOG_SHARD_SIZE = 256
DIRECT_RECORD_BUNDLE_MAX_BYTES = int(os.environ.get("MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES", "65536"))
DIRECT_WRITE_RETRIES = int(os.environ.get("MATRIXARK_DIRECT_WRITE_RETRIES", "3"))
DIRECT_WRITE_BACKOFF_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_BACKOFF_MS", "25"))
DIRECT_WRITE_THROTTLE_MS = int(os.environ.get("MATRIXARK_DIRECT_WRITE_THROTTLE_MS", "0"))
DIRECT_AUDIT_MODE = os.environ.get("MATRIXARK_DIRECT_AUDIT_MODE", "drop").strip().lower()
DIRECT_AUDIT_BUFFER_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS", "128"))
DIRECT_AUDIT_FLUSH_INTERVAL_MS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS", "1000"))
ENABLE_CONTEXT_DEBUG_RECORDS = os.environ.get("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_CONTEXT_REPLAY = os.environ.get("MATRIXARK_ENABLE_REPLAY", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_SUMMARY_REFRESH_AUDIT = os.environ.get("MATRIXARK_SUMMARY_REFRESH_AUDIT", "0").strip().lower() in {"1", "true", "yes"}
SUMMARY_REFRESH_INTERVAL_MS = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_INTERVAL_MS", "1000"))
SUMMARY_REFRESH_LIMIT = int(os.environ.get("MATRIXARK_SUMMARY_REFRESH_LIMIT", "64"))
BACKEND_READINESS_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_TIMEOUT_MS", "30000"))
BACKEND_READINESS_BACKOFF_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_BACKOFF_MS", "200"))
MATRIXARK_MCP_PROFILE = os.environ.get("MATRIXARK_MCP_PROFILE", "dev").strip().lower()
MATRIXARK_ALLOW_LOCAL_BACKEND = os.environ.get("MATRIXARK_ALLOW_LOCAL_BACKEND", "0").strip().lower() in {"1", "true", "yes"}
MATRIXARK_REQUIRE_BACKEND_READY = os.environ.get("MATRIXARK_REQUIRE_BACKEND_READY", "").strip().lower()
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))

MAX_SECONDARY_INDEX_TERMS_PER_RECORD = int(os.environ.get("MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD", "10"))
MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK", "6"))
MAX_INDEX_TERMS_PER_RESOURCE_CHUNK = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_CHUNK", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
MAX_INDEX_TERMS_PER_RESOURCE_FACT = int(os.environ.get("MATRIXARK_MAX_INDEX_TERMS_PER_RESOURCE_FACT", str(MAX_SECONDARY_INDEX_TERMS_PER_RECORD)))
DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
HARD_MAX_CHILDREN_SCORED_PER_PARENT = int(os.environ.get("MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT", "100000"))
SECONDARY_INDEX_PRIORITY_PREFIXES = (
    "source_type:",
    "resource_type:",
    "unit_kind:",
    "entity_type:",
    "event_type:",
    "classification:",
    "status:",
    "skill_name:",
    "skill_trigger:",
    "skill_tool:",
    "relative_path:",
    "heading_slug:",
    "segment_topic:",
    "keyword:",
)
MAX_RESOURCE_FACT_CHUNKS = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACT_CHUNKS", "8"))
MAX_RESOURCE_FACTS_PER_RESOURCE = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_RESOURCE", "8"))
MAX_RESOURCE_FACTS_PER_CHUNK = int(os.environ.get("MATRIXARK_MAX_RESOURCE_FACTS_PER_CHUNK", "2"))
ENABLE_GENERIC_RESOURCE_FACTS = os.environ.get("MATRIXARK_ENABLE_GENERIC_RESOURCE_FACTS", "0").strip().lower() in {"1", "true", "yes"}
RESOURCE_ASYNC_DEFAULT_BYTES = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_BYTES", str(2 * 1024 * 1024)))
RESOURCE_ASYNC_DEFAULT_TEXT_CHARS = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_TEXT_CHARS", "200000"))
RESOURCE_ASYNC_DEFAULT_PATH_COUNT = int(os.environ.get("MATRIXARK_RESOURCE_ASYNC_DEFAULT_PATH_COUNT", "32"))
MAX_CONTEXT_REF_CHARS = 4096
DEFAULT_TIME_DECAY_TOLERANCE_MS = 24 * 60 * 60 * 1000
DEFAULT_TIME_DECAY_HALFLIFE_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TIME_WEIGHT = 0.18
DEFAULT_BUSINESS_WEIGHT = 0.22
TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE", "256"))
TIME_COMPRESSION_WINDOW_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_WINDOW_EVENTS", "64"))
TIME_COMPRESSION_MIN_EVENTS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENTS", "8"))
TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH", "4"))
TIME_COMPRESSION_MIN_EVENT_AGE_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_MIN_EVENT_AGE_MS", "0"))
TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS = int(os.environ.get("MATRIXARK_TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS", str(30 * 24 * 60 * 60 * 1000)))
TIME_COMPRESSION_SUMMARY_PROVIDER = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_PROVIDER", "deterministic").strip().lower()
TIME_COMPRESSION_SUMMARY_MODEL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
TIME_COMPRESSION_SUMMARY_BASE_URL = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_BASE_URL", os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
TIME_COMPRESSION_SUMMARY_API_KEY_ENV = os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_API_KEY_ENV", "OPENAI_API_KEY")
TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC", "30"))
TIME_COMPRESSION_REQUIRE_LLM_SUMMARY = os.environ.get("MATRIXARK_REQUIRE_LLM_TIME_COMPRESSION", "").strip().lower() in {"1", "true", "yes"}
_OSS_SEGMENT_MODEL_CACHE: dict[str, Any] = {}
_OSS_EMBEDDING_MODEL_CACHE: dict[str, Any] = {}
_OSS_UNDERSTANDING_PROTOTYPE_CACHE: dict[str, dict[str, list[float]]] = {}
_EMBEDDING_VECTOR_CACHE: dict[tuple[str, str], list[float]] = {}
_EMBEDDING_VECTOR_CACHE_LOCK = threading.RLock()
_EMBEDDING_FALLBACK_USED = False
_DIRECT_RECORD_CACHE: dict[str, tuple[int, list[Json]]] = {}
_DIRECT_RECORD_CACHE_LOCK = threading.RLock()
_DIRECT_RECORD_CACHE_MAX_PREFIXES = 64
_DIRECT_RECORD_LOAD_LOCKS: dict[str, threading.RLock] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE: dict[str, Json] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK = threading.RLock()
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES = int(os.environ.get("MATRIXARK_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES", "256"))

DEFAULT_BUSINESS_TYPE_WEIGHTS: Json = {
    "confirmation": 1.0,
    "correction": 1.0,
    "approval_budget": 0.95,
    "approval": 0.95,
    "approval_state": 0.95,
    "budget": 0.9,
    "preference_update": 0.82,
    "plan_update": 0.78,
    "status_update": 0.76,
    "job_status": 0.76,
    "relationship": 0.74,
    "location": 0.7,
    "current_plan": 0.78,
    "family_profile": 0.72,
    "skill": 0.84,
    "resource": 0.68,
    "dialogue_batch": 0.45,
    "session": 0.45,
}
MATRIXARK_ADMIN_SCOPES = {"admin:account", "admin:tenant", "admin:user", "admin:api_key", "admin:sso", "admin:audit", "portal:read"}
MATRIXARK_CONTEXT_SCOPES = {
    "context:ingest",
    "context:retrieve",
    "context:feedback",
    "context:replay",
    "resource:ingest",
    "resource:read",
    "resource:manage",
    "skill:read",
    "skill:manage",
}
MATRIXARK_ALL_SCOPES = MATRIXARK_CONTEXT_SCOPES | MATRIXARK_ADMIN_SCOPES
MATRIXARK_TOOL_SCOPES: dict[str, set[str]] = {
    "matrixark_ingest": {"context:ingest"},
    "matrixark_batch_extract": {"context:ingest"},
    "matrixark_session_commit": {"context:ingest"},
    "matrixark_refresh_summaries": {"context:ingest"},
    "matrixark_retrieve": {"context:retrieve"},
    "matrixark_ingestion_dashboard": {"context:replay"},
    "matrixark_management_portal": {"portal:read"},
    "matrixark_auth_sso_login": set(),
    "matrixark_list_resources": {"resource:read"},
    "matrixark_list_skills": {"skill:read"},
    "matrixark_update_skill": {"skill:manage"},
    "matrixark_feedback": {"context:feedback"},
    "matrixark_replay": {"context:replay"},
    "matrixark_admin_create_account": {"admin:account"},
    "matrixark_admin_update_account": {"admin:account"},
    "matrixark_admin_list_accounts": {"admin:account"},
    "matrixark_admin_create_user": {"admin:user"},
    "matrixark_admin_update_user": {"admin:user"},
    "matrixark_admin_list_users": {"admin:user"},
    "matrixark_admin_create_api_key": {"admin:api_key"},
    "matrixark_admin_apply_api_key": {"admin:api_key", "admin:account", "admin:user"},
    "matrixark_admin_list_api_keys": {"admin:api_key"},
    "matrixark_admin_rotate_api_key": {"admin:api_key"},
    "matrixark_admin_revoke_api_key": {"admin:api_key"},
    "matrixark_admin_map_sso_user": {"admin:sso"},
    "matrixark_admin_audit": {"admin:audit"},
    "matrixark_backend_ready": set(),
    "matrixark_backend_metrics": set(),
}

MATRIXARK_ROLE_SCOPE_LIMITS: dict[str, set[str] | None] = {
    "owner": None,
    "admin": None,
    "operator": {
        "portal:read",
        "admin:audit",
        "context:ingest",
        "context:retrieve",
        "context:feedback",
        "context:replay",
        "resource:ingest",
        "resource:read",
        "resource:manage",
        "skill:read",
        "skill:manage",
    },
    "developer": {
        "portal:read",
        "context:ingest",
        "context:retrieve",
        "context:feedback",
        "context:replay",
        "resource:ingest",
        "resource:read",
        "skill:read",
    },
    "viewer": {"portal:read", "context:retrieve", "context:replay", "resource:read", "skill:read"},
    # Scoped service keys are capability-limited by their explicit scopes and
    # optional user/session allow-lists. They may be used by Codex, Claude,
    # Cursor, CI, or backend agents without forcing a human role name.
    "service": None,
    "local_agent": None,
    "dev_admin": None,
}


def normalize_matrixark_role(role: str) -> str:
    normalized = safe_identifier(role or "service", default="service")
    aliases = {"administrator": "admin", "tenant_admin": "admin", "portal_user": "developer", "read_only": "viewer", "readonly": "viewer", "agent": "service", "agent_service": "service"}
    return aliases.get(normalized, normalized)


def role_allows_scopes(role: str, scopes: set[str]) -> bool:
    normalized = normalize_matrixark_role(role)
    if normalized not in MATRIXARK_ROLE_SCOPE_LIMITS:
        return False
    limits = MATRIXARK_ROLE_SCOPE_LIMITS[normalized]
    return limits is None or scopes.issubset(limits)


def stable_hash(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_api_key(prefix: str = "mk_test") -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def now_ms() -> int:
    return int(time.time() * 1000)


def json_text(value: Json) -> Json:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(value, sort_keys=True),
            }
        ]
    }


class MatrixArkError(ValueError):
    pass


def is_retryable_temporalstore_error(error: Any) -> bool:
    text = str(error).lower()
    retryable_fragments = (
        "slot not found",
        "partition info not found",
        "partition no primary",
        "no primary",
        "not ready",
        "unavailable",
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "temporarily unavailable",
        "server is busy",
    )
    return any(fragment in text for fragment in retryable_fragments)


def parse_host_port(address: str) -> tuple[str, int] | None:
    if not address or ":" not in address:
        return None
    host, port_text = address.rsplit(":", 1)
    try:
        return host or "127.0.0.1", int(port_text)
    except ValueError:
        return None


def metaserver_reachable(address: str, timeout_ms: int = BACKEND_READINESS_CONNECT_TIMEOUT_MS) -> Json:
    parsed = parse_host_port(address)
    if parsed is None:
        return {"ok": False, "address": address, "error": "invalid metaserver address"}
    host, port = parsed
    try:
        with socket.create_connection((host, port), timeout=max(0.05, timeout_ms / 1000.0)):
            return {"ok": True, "address": address}
    except OSError as exc:
        return {"ok": False, "address": address, "error": str(exc)}


def require_string(data: Json, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise MatrixArkError(f"{field} must be a non-empty string")
    return value


def require_messages(data: Json) -> list[Json]:
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        raise MatrixArkError("messages must be a non-empty list")
    for message in messages:
        if not isinstance(message, dict):
            raise MatrixArkError("messages entries must be objects")
        role = message.get("role")
        content = message.get("content")
        if role not in {"user", "assistant", "tool", "system"}:
            raise MatrixArkError("message role must be user, assistant, tool, or system")
        if not isinstance(content, str) or not content:
            raise MatrixArkError("message content must be a non-empty string")
    return messages


def optional_object(data: Json, field: str) -> Json:
    value = data.get(field, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise MatrixArkError(f"{field} must be an object")
    return value


def optional_string(data: Json, field: str, default: str = "") -> str:
    value = data.get(field, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise MatrixArkError(f"{field} must be a string")
    return value


def optional_string_list(data: Json, field: str, default: list[str] | None = None) -> list[str]:
    value = data.get(field, default or [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise MatrixArkError(f"{field} must be a list of strings")
    return list(value)


def safe_identifier(value: str, *, default: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()).strip("._-").lower()
    return compact or default


def local_account_user_id() -> str:
    return safe_identifier(
        os.environ.get("MATRIXARK_LOCAL_USER_ID")
        or os.environ.get("USERNAME")
        or os.environ.get("USER")
        or "local_user",
        default="local_user",
    )


def local_agent_name(args: Json, scope: Json) -> str:
    hook = args.get("agent_hook") if isinstance(args.get("agent_hook"), dict) else {}
    metadata = args.get("metadata") if isinstance(args.get("metadata"), dict) else {}
    messages = args.get("messages") if isinstance(args.get("messages"), list) else []
    first_named_message = ""
    for message in messages:
        if isinstance(message, dict) and isinstance(message.get("name"), str) and message.get("name"):
            first_named_message = str(message["name"])
            break
    return safe_identifier(
        str(
            args.get("agent_name")
            or scope.get("agent_name")
            or scope.get("agent")
            or hook.get("source")
            or metadata.get("source")
            or first_named_message
            or os.environ.get("MATRIXARK_LOCAL_AGENT_NAME")
            or "local_agent"
        ),
        default="local_agent",
    )


def local_identity_defaults(args: Json, scope: Json) -> Json:
    agent_name = local_agent_name(args, scope)
    account_id = canonical_account_id(str(scope.get("account_id") or os.environ.get("MATRIXARK_LOCAL_ACCOUNT_ID") or "acct_local"))
    tenant_id = canonical_tenant_id(
        str(scope.get("tenant_id") or os.environ.get("MATRIXARK_LOCAL_TENANT_ID") or f"tenant_{agent_name}")
    )
    user_id = str(scope.get("user_id") or os.environ.get("MATRIXARK_LOCAL_USER_ID") or local_account_user_id())
    session_id = str(scope.get("session_id") or "")
    return {
        "account_id": account_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "agent_name": agent_name,
    }


def canonical_account_id(value: str) -> str:
    return value or "acct_local"


def canonical_tenant_id(value: str) -> str:
    return value or "tenant_local_agent"


def identity_hashes(account_id: str, tenant_id: str, user_id: str = "", session_id: str = "") -> Json:
    tenant_hash = stable_hash(f"{account_id}:{tenant_id}")
    user_hash = stable_hash(f"{tenant_hash}:user:{user_id}") if user_id else 0
    session_hash = stable_hash(f"{tenant_hash}:session:{session_id}") if session_id else 0
    return {
        "tenant_hash": tenant_hash,
        "user_hash": user_hash,
        "session_hash": session_hash,
        "scope_key": scope_key_from_hashes(tenant_hash, user_hash, session_hash),
    }


def scope_key_from_hashes(tenant_hash: int, user_hash: int = 0, session_hash: int = 0) -> str:
    parts = [f"t={int(tenant_hash)}"]
    if user_hash:
        parts.append(f"u={int(user_hash)}")
    if session_hash:
        parts.append(f"s={int(session_hash)}")
    return "|".join(parts) + "|"


def scope_key_prefix_for_query(query_scope: Json) -> str:
    explicit_keys = set(query_scope.get("_explicit_scope_keys", []))
    tenant_hash = int(query_scope.get("tenant_hash") or 0)
    if not tenant_hash:
        return ""
    user_hash = int(query_scope.get("user_hash") or 0) if "user_id" in explicit_keys else 0
    session_hash = int(query_scope.get("session_hash") or 0) if "session_id" in explicit_keys else 0
    return scope_key_from_hashes(tenant_hash, user_hash, session_hash)


def parse_scope_key(scope_key: str) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for part in str(scope_key or "").split("|"):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        try:
            parsed[key] = int(value)
        except ValueError:
            continue
    return parsed


def scope_key_matches_query(record_scope_key: str, query_scope: Json, explicit_keys: set[str]) -> bool:
    record_parts = parse_scope_key(record_scope_key)
    tenant_hash = int(query_scope.get("tenant_hash") or 0)
    if tenant_hash and record_parts.get("t") != tenant_hash:
        return False
    if "user_id" in explicit_keys:
        user_hash = int(query_scope.get("user_hash") or 0)
        if user_hash and record_parts.get("u") != user_hash:
            return False
    if "session_id" in explicit_keys:
        session_hash = int(query_scope.get("session_hash") or 0)
        if session_hash and record_parts.get("s") != session_hash:
            return False
    return True


def canonical_scope_key(scope: Json) -> str:
    scope_key = str(scope.get("scope_key") or "")
    if scope_key:
        return scope_key
    tenant_hash = int(scope.get("tenant_hash") or 0)
    if not tenant_hash:
        return ""
    return scope_key_from_hashes(
        tenant_hash,
        int(scope.get("user_hash") or 0),
        int(scope.get("session_hash") or 0),
    )


def serving_scope_ref(scope: Json) -> Json:
    key = canonical_scope_key(scope)
    return {"scope_key": key} if key else {}


def scope_from_serving_record(record: Json) -> Json:
    scope = record.get("scope")
    if isinstance(scope, dict) and scope:
        return scope
    scope_key = str(record.get("scope_key") or "")
    return {"scope_key": scope_key} if scope_key else {}


def node_id_ref(node_hash: int) -> Json:
    return {"node_hash": node_hash, "node_id": node_hash}


HOT_SERVING_RECORD_TYPES = {
    "context_event",
    "context_entity",
    "context_segment",
    "resource_chunk",
    "skill_section",
    "context_index",
    "context_embedding",
}
NODE_PATH_HEAVY_RECORD_TYPES = {
    "context_event",
    "context_entity",
    "context_segment",
    "resource_chunk",
    "skill_section",
    "context_index",
}
EVENT_DEBUG_FIELDS = {"envelope", "internal_extraction", "prior_context", "agent_hook", "storage_options", "summary_embedding"}
ENTITY_DEBUG_FIELDS = {"previous_state", "field_patches", "patch_results"}


def _record_debug_ref(record: Json) -> tuple[str, Any]:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_event":
        return "event", record.get("event_id_hash")
    if record_type == "context_entity":
        return "entity", record.get("entity_hash")
    if record_type == "context_segment":
        return "segment", record.get("segment_hash")
    if record_type == "resource_chunk":
        return "resource_chunk", record.get("chunk_hash")
    if record_type == "skill_section":
        return "skill_section", record.get("section_hash")
    return record_type, record.get("ref_hash")


def attach_storage_route(record: Json) -> Json:
    route_source = record.get("storage_options") if isinstance(record.get("storage_options"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    if not route_source and isinstance(envelope.get("storage_options"), dict):
        route_source = envelope.get("storage_options", {})
    if "storage_route" not in record or not isinstance(record.get("storage_route"), dict):
        if route_source:
            record = {**record, "storage_route": canonical_storage_route(route_source)}
    return record


def materialize_serving_records(record: Json) -> list[Json]:
    """Split bulky provider/debug fields from hot serving records.

    Serving records are optimized for retrieval scans and packing. Replay/debug
    rows keep provider payloads, raw extraction details, old entity patches, and
    full path context without forcing every hot read to load them.
    """
    record = attach_storage_route(record)
    record_type = str(record.get("record_type") or "")
    if record_type not in HOT_SERVING_RECORD_TYPES:
        return [record]

    serving = dict(record)
    envelope = serving.get("envelope") if isinstance(serving.get("envelope"), dict) else {}
    existing_scope_key = str(serving.get("scope_key") or "")
    scope = serving.get("scope") if isinstance(serving.get("scope"), dict) else envelope.get("scope", {})
    scope_key = canonical_scope_key(scope) if isinstance(scope, dict) and scope else existing_scope_key
    if scope_key:
        serving["scope_key"] = scope_key
    serving.pop("scope", None)

    node_hash = serving.get("node_hash")
    if node_hash is not None:
        serving.setdefault("node_id", node_hash)
    if record_type in NODE_PATH_HEAVY_RECORD_TYPES:
        serving.pop("node_path", None)

    debug_payload: Json = {}
    debug_type = ""
    if record_type == "context_event":
        extraction = serving.get("internal_extraction") if isinstance(serving.get("internal_extraction"), dict) else {}
        classification = non_default_classification(extraction.get("classification", serving.get("classification", "")))
        if classification:
            serving["classification"] = classification
        else:
            serving.pop("classification", None)
        serving["event_type"] = extraction.get("event_type", serving.get("event_type", ""))
        serving["status"] = extraction.get("status", serving.get("status", "observed"))
        serving["source_kind"] = envelope.get("kind", serving.get("source_kind", "message")) if isinstance(envelope, dict) else serving.get("source_kind", "message")
        debug_payload = {field: record[field] for field in EVENT_DEBUG_FIELDS if field in record and record[field] not in (None, "", [], {})}
        debug_type = "event_extraction_detail"
        for field in EVENT_DEBUG_FIELDS:
            serving.pop(field, None)
    elif record_type == "context_entity":
        debug_payload = {field: record[field] for field in ENTITY_DEBUG_FIELDS if field in record and record[field] not in (None, "", [], {})}
        debug_type = "entity_update_detail"
        for field in ENTITY_DEBUG_FIELDS:
            serving.pop(field, None)

    if not debug_payload or not ENABLE_CONTEXT_DEBUG_RECORDS:
        return [serving]

    ref_type, ref_hash = _record_debug_ref(record)
    debug_record: Json = {
        "record_type": "context_debug_record",
        "debug_type": debug_type,
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": record.get("node_hash"),
        "node_id": record.get("node_hash"),
        "node_path": record.get("node_path", []),
        "scope_key": scope_key,
        "debug_payload": debug_payload,
        "updated_at_ms": record.get("updated_at_ms") or (envelope.get("ingestion_time_ms") if isinstance(envelope, dict) else now_ms()),
    }
    return [debug_record, serving]


def materialize_serving_record_batch(records: list[Json]) -> list[Json]:
    materialized: list[Json] = []
    for record in records:
        materialized.extend(materialize_serving_records(record))
    return materialized


def enrich_scope_with_identity(scope: Json, identity: Json) -> Json:
    account_id = str(identity["account_id"])
    tenant_id = str(identity["tenant_id"])
    user_id = str(scope.get("user_id") or identity.get("user_id") or "")
    session_id = str(scope.get("session_id") or identity.get("session_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id)
    explicit_scope_keys = {str(key) for key in scope.keys()}
    if identity.get("mode") == "api_key":
        if user_id:
            explicit_scope_keys.add("user_id")
        if session_id:
            explicit_scope_keys.add("session_id")
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if identity.get("agent_name") and "agent_name" not in enriched:
        enriched["agent_name"] = identity["agent_name"]
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    return enriched


def validate_hook(hook: Json | None) -> Json | None:
    if hook is None:
        return None
    if not isinstance(hook, dict):
        raise MatrixArkError("agent_hook must be an object")
    hook_type = require_string(hook, "hook_type")
    if hook_type not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise MatrixArkError("agent_hook.hook_type is invalid")
    require_string(hook, "source")
    require_string(hook, "hook_id")
    if not isinstance(hook.get("observed_at_ms"), int):
        raise MatrixArkError("agent_hook.observed_at_ms must be an integer")
    if not isinstance(hook.get("auto_captured"), bool):
        raise MatrixArkError("agent_hook.auto_captured must be a boolean")
    if "idempotency_key" in hook and not isinstance(hook["idempotency_key"], str):
        raise MatrixArkError("agent_hook.idempotency_key must be a string")
    return hook


def adapter_ensure_backend_ready(adapter: Any, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
    """Call adapter readiness across old/new adapter signatures."""
    try:
        return adapter.ensure_backend_ready(reason=reason, probe=probe, timeout_ms=timeout_ms)
    except TypeError as exc:
        text = str(exc)
        if "unexpected keyword argument" not in text or "probe" not in text:
            raise
        return adapter.ensure_backend_ready(reason=reason)

def has_confirmation_context(envelope: Json) -> bool:
    metadata = optional_object(envelope, "metadata")
    return bool(
        envelope.get("context_pack_id")
        or metadata.get("reply_to_context_pack_id")
        or envelope.get("accepted_refs")
        or envelope.get("rejected_refs")
    )


def explicit_context_pack_id(envelope: Json) -> str:
    metadata = optional_object(envelope, "metadata")
    value = envelope.get("context_pack_id") or metadata.get("reply_to_context_pack_id") or ""
    return str(value) if value else ""


def session_key(envelope: Json) -> tuple[Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
    )


def user_key(envelope: Json) -> tuple[Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("team", ""),
    )


def context_node_key(envelope: Json) -> tuple[Any, Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
        scope.get("project", ""),
    )


def session_buffer_key_from_scope(scope: Json) -> tuple[str, str, str, str]:
    user_id = str(scope.get("user_id") or "")
    session_id = str(scope.get("session_id") or user_id or "")
    return (
        str(scope.get("account_id") or "acct_local"),
        str(scope.get("tenant_id") or "tenant_local_agent"),
        user_id,
        session_id,
    )


def session_buffer_key(envelope: Json) -> tuple[str, str, str, str]:
    return session_buffer_key_from_scope(envelope.get("scope", {}))


def message_from_event_record(record: Json) -> Json | None:
    messages = record.get("envelope", {}).get("messages", [])
    if isinstance(messages, list) and messages:
        message = messages[0]
        if isinstance(message, dict) and "role" in message and "content" in message:
            return dict(message)
    text = str(record.get("text", ""))
    if ":" in text:
        role, content = text.split(":", 1)
        role = role.strip() or "user"
        return {"role": role if role in {"user", "assistant", "tool", "system"} else "user", "content": content.strip()}
    return None


def session_summary_for_events(level: str, event_records: list[Json], all_records: list[Json]) -> Json | None:
    if level != "session" or not event_records:
        return None
    target_key = context_node_key(event_records[0].get("envelope", {}))
    event_hashes = {record.get("event_id_hash") for record in event_records if record.get("event_id_hash") is not None}
    fallback_summary = None
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary" or record.get("summary_type") != "session_l0":
            continue
        if target_key and tuple(record.get("context_node_key", [])) == target_key:
            return record
        if record.get("source_event_hash") in event_hashes and fallback_summary is None:
            fallback_summary = record
    return fallback_summary


def collect_prior_context(envelope: Json, records: list[Json]) -> Json:
    pack_id = explicit_context_pack_id(envelope)
    if pack_id:
        for record in reversed(records):
            if record.get("record_type") == "context_pack_audit" and str(record.get("context_pack_id")) == pack_id:
                summary_text = str(record.get("summary_text", ""))
                refs = record.get("selected_refs", [])
                return {
                    "level": "explicit",
                    "refs": refs,
                    "messages": [],
                    "summaries": [
                        {
                            "ref_type": "context_pack",
                            "ref_hash": pack_id,
                            "text": summary_text[:MAX_PRIOR_CHARS],
                        }
                    ]
                    if summary_text
                    else [],
                    "char_count": min(len(summary_text), MAX_PRIOR_CHARS),
                    "limit": MAX_PRIOR_MESSAGES,
                }

    if has_confirmation_context(envelope):
        return {
            "level": "explicit",
            "refs": envelope.get("accepted_refs") or envelope.get("rejected_refs") or [],
            "messages": [],
            "summaries": [],
            "char_count": 0,
            "limit": MAX_PRIOR_MESSAGES,
        }

    selected_events = []
    key = session_key(envelope)
    if key[1]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if session_key(prior_envelope) == key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("session", selected_events, records)

    selected_events = []
    fallback_key = user_key(envelope)
    if fallback_key[0]:
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            prior_envelope = record.get("envelope", {})
            if user_key(prior_envelope) == fallback_key or scope_matches(scope_from_serving_record(record), envelope.get("scope", {})):
                selected_events.append(record)
                if len(selected_events) >= MAX_PRIOR_MESSAGES:
                    break
        if selected_events:
            return prior_context_payload("user", selected_events, records)

    return {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}


def prior_context_payload(level: str, event_records: list[Json], all_records: list[Json]) -> Json:
    messages = []
    refs = []
    summaries = []
    char_count = 0
    session_summary = session_summary_for_events(level, event_records, all_records)
    if session_summary:
        text = str(session_summary.get("summary_text", ""))[:MAX_PRIOR_CHARS]
        char_count += len(text)
        summaries.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
                "text": text,
            }
        )
        refs.append(
            {
                "ref_type": "session_summary",
                "ref_hash": session_summary.get("summary_hash"),
                "node_hash": session_summary.get("node_hash"),
            }
        )

    event_hashes = {record.get("event_id_hash") for record in event_records}
    seen_summaries = set()
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary":
            continue
        if record.get("source_event_hash") not in event_hashes:
            continue
        summary_hash = record.get("summary_hash") or record.get("node_hash")
        node_hash = record.get("node_hash")
        if summary_hash in seen_summaries:
            continue
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        text = str(record.get("summary_text", ""))[:remaining]
        char_count += len(text)
        seen_summaries.add(summary_hash)
        summaries.append(
            {
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "text": text,
            }
        )
        refs.append({"ref_type": "summary", "ref_hash": summary_hash, "node_hash": node_hash})

    for record in event_records:
        text = str(record.get("text", ""))
        remaining = MAX_PRIOR_CHARS - char_count
        if remaining <= 0:
            break
        clipped = text[:remaining]
        char_count += len(clipped)
        messages.append(
            {
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
                "text": clipped,
            }
        )
        refs.append(
            {
                "ref_type": "event",
                "ref_hash": record.get("event_id_hash"),
                "node_hash": record.get("node_hash"),
            }
        )
    return {
        "level": level,
        "refs": refs,
        "messages": messages,
        "summaries": summaries,
        "char_count": char_count,
        "limit": MAX_PRIOR_MESSAGES,
    }


def compact_internal_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Rules-first internal extraction used by the local MCP MVP.

    Production MatrixArk can replace this with OSS/OpenAI/provider extraction,
    but callers still see the same Mem0-style envelope contract.
    """

    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return oss_encoder_compact_extraction(envelope, prior_context=prior_context)

    text = text_from_messages(envelope["messages"]).lower()
    if envelope["kind"] == "feedback":
        positive = any(term in text for term in ["yes", "confirmed", "approved", "correct", "looks good"])
        negative = any(term in text for term in ["no", "wrong", "incorrect", "reject", "not correct"])
        prior_level = prior_context.get("level", "")
        if not prior_level:
            return {
                "mode": "matrixark_internal",
                "classification": "AMBIGUOUS",
                "quality_warning": "short feedback lacks prior context",
                "prior_refs": [],
            }
        warning = ""
        if prior_level == "user":
            warning = "session_id missing; used user_id fallback for prior context"
        prior_refs = prior_context.get("refs", [])
        if positive:
            return {
                "mode": "matrixark_internal",
                "classification": "CONFIRMATION",
                "status": "accepted",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        if negative:
            return {
                "mode": "matrixark_internal",
                "classification": "CORRECTION",
                "status": "rejected",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        return {
            "mode": "matrixark_internal",
            "classification": "FEEDBACK",
            "status": "observed",
            "prior_context": prior_level,
            "prior_refs": prior_refs,
            "prior_message_count": len(prior_context.get("messages", [])),
            "prior_summary_count": len(prior_context.get("summaries", [])),
            "quality_warning": warning,
        }
    return {
        "mode": "matrixark_internal",
        "classification": "NEW_EVENT",
        "status": "observed",
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "",
    }


ONE_PASS_MEMORY_SCHEMA: Json = {
    "version": "matrixark-one-pass-memory-v1",
    "input": "logical session batch",
    "outputs": [
        "ContextEvent",
        "ContextEntity",
        "ContextSummary",
        "ContextIndex",
        "stale_blocker",
        "EntityPatch",
        "MemorySegment",
        "extraction_audit",
    ],
    "entity_types": [
        "preference",
        "relationship",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    ],
    "segmentation": {
        "phase_1": "semantic_saliency_filtering",
        "phase_2": "event_centric_partitioning",
        "output": "topic plus coordinate tuples over message indexes",
    },
}


def entity_patch(search: str, replace: str, *, field: str = "state") -> Json:
    return {
        "field": field,
        "patch": f"<< SEARCH\n{search}\n====\n{replace}\n>> REPLACE",
    }


def parse_entity_patch(patch_text: str) -> tuple[str, str] | None:
    match = re.search(
        r"<<\s*SEARCH\s*\n(?P<search>.*?)\n====\s*\n(?P<replace>.*?)\n>>\s*REPLACE",
        patch_text,
        flags=re.DOTALL,
    )
    if not match:
        return None
    return match.group("search").strip(), match.group("replace").strip()


def edit_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        for j, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            current.append(
                min(
                    current[j - 1] + 1,
                    previous[j] + 1,
                    previous[j - 1] + cost,
                )
            )
        previous = current
    return previous[-1]


def best_span_by_edit_distance(text: str, search: str) -> tuple[int, int, float]:
    if not text or not search:
        return -1, -1, 1.0
    lower_text = text.lower()
    lower_search = search.lower()
    exact = lower_text.find(lower_search)
    if exact >= 0:
        return exact, exact + len(search), 0.0
    search_len = len(search)
    best = (-1, -1, 1.0)
    min_len = max(1, int(search_len * 0.7))
    max_len = min(len(text), max(search_len + 8, int(search_len * 1.3)))
    step = max(1, search_len // 8)
    for start in range(0, len(text), step):
        for span_len in range(min_len, max_len + 1, step):
            end = min(len(text), start + span_len)
            if end <= start:
                continue
            candidate = text[start:end]
            distance = edit_distance(candidate.lower(), lower_search)
            ratio = distance / max(len(candidate), len(search), 1)
            if ratio < best[2]:
                best = (start, end, ratio)
    return best


def apply_entity_patch(old_value: str, patch_text: str, *, max_distance_ratio: float = 0.45) -> Json:
    parsed = parse_entity_patch(patch_text)
    if not parsed:
        return {
            "updated": old_value,
            "applied": False,
            "reason": "invalid_patch",
            "distance_ratio": 1.0,
        }
    search, replace = parsed
    if not old_value:
        return {
            "updated": replace,
            "applied": True,
            "reason": "empty_old_value",
            "distance_ratio": 0.0,
        }
    start, end, ratio = best_span_by_edit_distance(old_value, search)
    if start < 0 or ratio > max_distance_ratio:
        return {
            "updated": replace,
            "applied": False,
            "reason": "no_close_span",
            "distance_ratio": round(ratio, 6),
        }
    return {
        "updated": old_value[:start] + replace + old_value[end:],
        "applied": True,
        "reason": "approximate_patch",
        "distance_ratio": round(ratio, 6),
        "span": [start, end],
    }


def apply_entity_patches(old_entity: Json | None, extracted_entity: Json) -> Json:
    state = str((old_entity or {}).get("state", ""))
    patches = extracted_entity.get("field_patches", [])
    patch_results = []
    updated_state = state or str(extracted_entity.get("state", ""))
    for patch in patches:
        if not isinstance(patch, dict) or patch.get("field", "state") != "state":
            continue
        result = apply_entity_patch(updated_state, str(patch.get("patch", "")))
        updated_state = str(result.get("updated", updated_state))
        patch_results.append(result)
    if not patches and not state:
        updated_state = str(extracted_entity.get("state", ""))
    elif not patches and state:
        new_state = str(extracted_entity.get("state", ""))
        if new_state and new_state.lower() not in state.lower():
            updated_state = summarize_text(state + " " + new_state, limit=320)
    return {
        **extracted_entity,
        "state": summarize_text(updated_state, limit=320),
        "previous_state": state,
        "patch_results": patch_results,
        "update_mode": "deterministic_eua" if patches else "merge_without_patch",
    }


def one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Extract events, entities, summaries, and indexes from one batch pass.

    This mirrors the VikingMem one-pass idea: compile the desired memory outputs
    into one schema and process the input session once. The local MVP uses
    deterministic rules, while a production provider can replace this function
    with one GPT-4o-mini/OSS call that emits the same JSON shape.
    """

    provider = understanding_provider(envelope)
    messages = envelope["messages"]
    batch_text = text_from_messages(messages)
    batch_terms = tokens(batch_text)
    segments, segment_provider_meta = detect_memory_segments(messages, envelope)
    if provider == "oss_encoder":
        entities = oss_encoder_extract_batch_entities(messages, envelope)
        event_type = oss_encoder_event_type(batch_text)
    else:
        entities = extract_batch_entities(messages, envelope)
        event_type = infer_event_type(batch_text)
    classification = "BATCH_MEMORY"
    if any(entity["entity_type"] == "confirmation" for entity in entities):
        classification = "CONFIRMATION"
    elif any(entity["entity_type"] == "correction" for entity in entities):
        classification = "CORRECTION"
    indexes = ordered_unique(
        [
            context_index_name("event_type", event_type),
            context_index_name("classification", classification),
            context_index_name("status", "observed"),
            context_index_name("source_type", envelope.get("kind", "message")),
        ]
        + [context_index_name("entity_type", entity["entity_type"]) for entity in entities]
        + [context_index_name("segment_topic", segment["topic"]) for segment in segments]
    )
    return {
        "mode": "matrixark_one_pass_schema_oss_encoder" if provider == "oss_encoder" else "matrixark_one_pass_schema",
        "understanding_provider": provider,
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": "observed",
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": segment_provider_meta,
        "indexes": indexes[:8],
        "batch_summary": summarize_text(batch_text, limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(batch_terms),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }



def detect_memory_segments(messages: list[Json], envelope: Json | None = None) -> tuple[list[Json], Json]:
    envelope = envelope or {}
    provider = str(envelope.get("segment_provider") or os.getenv("MATRIXARK_SEGMENT_PROVIDER", "deterministic")).strip().lower()
    if provider in {"oss_encoder", "oss-encoder", "embedding"}:
        segments = oss_encoder_memory_segments(messages)
        return segments, {
            "provider": "oss_encoder",
            "execution_mode": "oss_embedding_model",
            "model": embedding_model_name(),
            "fallback_used": False,
            "segment_count": len(segments),
        }
    if provider in {"", "deterministic", "rules", "local"}:
        if require_oss_understanding():
            raise MatrixArkError("deterministic segmentation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
        return intelligent_memory_segments(messages), {
            "provider": "deterministic",
            "execution_mode": "rules",
            "model": "matrixark-local-segmentation-v1",
            "fallback_used": False,
        }

    fallback_enabled = bool(envelope.get("segment_provider_fallback", False)) or provider in {"oss-fallback", "oss_with_fallback"} or os.getenv("MATRIXARK_SEGMENT_PROVIDER_FALLBACK", "").lower() in {"1", "true", "yes"}
    if provider in {"oss", "oss-fallback", "oss_with_fallback"}:
        model = str(envelope.get("segment_model") or os.getenv("MATRIXARK_SEGMENT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"))
        model_path = str(envelope.get("segment_model_path") or os.getenv("MATRIXARK_SEGMENT_MODEL_PATH", ""))
        max_new_tokens = int(envelope.get("segment_max_new_tokens") or os.getenv("MATRIXARK_SEGMENT_MAX_NEW_TOKENS", "512"))
        try:
            raw = oss_model_memory_segments(
                messages,
                model=model,
                model_path=model_path,
                max_new_tokens=max_new_tokens,
                local_only=fallback_enabled,
            )
            segments = normalize_model_segments(raw, messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "oss_model",
                "model": model_path or model,
                "fallback_used": False,
                "segment_count": len(segments),
            }
        except Exception as exc:  # pragma: no cover - optional local model stack.
            if not fallback_enabled:
                raise MatrixArkError(f"OSS segment provider failed: {exc}") from exc
            segments = intelligent_memory_segments(messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "rules_fallback",
                "model": model_path or model,
                "fallback_used": True,
                "fallback_reason": str(exc),
                "segment_count": len(segments),
            }
    raise MatrixArkError("segment_provider must be deterministic, oss, or oss-fallback")


def build_segment_prompt(messages: list[Json]) -> str:
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    return (
        "You are MatrixArk's memory segmentation extractor. Identify high-saliency memory segments from the indexed conversation. "
        "Prune greetings, acknowledgements, and filler. Merge semantically related non-contiguous messages into the same segment. "
        "Return only valid JSON with this shape: "
        '{"segments":[{"topic":"short_snake_case","coordinate_tuples":[[start,end]],"message_indexes":[0],"saliency_score":0.0,"summary_text":"short summary"}]} '
        "Indexes are zero-based and coordinate end is inclusive. Do not include messages that are only filler.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )


def parse_first_json_object(text: str) -> Json:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise MatrixArkError("model response did not contain a JSON object")


def oss_model_memory_segments(messages: list[Json], *, model: str, model_path: str = "", max_new_tokens: int = 512, local_only: bool = False) -> Json:
    try:
        import torch  # type: ignore
        from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional OSS stack.
        raise MatrixArkError("torch and transformers are required for segment_provider=oss") from exc

    target = model_path or model
    cache_key = f"{target}:{max_new_tokens}"
    cached = _OSS_SEGMENT_MODEL_CACHE.get(cache_key)
    if cached is None:
        local_only = bool(local_only) or bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").lower() in {"1", "true", "yes"}
        tokenizer = AutoTokenizer.from_pretrained(target, local_files_only=local_only)
        model_obj = AutoModelForCausalLM.from_pretrained(target, local_files_only=local_only)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model_obj.to(device)
        model_obj.eval()
        cached = {"tokenizer": tokenizer, "model": model_obj, "device": device}
        _OSS_SEGMENT_MODEL_CACHE[cache_key] = cached
    tokenizer = cached["tokenizer"]
    model_obj = cached["model"]
    device = cached["device"]
    prompt = build_segment_prompt(messages)
    if getattr(tokenizer, "chat_template", None):
        chat = [
            {"role": "system", "content": "Return only JSON. No markdown."},
            {"role": "user", "content": prompt},
        ]
        input_ids = tokenizer.apply_chat_template(chat, add_generation_prompt=True, return_tensors="pt").to(device)
        outputs = model_obj.generate(input_ids, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][input_ids.shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    else:
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
        inputs = {key: value.to(device) for key, value in inputs.items()}
        outputs = model_obj.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][inputs["input_ids"].shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    return parse_first_json_object(response)


def normalize_model_segments(raw: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw, list):
        raw_segments = raw
    elif isinstance(raw, dict) and isinstance(raw.get("segments"), list):
        raw_segments = raw["segments"]
    else:
        raise MatrixArkError("OSS segment provider must return {segments:[...]}")
    max_index = len(messages) - 1
    normalized: list[Json] = []
    for raw_segment in raw_segments[:12]:
        if not isinstance(raw_segment, dict):
            continue
        topic = re.sub(r"[^a-z0-9_]+", "_", str(raw_segment.get("topic") or "model_segment").lower()).strip("_") or "model_segment"
        coordinate_tuples = normalize_coordinate_tuples(raw_segment.get("coordinate_tuples"), max_index)
        message_indexes = normalize_message_indexes(raw_segment.get("message_indexes"), coordinate_tuples, max_index)
        if not message_indexes:
            continue
        if not coordinate_tuples:
            coordinate_tuples = contiguous_ranges(message_indexes)
        segment_text = "\n".join(f"{index}: {messages[index].get('content', '')}" for index in message_indexes)
        saliency = raw_segment.get("saliency_score", 0.85)
        try:
            saliency_score = max(0.0, min(1.0, float(saliency)))
        except (TypeError, ValueError):
            saliency_score = 0.85
        summary_text = str(raw_segment.get("summary_text") or summarize_text(segment_text, limit=420))
        normalized.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": message_indexes,
                "saliency_score": round(saliency_score, 6),
                "summary_text": summarize_text(summary_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "detected_by": "oss_model",
            }
        )
    normalized.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return normalized


def normalize_coordinate_tuples(value: Any, max_index: int) -> list[list[int]]:
    ranges: list[list[int]] = []
    if not isinstance(value, list):
        return ranges
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            continue
        try:
            start = int(item[0])
            end = int(item[1])
        except (TypeError, ValueError):
            continue
        start = max(0, min(max_index, start))
        end = max(0, min(max_index, end))
        if end < start:
            start, end = end, start
        ranges.append([start, end])
    return ranges


def normalize_message_indexes(value: Any, coordinate_tuples: list[list[int]], max_index: int) -> list[int]:
    indexes: set[int] = set()
    if isinstance(value, list):
        for item in value:
            try:
                index = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= index <= max_index:
                indexes.add(index)
    for start, end in coordinate_tuples:
        indexes.update(range(start, end + 1))
    return sorted(indexes)

def intelligent_memory_segments(messages: list[Json]) -> list[Json]:
    """Segment a batch into salient, event-centric memories.

    The production provider can emit the same coordinate tuples from one LLM
    call. The local implementation does deterministic semantic saliency and
    topic grouping, including non-contiguous segment consolidation.
    """

    salient: list[tuple[int, Json, str, str, float]] = []
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        saliency = semantic_saliency_score(text)
        if saliency < 0.5:
            continue
        topic = infer_segment_topic(text)
        salient.append((index, message, text, topic, saliency))
    grouped: dict[str, list[tuple[int, Json, str, float]]] = {}
    for index, message, text, topic, saliency in salient:
        grouped.setdefault(topic, []).append((index, message, text, saliency))

    segments = []
    for topic, items in grouped.items():
        if not items:
            continue
        coordinate_tuples = contiguous_ranges([item[0] for item in items])
        segment_text = "\n".join(f"{index}: {text}" for index, _message, text, _score in items)
        avg_saliency = sum(item[3] for item in items) / len(items)
        segments.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": [item[0] for item in items],
                "saliency_score": round(avg_saliency, 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def semantic_saliency_score(text: str) -> float:
    lower = text.lower().strip()
    if not lower:
        return 0.0
    filler = {
        "hi",
        "hello",
        "hey",
        "thanks",
        "thank you",
        "ok",
        "okay",
        "cool",
        "great",
        "sounds good",
    }
    compact = re.sub(r"[^a-z0-9 ]+", "", lower).strip()
    if compact in filler or len(compact) < 8:
        return 0.0
    score = 0.2
    if re.search(r"\b(recursion|base case|merge sort|algorithm|complexity|efficiency|dynamic programming|graph|game)\b", lower):
        score += 0.55
    if re.search(r"\b(prefer|favorite|approved|budget|plan|correction|instead|current|remember|important|moved|moving|located|location|live|lives|staying|deadline|owner|owns|reviewer|checklist|decision|decided|require|requires|required|incident|runbook|alert|outage|rollback|metric|latency|p95|p99|sla|policy|risk|blocked|blocker)\b", lower):
        score += 0.45
    if re.search(r"\b(is|means|because|therefore|warning|avoid|must|should|cannot|can|require|requires|required|blocked|blocker)\b", lower):
        score += 0.2
    if re.search(r"\b(\d{2,}|monday|tuesday|wednesday|thursday|friday|saturday|sunday|january|february|march|april|may|june|july|august|september|october|november|december)\b", lower):
        score += 0.1
    if len(tokens(text)) >= 8:
        score += 0.15
    return min(score, 1.0)


def infer_segment_topic(text: str) -> str:
    lower = text.lower()
    topic_keywords = [
        ("recursion", ["recursion", "recursive", "base case", "merge sort", "call stack"]),
        ("game_algorithm", ["game", "minimax", "alpha beta", "pathfinding", "npc"]),
        ("preference", ["prefer", "favorite", "likes", "loves"]),
        ("location", ["moved", "moving", "located", "location", "live", "lives", "staying"]),
        ("approval_budget", ["approved", "approval", "budget", "cost", "purchase"]),
        ("incident_runbook", ["incident", "runbook", "alert", "outage", "rollback", "postmortem"]),
        ("task_decision", ["decision", "decided", "owner", "owns", "deadline", "checklist", "reviewer", "require", "requires", "required"]),
        ("metric_sla", ["metric", "latency", "p95", "p99", "qps", "sla", "error rate"]),
        ("plan_status", ["plan", "current", "status", "going to", "will"]),
        ("correction", ["correction", "instead", "wrong", "changed", "updated"]),
    ]
    for topic, keywords in topic_keywords:
        if any(keyword in lower for keyword in keywords):
            return topic
    token_list = [token for token in tokens(text) if len(token) > 4]
    return token_list[0] if token_list else "general"


def contiguous_ranges(indexes: list[int]) -> list[list[int]]:
    if not indexes:
        return []
    ordered = sorted(set(indexes))
    ranges: list[list[int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append([start, previous])
        start = previous = value
    ranges.append([start, previous])
    return ranges


def infer_event_type(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ["correct", "correction", "wrong", "instead", "updated", "changed"]):
        return "correction"
    if any(term in lower for term in ["yes", "confirmed", "approved", "looks good"]):
        return "confirmation"
    if any(term in lower for term in ["prefer", "favorite", "like", "love"]):
        return "preference_update"
    if any(term in lower for term in ["plan", "going to", "will ", "schedule"]):
        return "plan_update"
    if any(term in lower for term in ["work", "job", "role", "status", "position"]):
        return "status_update"
    return "dialogue_batch"


UNDERSTANDING_LABELS: dict[str, str] = {
    "confirmation": "confirmation approval accepted answer yes correct looks good",
    "correction": "correction wrong changed updated instead stale fact",
    "preference_update": "user preference likes prefers favorite language tool choice",
    "plan_update": "future plan schedule going to next step planned trip",
    "status_update": "job role work status position current responsibility",
    "approval": "business approval purchase approval budget approval confirmed cost",
    "location": "current location city moved to lives in staying at",
    "relationship": "relationship manager sister brother teammate family person",
    "family_profile": "family profile pet dog cat child sibling household fact",
    "current_plan": "current plan upcoming action task to complete next milestone",
    "session": "general conversation memory useful session fact",
}

QUERY_TYPE_LABELS: dict[str, str] = {
    "date": "question asks when date before after yesterday tomorrow week month year",
    "current_state": "question asks current latest now still status preference location role valid state",
    "why_emotion": "question asks why reason feeling emotion because",
    "evidence": "question asks quote exact message evidence what did someone say",
    "procedure": "question asks procedure steps troubleshoot debug rollback runbook checklist how to fix",
    "broad_exploration": "question asks overview summarize broad exploration topics inventory what is known",
    "multi_hop": "question requires combining multiple sessions people facts cross conversation reasoning",
    "fact": "question asks a direct factual answer",
}

QUERY_INDEX_LABELS: dict[str, str] = {
    "entity_type:location": "location city moved lives staying where user is",
    "entity_type:preference": "preference prefer favorite likes language tool choice",
    "event_type:preference_update": "preference update changed choice likes prefers",
    "entity_type:relationship": "relationship manager sister brother teammate family person",
    "entity_type:family_profile": "family pet dog cat child household",
    "entity_type:job_status": "job role work status position responsibility",
    "event_type:status_update": "job status role work update",
    "entity_type:current_plan": "plan current plan upcoming task schedule next milestone",
    "event_type:plan_update": "plan update going to schedule will next",
    "event_type:confirmation": "confirmation approved accepted yes correct confirmed",
    "entity_type:approval_state": "approval budget purchase cost approved",
    "entity_type:confirmation": "confirmation approved correct accepted",
    "classification:confirmation": "confirmation approved accepted yes correct",
    "segment_topic:approval_budget": "approval budget purchase GPU cost finance",
    "event_type:correction": "correction wrong changed updated instead stale",
    "entity_type:correction": "correction wrong changed updated instead",
    "classification:correction": "correction wrong changed update",
    "segment_topic:correction": "correction updated stale changed",
    "source_type:message": "raw message dialogue evidence",
    "source_type:feedback": "feedback accepted rejected final answer",
    "source_type:resource": "resource document file pdf markdown text csv table runbook policy docs",
    "source_type:skill": "skill tool instruction playbook procedure capability",
    "source_type:resource_fact": "extracted fact from resource decision owner cost deadline policy approval risk procedure api",
    "resource_type:pdf": "pdf document page file",
    "resource_type:md": "markdown md readme documentation runbook",
    "resource_type:txt": "text txt note plain document",
    "resource_type:csv": "csv table rows spreadsheet",
    "resource_type:tsv": "tsv table rows spreadsheet",
    "resource_type:xlsx": "excel xlsx spreadsheet workbook sheet table",
    "resource_type:html": "html web page documentation",
    "resource_type:docx": "word docx document",
    "resource_type:pptx": "powerpoint pptx slide deck presentation",
    "unit_kind:paragraph": "paragraph text passage",
    "unit_kind:heading": "heading section title",
    "unit_kind:table_row_group": "table rows row group csv spreadsheet",
    "unit_kind:page": "page pdf page",
    "unit_kind:slide": "slide presentation deck",
    "unit_kind:code_symbol": "code function class symbol",
    "skill_trigger:context_pack_replay": "context pack replay audit inspect selected refs",
    "skill_tool:matrixark_replay": "matrixark replay tool context replay",
    "skill_tool:matrixark_audit": "matrixark audit tool context audit",
}


def require_oss_understanding() -> bool:
    return os.getenv("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "").strip().lower() in {"1", "true", "yes"}


def understanding_provider(envelope: Json | None = None) -> str:
    provider = ""
    if envelope:
        provider = str(envelope.get("understanding_provider") or envelope.get("extraction_provider") or "")
    provider = provider or os.getenv("MATRIXARK_UNDERSTANDING_PROVIDER", os.getenv("MATRIXARK_EXTRACTION_PROVIDER", "deterministic"))
    provider = provider.strip().lower().replace("-", "_")
    if provider in {"oss", "open_source", "embedding", "oss_embedding"}:
        return "oss_encoder"
    if provider in {"", "deterministic", "rules", "local"} and require_oss_understanding():
        raise MatrixArkError("deterministic extraction/query understanding is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
    return provider or "deterministic"


def prototype_vectors(labels: dict[str, str]) -> dict[str, list[float]]:
    cache_key = json.dumps(labels, sort_keys=True) + "|" + embedding_model_name()
    cached = _OSS_UNDERSTANDING_PROTOTYPE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    vectors = {label: embedding_for_text(description) for label, description in labels.items()}
    _OSS_UNDERSTANDING_PROTOTYPE_CACHE[cache_key] = vectors
    return vectors


def oss_encoder_rank_labels(text: str, labels: dict[str, str], *, limit: int = 5) -> list[Json]:
    query_vector = embedding_for_text(text)
    ranked = [
        {
            "label": label,
            "score": round(normalized_dense_score(cosine(query_vector, vector)), 6),
            "description": labels[label],
        }
        for label, vector in prototype_vectors(labels).items()
    ]
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:limit]


def oss_encoder_event_type(text: str) -> str:
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
    label = str(ranked[0]["label"]) if ranked else "session"
    if label == "approval":
        return "confirmation"
    if label == "location":
        return "status_update"
    if label in {"relationship", "family_profile"}:
        return "dialogue_batch"
    return label


def oss_encoder_compact_extraction(envelope: Json, *, prior_context: Json) -> Json:
    text = text_from_messages(envelope["messages"])
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=5)
    top = str(ranked[0]["label"]) if ranked else "session"
    classification = "NEW_EVENT"
    status = "observed"
    if envelope["kind"] == "feedback":
        if not prior_context.get("level"):
            classification = "AMBIGUOUS"
        elif top in {"confirmation", "approval"}:
            classification = "CONFIRMATION"
            status = "accepted"
        elif top == "correction":
            classification = "CORRECTION"
            status = "rejected"
        else:
            classification = "FEEDBACK"
    return {
        "mode": "matrixark_internal_oss_encoder",
        "understanding_provider": "oss_encoder",
        "classification": classification,
        "status": status,
        "event_type": oss_encoder_event_type(text),
        "label_scores": ranked,
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "" if classification != "AMBIGUOUS" else "short feedback lacks prior context",
    }


def oss_encoder_extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    text = text_from_messages(messages)
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=8)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    entities: list[Json] = []
    for item in ranked:
        label = str(item["label"])
        if label == "approval":
            entity_type = "approval_state"
        elif label == "status_update":
            entity_type = "job_status"
        elif label == "plan_update":
            entity_type = "current_plan"
        elif label == "preference_update":
            entity_type = "preference"
        else:
            entity_type = label
        if float(item["score"]) < 0.42 and entity_type != "session":
            continue
        state = summarize_text(f"{entity_type}: {text}", limit=220)
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": canonical_entity_name(entity_type, state) or entity_type,
                "state": state,
                "confidence": round(float(item["score"]), 6),
                "source_refs": source_refs,
                "operator": "LLM_MERGE" if entity_type not in {"confirmation", "correction"} else "LATEST",
                "field_patches": [entity_patch("", state)] if entity_type != "session" else [],
                "extracted_by": "oss_encoder",
            }
        )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.5,
                "source_refs": source_refs,
                "operator": "LLM_MERGE",
                "field_patches": [],
                "extracted_by": "oss_encoder",
            }
        )
    return dedupe_entities(entities)


def oss_encoder_memory_segments(messages: list[Json]) -> list[Json]:
    labeled: dict[str, list[tuple[int, Json, float]]] = {}
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        if not text.strip():
            continue
        ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
        label = str(ranked[0]["label"]) if ranked else "session"
        score = float(ranked[0]["score"]) if ranked else 0.5
        labeled.setdefault(label, []).append((index, message, score))
    segments = []
    for label, items in labeled.items():
        indexes = [index for index, _message, _score in items]
        ranges = contiguous_ranges(indexes)
        segment_text = "\n".join(f"{index}: {message.get('content', '')}" for index, message, _score in items)
        segments.append(
            {
                "topic": label,
                "coordinate_tuples": ranges,
                "message_indexes": indexes,
                "saliency_score": round(sum(score for _index, _message, score in items) / max(len(items), 1), 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(ranges) > 1,
                "detected_by": "oss_encoder",
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*([^.;!?]{2,120})"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
    ]
    for entity_type, pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = " ".join(match.group(1).split()).strip(" :-") if match.groups() else ""
            if entity_type == "confirmation" and not envelope.get("context_pack_id") and not lower.strip() in {
                "yes",
                "yes.",
                "correct",
                "correct.",
                "approved",
                "approved.",
            }:
                continue
            entity_name = canonical_entity_name(entity_type, value)
            field_patches = infer_entity_field_patches(entity_type, value, text)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": source_refs,
                    "operator": "LLM_MERGE" if entity_type not in {"confirmation", "correction"} else "LATEST",
                    "field_patches": field_patches,
                }
            )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.6,
                "source_refs": source_refs,
                "operator": "LLM_MERGE",
                "field_patches": [],
            }
        )
    return dedupe_entities(entities)


def infer_entity_field_patches(entity_type: str, value: str, text: str) -> list[Json]:
    patches: list[Json] = []
    correction = re.search(
        r"\b(?:correction|correct|wrong|updated|changed)[:\s]+([^.;!?]+?)\s+(?:instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if correction:
        replace = clean_patch_value(correction.group(1))
        search = clean_patch_value(correction.group(2))
        patches.append(entity_patch(search, replace))
    preference = re.search(
        r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]+?)\s+(?:now|instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if entity_type == "preference" and preference:
        replace = clean_patch_value(preference.group(1))
        search = clean_patch_value(preference.group(2))
        patches.append(entity_patch(search, replace))
    evolving_entity_types = {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "relationship",
        "approval_state",
        "correction",
        "confirmation",
    }
    if entity_type in evolving_entity_types and not patches and value:
        patches.append(entity_patch("", summarize_text(value, limit=180)))
    return patches[:3]


def clean_patch_value(value: str) -> str:
    return summarize_text(" ".join(value.split()).strip(" ,;:-"), limit=180)


def canonical_entity_name(entity_type: str, value: str) -> str:
    if entity_type in {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "correction",
        "confirmation",
    }:
        return entity_type
    return value[:80] if value else entity_type


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    positions: dict[tuple[Any, str], int] = {}
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            if entity.get("entity_name") == entity.get("entity_type"):
                out[positions[key]] = entity
            continue
        seen.add(key)
        positions[key] = len(out)
        out.append(entity)
    return out[:12]


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


RAW_BYTE_METADATA_FIELDS = {"raw_bytes", "file_bytes", "bytes", "binary", "payload_bytes", "data_url", "base64"}
SERVING_RESOURCE_METADATA_FIELDS = {
    "resource_type",
    "resource_version",
    "unit_kind",
    "relative_path",
    "heading",
    "heading_slug",
    "heading_path",
    "source_locator",
    "content_hash",
    "token_estimate",
    "row_start",
    "row_end",
    "record_start",
    "record_end",
    "page",
    "page_section",
    "slide_number",
    "sheet_name",
    "row_count",
    "supersedes_chunk_hash",
}
DEBUG_RESOURCE_METADATA_FIELDS = {
    "embedding_text",
    "parse_warnings",
    "parser_name",
    "parser_version",
    "parse_warning_count",
    "columns",
    "links",
    "tables",
    "front_matter",
}


def sanitize_resource_metadata(metadata: Json) -> Json:
    sanitized = {
        key: value
        for key, value in metadata.items()
        if key not in RAW_BYTE_METADATA_FIELDS
    }
    sanitized["parse_warnings"] = normalize_parse_warnings(sanitized)
    sanitized["raw_storage_policy"] = str(sanitized.get("raw_storage_policy") or "raw_uri_only")
    sanitized["raw_bytes_stored"] = False
    return sanitized


def serving_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    serving = {
        key: sanitized[key]
        for key in SERVING_RESOURCE_METADATA_FIELDS
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    serving["raw_storage_policy"] = sanitized.get("raw_storage_policy", "raw_uri_only")
    serving["raw_bytes_stored"] = False
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        serving["parse_warning_count"] = len(parse_warnings)
        serving["has_parse_warnings"] = True
    return serving


def debug_resource_metadata(metadata: Json) -> Json:
    sanitized = sanitize_resource_metadata(metadata)
    debug = {
        key: sanitized[key]
        for key in sorted(DEBUG_RESOURCE_METADATA_FIELDS)
        if key in sanitized and sanitized[key] not in (None, "", [], {})
    }
    parse_warnings = normalize_parse_warnings(sanitized)
    if parse_warnings:
        debug["parse_warnings"] = parse_warnings
        debug["parse_warning_count"] = len(parse_warnings)
    embedding_text = str(sanitized.get("embedding_text") or "")
    if embedding_text:
        debug["embedding_text_hash"] = stable_hash(embedding_text)
        debug["embedding_text_preview"] = summarize_text(embedding_text, limit=320)
    return debug


def source_locator_from_ref(source_ref: str, raw_uri: str) -> str:
    source_ref = str(source_ref or "")
    raw_uri = str(raw_uri or "")
    if not source_ref:
        return ""
    if raw_uri and source_ref == raw_uri:
        return ""
    if raw_uri and source_ref.startswith(raw_uri + "#"):
        return source_ref.partition("#")[2]
    if "#" in source_ref:
        return source_ref.partition("#")[2]
    return source_ref


def source_ref_from_locator(raw_uri: str, source_locator: str) -> str:
    raw_uri = str(raw_uri or "")
    source_locator = str(source_locator or "")
    if not source_locator:
        return raw_uri
    if source_locator.startswith(("file:", "s3://", "http://", "https://", "/")):
        return source_locator
    return f"{raw_uri}#{source_locator}" if raw_uri else source_locator


def registry_access_scope(scope: Json) -> Json:
    return {
        "account_id": str(scope.get("account_id") or ""),
        "tenant_id": str(scope.get("tenant_id") or ""),
        "team": str(scope.get("team") or ""),
        "user_id": str(scope.get("user_id") or ""),
        "session_id": str(scope.get("session_id") or ""),
        "account_hash": scope.get("account_hash", 0),
        "tenant_hash": scope.get("tenant_hash", 0),
        "user_hash": scope.get("user_hash", 0),
        "session_hash": scope.get("session_hash", 0),
        "scope_key": scope.get("scope_key", ""),
    }


def deployment_scope_from_args(args: Json, envelope: Json) -> str:
    value = str(
        args.get("deployment_scope")
        or envelope.get("metadata", {}).get("deployment_scope")
        or os.environ.get("MATRIXARK_DEPLOYMENT_SCOPE")
        or "local"
    ).strip().lower()
    return value if value in {"local", "global", "cloud", "on_prem", "hybrid"} else "local"


def resource_storage_mode_from_args(args: Json, envelope: Json, deployment_scope: str) -> str:
    value = str(
        args.get("raw_storage_mode")
        or envelope.get("metadata", {}).get("raw_storage_mode")
        or os.environ.get("MATRIXARK_RESOURCE_STORAGE_MODE")
        or ("cloud" if deployment_scope == "cloud" else "local")
    ).strip().lower()
    if value in {"s3", "remote"}:
        value = "cloud"
    if value not in {"local", "cloud"}:
        raise MatrixArkError("raw_storage_mode must be local or cloud")
    return value


def is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not is_s3_uri(uri):
        raise MatrixArkError(f"not an s3 uri: {uri}")
    rest = uri[len("s3://") :]
    bucket, sep, key = rest.partition("/")
    if not bucket or not sep or not key:
        raise MatrixArkError(f"invalid s3 uri: {uri}")
    return bucket, key


def _cloud_resource_bucket(args: Json, envelope: Json) -> str:
    bucket = str(
        args.get("s3_bucket")
        or envelope.get("metadata", {}).get("s3_bucket")
        or os.environ.get("MATRIXARK_RESOURCE_S3_BUCKET")
        or os.environ.get("MATRIXARK_S3_BUCKET")
        or ""
    ).strip()
    if not bucket:
        raise MatrixArkError("cloud raw resource storage requires s3_bucket or MATRIXARK_RESOURCE_S3_BUCKET")
    return bucket


def _cloud_resource_prefix(args: Json, envelope: Json) -> str:
    prefix = str(
        args.get("s3_prefix")
        or envelope.get("metadata", {}).get("s3_prefix")
        or os.environ.get("MATRIXARK_RESOURCE_S3_PREFIX")
        or "matrixark/raw"
    ).strip().strip("/")
    scope = envelope.get("scope", {}) if isinstance(envelope.get("scope", {}), dict) else {}
    parts = [
        prefix,
        safe_identifier(str(scope.get("account_id") or "acct"), default="acct"),
        safe_identifier(str(scope.get("tenant_id") or "tenant"), default="tenant"),
        safe_identifier(str(scope.get("user_id") or "user"), default="user"),
    ]
    session_id = str(scope.get("session_id") or "")
    if session_id:
        parts.append(safe_identifier(session_id, default="session"))
    return "/".join(part for part in parts if part)


def _s3_client() -> Any:
    try:
        import boto3  # type: ignore

        kwargs: Json = {}
        endpoint_url = os.environ.get("MATRIXARK_S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL_S3")
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        region_name = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if region_name:
            kwargs["region_name"] = region_name
        return boto3.client("s3", **kwargs)
    except Exception:
        return None


def _aws_cli_s3_cp(source: str, target: str) -> None:
    command = ["aws"]
    profile = os.environ.get("AWS_PROFILE")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if profile:
        command.extend(["--profile", profile])
    if region:
        command.extend(["--region", region])
    endpoint_url = os.environ.get("MATRIXARK_S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL_S3")
    if endpoint_url:
        command.extend(["--endpoint-url", endpoint_url])
    command.extend(["s3", "cp", source, target])
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise MatrixArkError(compact_ws(completed.stderr or completed.stdout or f"aws s3 cp failed: {source} -> {target}"))


def upload_file_to_s3(path: Path, *, bucket: str, key: str) -> str:
    client = _s3_client()
    if client is not None:
        try:
            client.upload_file(str(path), bucket, key)
            return f"s3://{bucket}/{key}"
        except Exception as exc:
            raise MatrixArkError(f"S3 upload failed for {path}: {exc}") from exc
    target = f"s3://{bucket}/{key}"
    _aws_cli_s3_cp(str(path), target)
    return target


def download_s3_to_file(uri: str, target: Path) -> Path:
    bucket, key = parse_s3_uri(uri)
    target.parent.mkdir(parents=True, exist_ok=True)
    client = _s3_client()
    if client is not None:
        try:
            client.download_file(bucket, key, str(target))
            return target
        except Exception as exc:
            raise MatrixArkError(f"S3 download failed for {uri}: {exc}") from exc
    _aws_cli_s3_cp(uri, str(target))
    return target


def _resource_object_key(prefix: str, raw_uri: str, source_path: Path | None, resource_type: str) -> str:
    suffix = Path(raw_uri).name if raw_uri and raw_uri != "inline-resource" else ""
    if source_path is not None:
        suffix = source_path.name
    suffix = safe_identifier(suffix or f"resource.{resource_type or 'txt'}", default="resource")
    digest = hashlib.sha256()
    digest.update(raw_uri.encode("utf-8", errors="ignore"))
    if source_path is not None and source_path.exists() and source_path.is_file():
        try:
            with source_path.open("rb") as fh:
                for block in iter(lambda: fh.read(1024 * 1024), b""):
                    digest.update(block)
        except OSError:
            pass
    return f"{prefix}/{digest.hexdigest()[:16]}-{suffix}"


def _archive_directory_for_upload(path: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="matrixark-resource-dir-"))
    archive_base = temp_dir / safe_identifier(path.name or "resource-dir", default="resource-dir")
    archive_path = shutil.make_archive(str(archive_base), "gztar", root_dir=str(path))
    return Path(archive_path)


def resolve_raw_resource_for_ingest(args: Json, envelope: Json, raw_uri: str, resource_type: str, deployment_scope: str, resource_text: str) -> Json:
    """Resolve local/cloud raw storage and parser source for resource/skill ingest."""
    mode = resource_storage_mode_from_args(args, envelope, deployment_scope)
    raw_uri = raw_uri or "inline-resource"
    result: Json = {
        "storage_mode": mode,
        "original_raw_uri": raw_uri,
        "stored_raw_uri": raw_uri,
        "parse_uri": raw_uri,
        "parse_text": resource_text,
        "raw_storage_policy": "local_raw_uri_only" if mode == "local" else "s3_raw_uri_only",
        "raw_bytes_stored": False,
        "upload_status": "not_required",
        "temp_paths": [],
        "cloud_bucket": "",
        "cloud_key": "",
    }
    local_path = Path(raw_uri) if raw_uri != "inline-resource" and not is_s3_uri(raw_uri) else None
    if mode == "local":
        if local_path is not None and local_path.exists():
            result["parse_text"] = None
        return result

    bucket = _cloud_resource_bucket(args, envelope)
    prefix = _cloud_resource_prefix(args, envelope)
    result["cloud_bucket"] = bucket

    if is_s3_uri(raw_uri):
        stored_uri = raw_uri
    else:
        upload_path: Path
        if local_path is not None and local_path.exists():
            upload_path = _archive_directory_for_upload(local_path) if local_path.is_dir() else local_path
            if local_path.is_dir():
                result["temp_paths"].append(str(upload_path.parent))
                result["parse_uri"] = str(local_path)
                result["parse_text"] = None
        else:
            suffix = infer_resource_suffix(resource_type, raw_uri)
            temp_file = Path(tempfile.mkdtemp(prefix="matrixark-inline-resource-")) / f"inline.{suffix}"
            temp_file.write_text(resource_text, encoding="utf-8")
            result["temp_paths"].append(str(temp_file.parent))
            upload_path = temp_file
        key = _resource_object_key(prefix, raw_uri, upload_path, resource_type)
        stored_uri = upload_file_to_s3(upload_path, bucket=bucket, key=key)
        result["upload_status"] = "uploaded"
        result["cloud_key"] = key

    result["stored_raw_uri"] = stored_uri
    if result.get("parse_uri") == raw_uri or is_s3_uri(raw_uri):
        suffix = infer_resource_suffix(resource_type, stored_uri)
        temp_file = Path(tempfile.mkdtemp(prefix="matrixark-s3-resource-")) / f"downloaded.{suffix}"
        result["temp_paths"].append(str(temp_file.parent))
        download_s3_to_file(stored_uri, temp_file)
        result["parse_uri"] = str(temp_file)
        result["parse_text"] = None
    return result


def infer_resource_suffix(resource_type: str, raw_uri: str) -> str:
    suffix = (resource_type or "").lower().lstrip(".")
    if not suffix and raw_uri and raw_uri != "inline-resource":
        suffix = Path(raw_uri).suffix.lower().lstrip(".")
    return suffix or "txt"


def rewrite_chunk_uris(chunks: list[Any], *, parse_uri: str, stored_raw_uri: str) -> list[Any]:
    if not stored_raw_uri or stored_raw_uri == parse_uri:
        return chunks
    rewritten: list[Any] = []
    for chunk in chunks:
        metadata = dict(getattr(chunk, "metadata", {}) or {})
        old_source_ref = str(getattr(chunk, "source_ref", ""))
        fragment = old_source_ref.partition("#")[2]
        relative_path = str(metadata.get("relative_path") or "").strip()
        if relative_path and fragment:
            new_source_ref = f"{stored_raw_uri}#path={relative_path}&{fragment}"
        elif fragment:
            new_source_ref = f"{stored_raw_uri}#{fragment}"
        else:
            new_source_ref = stored_raw_uri
        metadata["raw_uri"] = stored_raw_uri
        metadata["citation"] = new_source_ref
        metadata["source_ref"] = new_source_ref
        metadata["raw_storage_policy"] = "s3_raw_uri_only" if is_s3_uri(stored_raw_uri) else metadata.get("raw_storage_policy", "raw_uri_only")
        metadata["raw_bytes_stored"] = False
        piece_hash = str(metadata.get("content_hash") or content_hash(str(getattr(chunk, "text", ""))))
        version = str(metadata.get("resource_version") or "")
        chunk_hash = stable_hash(f"resource_chunk:{new_source_ref}:{version}:{piece_hash}")
        rewritten.append(
            chunk.__class__(
                chunk_hash=chunk_hash,
                source_ref=new_source_ref,
                text=getattr(chunk, "text", ""),
                token_estimate=int(getattr(chunk, "token_estimate", 1)),
                metadata=metadata,
            )
        )
    return rewritten


def cleanup_temp_paths(paths: list[str]) -> None:
    for path_text in paths:
        try:
            path = Path(path_text)
            if path.exists() and path.is_dir() and path.name.startswith("matrixark-"):
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass


def aggregate_parse_warnings_from_chunks(chunks: list[Any]) -> list[str]:
    warnings: list[str] = []
    for chunk in chunks:
        metadata = getattr(chunk, "metadata", {}) or {}
        for warning in normalize_parse_warnings(metadata):
            if warning not in warnings:
                warnings.append(warning)
    return warnings


def normalized_index_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_.:/-]+", "_", text)
    return text.strip("_")


def context_index_name(kind: str, value: Any) -> str:
    normalized = normalized_index_value(value)
    return f"{kind}:{normalized}" if normalized else ""


def non_default_classification(value: Any) -> str:
    classification = str(value or "").strip().upper()
    return "" if classification in {"", "NEW_EVENT"} else classification


def metadata_index_terms(metadata: Json, *, keyword_limit: int = MAX_METADATA_KEYWORD_INDEXES_PER_CHUNK) -> list[str]:
    terms: list[str] = []
    for field in ["unit_kind", "heading_slug", "relative_path"]:
        terms.append(context_index_name(field, metadata.get(field)))
    for keyword in metadata.get("keywords", [])[: max(0, keyword_limit)]:
        terms.append(context_index_name("keyword", keyword))
    return ordered_unique(terms)


def secondary_index_priority(term: str) -> int:
    for index, prefix in enumerate(SECONDARY_INDEX_PRIORITY_PREFIXES):
        if term.startswith(prefix):
            return index
    return len(SECONDARY_INDEX_PRIORITY_PREFIXES)


def limited_index_terms(terms: list[str], *, limit: int) -> list[str]:
    unique_terms = ordered_unique([term for term in terms if term])
    capped_limit = max(0, int(limit))
    return [
        term
        for _, term in sorted(
            enumerate(unique_terms),
            key=lambda item: (secondary_index_priority(item[1]), item[0]),
        )
    ][:capped_limit]


RESOURCE_FACT_KEYWORDS = re.compile(
    r"\b(decision|decided|owner|owns|deadline|due|cost|budget|approval|approved|risk|policy|must|should|required|requires|api|endpoint|contract|runbook|rollback|incident|troubleshoot|alert|sla|p95|p99|procedure|checklist)\b",
    flags=re.IGNORECASE,
)

RESOURCE_FACT_SCHEMAS: list[Json] = [
    {
        "fact_type": "resource_decision",
        "entity_type": "resource_decision",
        "entity_prefix": "decision",
        "keywords": ["decision", "decided", "approved", "rejected", "selected"],
    },
    {
        "fact_type": "resource_owner",
        "entity_type": "resource_owner",
        "entity_prefix": "owner",
        "keywords": ["owner", "owns", "reviewer", "assignee", "responsible"],
    },
    {
        "fact_type": "resource_cost",
        "entity_type": "resource_cost",
        "entity_prefix": "cost",
        "keywords": ["cost", "budget", "amount", "price", "spend", "$"],
    },
    {
        "fact_type": "resource_deadline",
        "entity_type": "resource_deadline",
        "entity_prefix": "deadline",
        "keywords": ["deadline", "due", "by monday", "by tuesday", "by wednesday", "by thursday", "by friday", "by saturday", "by sunday"],
    },
    {
        "fact_type": "resource_api_contract",
        "entity_type": "resource_api_contract",
        "entity_prefix": "api",
        "keywords": ["api", "endpoint", "contract", "schema", "request", "response", "http", "grpc"],
    },
    {
        "fact_type": "resource_troubleshooting_step",
        "entity_type": "resource_troubleshooting",
        "entity_prefix": "troubleshooting",
        "keywords": ["troubleshoot", "debug", "incident", "alert", "rollback", "runbook", "remediation", "mitigation"],
    },
    {
        "fact_type": "resource_policy",
        "entity_type": "resource_policy",
        "entity_prefix": "policy",
        "keywords": ["policy", "must", "should", "required", "requires", "cannot", "allowed"],
    },
    {
        "fact_type": "resource_approval",
        "entity_type": "resource_approval",
        "entity_prefix": "approval",
        "keywords": ["approval", "approved", "approve", "signoff", "confirmed"],
    },
    {
        "fact_type": "resource_risk",
        "entity_type": "resource_risk",
        "entity_prefix": "risk",
        "keywords": ["risk", "blocker", "blocked", "failure", "unsafe", "degraded"],
    },
    {
        "fact_type": "resource_procedure",
        "entity_type": "resource_procedure",
        "entity_prefix": "procedure",
        "keywords": ["procedure", "step", "checklist", "first", "then", "verify", "confirm"],
    },
]


def should_extract_resource_fact(text: str, metadata: Json) -> bool:
    if RESOURCE_FACT_KEYWORDS.search(text):
        return True
    unit_kind = str(metadata.get("unit_kind", ""))
    return unit_kind in {"table_row", "table_row_group", "xlsx_row", "xlsx_row_group", "json_document", "json_record", "json_record_group"}


def matched_resource_fact_schemas(text: str, metadata: Json) -> list[Json]:
    lower = text.lower()
    matches = [
        schema
        for schema in RESOURCE_FACT_SCHEMAS
        if any(keyword in lower for keyword in schema["keywords"])
    ]
    if matches:
        return matches[: max(0, MAX_RESOURCE_FACTS_PER_CHUNK)]
    if ENABLE_GENERIC_RESOURCE_FACTS and should_extract_resource_fact(text, metadata):
        return [{"fact_type": "resource_fact", "entity_type": "resource_fact", "entity_prefix": "fact", "keywords": []}]
    return []


def extract_resource_fact_value(text: str, fact_type: str) -> str:
    patterns = {
        "resource_owner": r"\b(?:owner|owns|reviewer|assignee|responsible)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_deadline": r"\b(?:deadline|due)\s*(?:is|:|=|by)?\s*([^.;\n]{2,120})",
        "resource_cost": r"\b(?:cost|budget|amount|price|spend)\s*(?:is|:|=)?\s*([^.;\n]{2,120})",
        "resource_api_contract": r"\b(?:api|endpoint|contract|schema)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_approval": r"\b(?:approval|approved|approve|signoff|confirmed)\s*(?:is|:|=)?\s*([^.;\n]{0,140})",
        "resource_risk": r"\b(?:risk|blocker|blocked|failure)\s*(?:is|:|=)?\s*([^.;\n]{2,160})",
        "resource_decision": r"\b(?:decision|decided)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_policy": r"\b(?:policy|must|should|required|requires)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_troubleshooting_step": r"\b(?:troubleshoot|debug|incident|alert|rollback|runbook|remediation|mitigation)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
        "resource_procedure": r"\b(?:procedure|step|checklist|verify|confirm)\s*(?:is|:|=)?\s*([^.;\n]{2,180})",
    }
    pattern = patterns.get(fact_type, "")
    if pattern:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return summarize_text(match.group(1).strip(" :-"), limit=180)
    return summarize_text(text, limit=220)


def resource_fact_entity_name(schema: Json, value: str, chunk_metadata: Json, raw_uri: str) -> str:
    prefix = str(schema.get("entity_prefix") or schema.get("entity_type") or "fact")
    semantic_value = summarize_text(str(value or "").strip(), limit=80).strip()
    if semantic_value:
        return f"{prefix}:{semantic_value}"
    heading = str(chunk_metadata.get("heading") or chunk_metadata.get("heading_slug") or "").strip()
    if heading:
        return f"{prefix}:{summarize_text(heading, limit=80)}"
    return prefix


def resource_extraction_mode(envelope: Json) -> str:
    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return "matrixark_resource_schema_oss_encoder"
    if provider in {"openai", "openai_compatible", "openai-compatible"}:
        return "matrixark_resource_schema_openai_compatible"
    return "matrixark_resource_schema"


def extract_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    """Extract cited resource facts through the same provider-shaped contract as messages.

    The local implementation is deterministic for CI. OSS/OpenAI-compatible
    providers should emit the same fields so storage, indexes, and replay stay
    unchanged.
    """
    mode = resource_extraction_mode(envelope)
    facts: list[Json] = []
    for fact_schema in matched_resource_fact_schemas(chunk.text, chunk.metadata):
        fact_event_type = str(fact_schema["fact_type"])
        fact_entity_type = str(fact_schema["entity_type"])
        fact_value = extract_resource_fact_value(chunk.text, fact_event_type)
        facts.append(
            {
                "mode": mode,
                "classification": "RESOURCE_FACT",
                "event_type": fact_event_type,
                "entity_type": fact_entity_type,
                "status": "observed",
                "value": fact_value,
                "entity_name": resource_fact_entity_name(fact_schema, fact_value, chunk_metadata, raw_uri),
                "confidence": 0.82 if fact_event_type != "resource_fact" else 0.68,
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": understanding_provider(envelope),
            }
        )
    return facts


def normalize_envelope(args: Json, *, default_kind: str) -> Json:
    messages = require_messages(args)
    scope = optional_object(args, "scope")
    metadata = optional_object(args, "metadata")
    kind = args.get("kind", default_kind)
    if kind not in {"message", "feedback", "resource", "skill", "business_data"}:
        raise MatrixArkError("kind is invalid")
    envelope: Json = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
        "ingestion_time_ms": now_ms(),
        "storage_options": normalize_storage_options(args, metadata),
    }
    envelope["storage_route"] = canonical_storage_route(envelope.get("storage_options", {}))
    for field in [
        "context_pack_id",
        "query_id_hash",
        "accepted_refs",
        "rejected_refs",
        "raw_uri",
        "resource_type",
        "source_event_ids",
        "segment_provider",
        "segment_model",
        "segment_model_path",
        "segment_max_new_tokens",
        "segment_provider_fallback",
        "understanding_provider",
        "extraction_provider",
    ]:
        if field in args:
            envelope[field] = args[field]
    return envelope


STORAGE_ROUTE_PRESETS: dict[str, Json] = {
    "shared_store_async": {
        "storage_family": "shared_store",
        "write_mode": "async",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "async",
        "raft_mode": False,
    },
    "shared_store_sync": {
        "storage_family": "shared_store",
        "write_mode": "sync",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "sync",
        "raft_mode": False,
    },
    "raft_async": {
        "storage_family": "raft",
        "write_mode": "async",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "async",
        "raft_mode": True,
    },
    "raft_sync": {
        "storage_family": "raft",
        "write_mode": "sync",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "sync",
        "raft_mode": True,
    },
}


def canonical_storage_route(storage_options: Json | None) -> Json:
    options = storage_options if isinstance(storage_options, dict) else {}
    storage_mode = str(options.get("storage_mode") or "default")
    replication_mode = str(options.get("replication_mode") or "default")
    oplog_mode = str(options.get("oplog_mode") or "default")
    raft_mode = bool(options.get("raft_mode", False))
    requested_family = str(options.get("storage_family") or options.get("family") or "default")
    requested_write_mode = str(options.get("write_mode") or "default")
    write_mode = requested_write_mode if requested_write_mode in {"async", "sync"} else oplog_mode
    if write_mode not in {"async", "sync"}:
        write_mode = "async"
    if requested_family == "raft" or storage_mode == "raft" or replication_mode == "raft" or raft_mode:
        route = f"raft_{write_mode}"
        backend_family = "raft"
        storage_mode = "raft" if storage_mode == "default" else storage_mode
        replication_mode = "raft" if replication_mode == "default" else replication_mode
        raft_mode = True
    elif requested_family == "shared_store" or storage_mode == "shared_store" or replication_mode == "shared_store":
        route = f"shared_store_{write_mode}"
        backend_family = "shared_store"
        storage_mode = "shared_store" if storage_mode == "default" else storage_mode
        replication_mode = "shared_store" if replication_mode == "default" else replication_mode
    else:
        route = f"{storage_mode}_{write_mode}" if storage_mode != "default" else "default"
        backend_family = storage_mode
    oplog_mode = write_mode if oplog_mode == "default" else oplog_mode
    background_write = bool(options.get("background_write", write_mode == "async"))
    return {
        "route": route,
        "route_key": route,
        "backend_family": backend_family,
        "storage_family": backend_family,
        "write_mode": write_mode,
        "storage_mode": storage_mode,
        "replication_mode": replication_mode,
        "oplog_mode": oplog_mode,
        "raft_mode": raft_mode,
        "consistency": str(options.get("consistency") or "default"),
        "sync_write": write_mode == "sync",
        "async_write": write_mode == "async",
        "background_write": background_write,
        "write_ack_policy": "ack_after_durable_commit" if write_mode == "sync" else "ack_after_memory_append",
        "native_backend_decides_route": True,
    }


def normalize_storage_options(args: Json, metadata: Json | None = None) -> Json:
    metadata = metadata if isinstance(metadata, dict) else optional_object(args, "metadata")
    raw_options = args.get("storage_options")
    options = dict(raw_options) if isinstance(raw_options, dict) else {}
    metadata_options = metadata.get("storage_options") if isinstance(metadata, dict) else None
    if isinstance(metadata_options, dict):
        options = {**metadata_options, **options}
    aliases = {
        "temporalstore_storage_mode": "storage_mode",
        "temporalstore_oplog_mode": "oplog_mode",
        "temporalstore_replication_mode": "replication_mode",
        "temporalstore_raft_mode": "raft_mode",
        "temporalstore_consistency": "consistency",
        "temporalstore_route": "route",
        "temporalstore_storage_family": "storage_family",
        "temporalstore_write_mode": "write_mode",
        "temporalstore_background_write": "background_write",
    }
    for source, target in aliases.items():
        if source in args:
            options[target] = args[source]
        if isinstance(metadata, dict) and source in metadata:
            options.setdefault(target, metadata[source])
    if not options:
        return {}

    allowed = {
        "storage_mode": {"default", "local", "single_node", "multi_node", "shared_store", "raft"},
        "oplog_mode": {"default", "async", "sync"},
        "replication_mode": {"default", "none", "shared_store", "raft"},
        "consistency": {"default", "eventual", "read_your_writes", "linearizable"},
        "route": set(STORAGE_ROUTE_PRESETS),
        "storage_family": {"default", "shared_store", "raft"},
        "family": {"default", "shared_store", "raft"},
        "write_mode": {"default", "async", "sync"},
    }
    route_value = options.get("route")
    if route_value is not None:
        if not isinstance(route_value, str):
            raise MatrixArkError("storage_options.route must be a string")
        route_key = route_value.strip().lower().replace("-", "_")
        if route_key not in STORAGE_ROUTE_PRESETS:
            raise MatrixArkError(f"storage_options.route must be one of {sorted(STORAGE_ROUTE_PRESETS)}")
        options = {**STORAGE_ROUTE_PRESETS[route_key], **options, "route": route_key}

    normalized: Json = {}
    for key, value in options.items():
        if key in {"raft_mode", "background_write"}:
            if not isinstance(value, bool):
                raise MatrixArkError(f"storage_options.{key} must be a boolean")
            normalized[key] = value
            continue
        if key not in allowed:
            normalized[key] = value
            continue
        if not isinstance(value, str):
            raise MatrixArkError(f"storage_options.{key} must be a string")
        compact = value.strip().lower().replace("-", "_")
        if compact not in allowed[key]:
            raise MatrixArkError(f"storage_options.{key} must be one of {sorted(allowed[key])}")
        normalized[key] = compact
    storage_family = normalized.get("storage_family") or normalized.get("family")
    if storage_family == "raft":
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
        normalized["raft_mode"] = True
    elif storage_family == "shared_store":
        normalized.setdefault("replication_mode", "shared_store")
        normalized.setdefault("storage_mode", "shared_store")
        normalized["raft_mode"] = False
    if normalized.get("write_mode") in {"async", "sync"}:
        normalized["oplog_mode"] = normalized["write_mode"]
    if normalized.get("oplog_mode") == "sync" and normalized.get("background_write") is True:
        raise MatrixArkError("storage_options.background_write cannot be true when write_mode/oplog_mode is sync")
    if normalized.get("raft_mode") is True:
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
    route = canonical_storage_route(normalized)
    normalized.update(
        {
            key: value
            for key, value in route.items()
            if key
            in {
                "route",
                "route_key",
                "backend_family",
                "storage_family",
                "write_mode",
                "sync_write",
                "async_write",
                "background_write",
                "write_ack_policy",
                "native_backend_decides_route",
            }
        }
    )
    normalized["request_level"] = True
    return normalized


def text_from_messages(messages: list[Json]) -> str:
    return "\n".join(f"{item['role']}: {item['content']}" for item in messages)


def tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def token_count(text: str) -> int:
    return len(tokens(text))


def clip_context_text(text: str, *, max_chars: int = MAX_CONTEXT_REF_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ...[truncated]"


def embedding_for_text(text: str) -> list[float]:
    model = embedding_model_name()
    cache_key = (model, text)
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        cached = _EMBEDDING_VECTOR_CACHE.get(cache_key)
        if cached is not None:
            return list(cached)
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        vector = oss_embedding_for_text(text)
        with _EMBEDDING_VECTOR_CACHE_LOCK:
            if len(_EMBEDDING_VECTOR_CACHE) >= 8192:
                _EMBEDDING_VECTOR_CACHE.clear()
            _EMBEDDING_VECTOR_CACHE[cache_key] = list(vector)
        return vector
    vector = [0.0] * EMBEDDING_DIM
    for token in tokens(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = digest[0] % EMBEDDING_DIM
        sign = 1.0 if digest[1] % 2 == 0 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        result = vector
    else:
        result = [round(value / norm, 6) for value in vector]
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        if len(_EMBEDDING_VECTOR_CACHE) >= 8192:
            _EMBEDDING_VECTOR_CACHE.clear()
        _EMBEDDING_VECTOR_CACHE[cache_key] = list(result)
    return result


def embeddings_for_texts(texts: list[str]) -> list[list[float]]:
    """Batch-friendly embedding helper with the same cache as embedding_for_text."""
    if not texts:
        return []
    model = embedding_model_name()
    results: list[list[float] | None] = []
    missing: list[tuple[int, str]] = []
    with _EMBEDDING_VECTOR_CACHE_LOCK:
        for index, text in enumerate(texts):
            cached = _EMBEDDING_VECTOR_CACHE.get((model, text))
            if cached is None:
                results.append(None)
                missing.append((index, text))
            else:
                results.append(list(cached))
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if missing and provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )
        try:
            encoder = _OSS_EMBEDDING_MODEL_CACHE.get(model_ref)
            if encoder is None:
                from sentence_transformers import SentenceTransformer  # type: ignore

                encoder = SentenceTransformer(model_ref)
                _OSS_EMBEDDING_MODEL_CACHE[model_ref] = encoder
            vectors = encoder.encode([text for _index, text in missing], normalize_embeddings=True, show_progress_bar=False)
            with _EMBEDDING_VECTOR_CACHE_LOCK:
                if len(_EMBEDDING_VECTOR_CACHE) + len(missing) >= 8192:
                    _EMBEDDING_VECTOR_CACHE.clear()
                for (index, text), vector in zip(missing, vectors):
                    materialized = [round(float(value), 6) for value in vector]
                    _EMBEDDING_VECTOR_CACHE[(model, text)] = list(materialized)
                    results[index] = materialized
        except Exception:
            for index, text in missing:
                results[index] = embedding_for_text(text)
    else:
        for index, text in missing:
            results[index] = embedding_for_text(text)
    return [list(item or []) for item in results]


def embedding_model_name() -> str:
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
            "MATRIXARK_EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )
    return "matrixark-local-token-hash-v1"


def embedding_execution_mode_name() -> str:
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip().lower()
    if embedding_fallback_used():
        return "local_hash_embedding_fallback"
    if provider in {"oss", "open_source", "sentence_transformers", "sentence-transformers"}:
        return "oss_embedding_model"
    if provider == "hash":
        return "hashing-local"
    return "deterministic-token-hash"


def embedding_fallback_used() -> bool:
    return _EMBEDDING_FALLBACK_USED


def oss_embedding_for_text(text: str) -> list[float]:
    global _EMBEDDING_FALLBACK_USED
    model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
        "MATRIXARK_EMBEDDING_MODEL",
        "sentence-transformers/all-MiniLM-L6-v2",
    )
    try:
        encoder = _OSS_EMBEDDING_MODEL_CACHE.get(model_ref)
        if encoder is None:
            from sentence_transformers import SentenceTransformer  # type: ignore

            encoder = SentenceTransformer(model_ref)
            _OSS_EMBEDDING_MODEL_CACHE[model_ref] = encoder
        vector = encoder.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        return [round(float(value), 6) for value in vector]
    except Exception as exc:  # pragma: no cover - depends on optional local model packages.
        if os.environ.get("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "").strip().lower() in {"1", "true", "yes"}:
            raise MatrixArkError(f"OSS embedding model is required but unavailable: {model_ref}: {exc}") from exc
        _EMBEDDING_FALLBACK_USED = True
        previous = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER")
        try:
            os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = "deterministic"
            return embedding_for_text(text)
        finally:
            if previous is None:
                os.environ.pop("MATRIXARK_EMBEDDING_PROVIDER", None)
            else:
                os.environ["MATRIXARK_EMBEDDING_PROVIDER"] = previous


def cosine(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return round(sum(a * b for a, b in zip(left, right)), 6)


def clamp01(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    return max(0.0, min(1.0, number))


def normalized_dense_score(value: float) -> float:
    return clamp01((value + 1.0) / 2.0)


def sparse_lexical_score(query_terms: set[str], text: str) -> float:
    if not query_terms:
        return 0.0
    matched = len(query_terms.intersection(tokens(text)))
    return clamp01(matched / max(len(query_terms), 1))


def infer_query_type(query: str) -> str:
    if understanding_provider() == "oss_encoder":
        return oss_encoder_query_type(query)
    lower = query.lower()
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week|before|after|as of|valid as of)\b", lower):
        return "date"
    if re.search(r"\b(current|currently|latest|now|still|today|valid|status|preference|prefer|likes|where does|where is)\b", lower):
        return "current_state"
    if re.search(r"\b(why|reason|because|feel|felt|emotion|happy|sad|angry|worried|excited)\b", lower):
        return "why_emotion"
    if re.search(r"\b(evidence|quote|exactly|what did .* say|conversation|dialogue|message)\b", lower):
        return "evidence"
    if re.search(r"\b(overview|summarize|summary|explore|broad|what is in|what do we know|topics|map|inventory)\b", lower):
        return "broad_exploration"
    if re.search(r"\b(procedure|steps?|how to|troubleshoot|debug|rollback|runbook|playbook|checklist|fix|remediate|mitigate)\b", lower):
        return "procedure"
    if re.search(r"\b(both|together|across|between|compare|combine|sessions|multi-hop|multi session|multi-session)\b", lower):
        return "multi_hop"
    return "fact"


RESOURCE_TYPE_QUERY_ALIASES: dict[str, str] = {
    "pdf": "pdf",
    "markdown": "md",
    "md": "md",
    "readme": "md",
    "text": "txt",
    "txt": "txt",
    "csv": "csv",
    "tsv": "tsv",
    "excel": "xlsx",
    "xlsx": "xlsx",
    "spreadsheet": "xlsx",
    "html": "html",
    "webpage": "html",
    "docx": "docx",
    "word": "docx",
    "pptx": "pptx",
    "slides": "pptx",
    "deck": "pptx",
}

UNIT_KIND_QUERY_ALIASES: dict[str, str] = {
    "paragraph": "paragraph",
    "passage": "paragraph",
    "heading": "heading",
    "section": "heading",
    "table": "table_row_group",
    "row": "table_row_group",
    "rows": "table_row_group",
    "sheet": "table_row_group",
    "page": "page",
    "slide": "slide",
    "slides": "slide",
    "function": "code_symbol",
    "class": "code_symbol",
    "symbol": "code_symbol",
}

QUERY_INDEX_STOPWORDS = {
    "what", "which", "where", "when", "who", "why", "how", "does", "did", "the", "and", "for",
    "from", "with", "that", "this", "into", "about", "show", "give", "list", "find", "current",
    "latest", "now", "need", "needs", "using", "use", "tool", "skill", "resource", "document", "file",
}


def slug_candidates_from_query(query: str) -> list[str]:
    lower = query.lower()
    candidates: list[str] = []
    for pattern in [
        r"(?:heading|section|chapter)\s+['\"]?([a-z0-9][a-z0-9 _./:-]{1,80})",
        r"#\s*([a-z0-9][a-z0-9 _./:-]{1,80})",
    ]:
        for match in re.finditer(pattern, lower):
            raw_value = re.split(r"\b(?:in|from|for|about|with|under)\b", match.group(1).split("?")[0], maxsplit=1)[0]
            value = normalized_index_value(raw_value)
            if value:
                candidates.append(value)
    return ordered_unique(candidates)[:4]


def path_candidates_from_query(query: str) -> list[str]:
    values: list[str] = []
    for raw in re.findall(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+|[a-zA-Z0-9_.-]+\.(?:md|txt|pdf|csv|tsv|json|jsonl|yaml|yml|html|docx|pptx|xlsx|py|js|ts|go|rs|cpp|h)", query):
        normalized = normalized_index_value(raw)
        if normalized:
            values.append(normalized)
    return ordered_unique(values)[:6]


def keyword_candidates_from_query(query: str) -> list[str]:
    values = []
    for term in tokens(query):
        if len(term) < 4 or term in QUERY_INDEX_STOPWORDS:
            continue
        values.append(context_index_name("keyword", term))
    return ordered_unique(values)[:8]


def infer_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    if understanding_provider() == "oss_encoder":
        return oss_encoder_secondary_index_filter_groups(query, question_type)
    lower = query.lower()
    groups: list[set[str]] = []

    def add_group(*terms: str) -> None:
        clean = {term for term in terms if term}
        if clean and clean not in groups:
            groups.append(clean)

    if re.search(r"\b(where|location|located|moved|moving|live|lives|city|home|staying)\b", lower):
        location_terms = [context_index_name("entity_type", "location")]
        if question_type == "date" or re.search(r"\b(before|after|as of|used to|previously|formerly)\b", lower):
            location_terms.append(context_index_name("source_type", "message"))
        add_group(*location_terms)
    if re.search(r"\b(prefer|preference|favorite|like|likes|love|loves)\b", lower):
        add_group(context_index_name("entity_type", "preference"), context_index_name("event_type", "preference_update"))
    if re.search(r"\b(friend|partner|mother|father|sister|brother|wife|husband|manager|teammate|relationship|family|child|children|son|daughter|pet)\b", lower):
        add_group(context_index_name("entity_type", "relationship"), context_index_name("entity_type", "family_profile"))
    if re.search(r"\b(job|role|work|works|position|status|company|employer)\b", lower):
        add_group(context_index_name("entity_type", "job_status"), context_index_name("event_type", "status_update"))
    if re.search(r"\b(plan|plans|planning|going to|schedule|next)\b", lower):
        add_group(context_index_name("entity_type", "current_plan"), context_index_name("event_type", "plan_update"))
    if re.search(r"\b(approval|approved|approve|confirmed|confirmation|budget|purchase|cost|gpu)\b", lower):
        add_group(
            context_index_name("event_type", "confirmation"),
            context_index_name("event_type", "resource_approval_fact"),
            context_index_name("entity_type", "approval_state"),
            context_index_name("entity_type", "confirmation"),
            context_index_name("entity_type", "resource_fact"),
            context_index_name("classification", "confirmation"),
            context_index_name("classification", "resource_fact"),
            context_index_name("segment_topic", "approval_budget"),
            context_index_name("source_type", "resource"),
            context_index_name("source_type", "resource_fact"),
        )
    if re.search(r"\b(correction|corrected|wrong|instead|updated|changed)\b", lower):
        add_group(
            context_index_name("event_type", "correction"),
            context_index_name("entity_type", "correction"),
            context_index_name("classification", "correction"),
            context_index_name("segment_topic", "correction"),
        )
    if re.search(r"\b(resource|document|doc|file|pdf|markdown|readme|csv|spreadsheet|excel|html|word|slides?|deck)\b", lower):
        add_group(context_index_name("source_type", "resource"), context_index_name("source_type", "resource_fact"))
    for alias, resource_type in RESOURCE_TYPE_QUERY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            add_group(context_index_name("resource_type", resource_type))
    for alias, unit_kind in UNIT_KIND_QUERY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            extra_unit_terms = [context_index_name("unit_kind", unit_kind)]
            if unit_kind == "heading":
                extra_unit_terms.append(context_index_name("unit_kind", "markdown_section"))
            if unit_kind == "paragraph":
                extra_unit_terms.append(context_index_name("unit_kind", "text_paragraph"))
            add_group(*extra_unit_terms)
    heading_terms = [context_index_name("heading_slug", slug) for slug in slug_candidates_from_query(query)]
    if heading_terms:
        add_group(*heading_terms)
    path_terms = [context_index_name("relative_path", path) for path in path_candidates_from_query(query)]
    if path_terms:
        add_group(*path_terms)
    keyword_terms = keyword_candidates_from_query(query)
    if keyword_terms and re.search(r"\b(resource|document|doc|file|pdf|markdown|readme|csv|spreadsheet|excel|html|word|slides?|deck|skill|tool|section|heading)\b", lower):
        add_group(*keyword_terms)
    if re.search(r"\b(skill|tool|playbook|procedure|instruction|capability)\b", lower):
        add_group(context_index_name("source_type", "skill"))
        tool_terms = [context_index_name("skill_tool", term) for term in tokens(query) if term.startswith("matrixark_") or term in {"replay", "audit", "retrieve", "ingest"}]
        query_tokens = [term for term in tokens(query) if len(term) >= 4 and term not in QUERY_INDEX_STOPWORDS]
        trigger_values: list[str] = []
        for size in (3, 2):
            trigger_values.extend("_".join(query_tokens[index : index + size]) for index in range(0, max(0, len(query_tokens) - size + 1)))
        trigger_values.extend(query_tokens)
        trigger_terms = [context_index_name("skill_trigger", term) for term in ordered_unique(trigger_values)]
        if tool_terms:
            add_group(*tool_terms[:6])
        if trigger_terms:
            add_group(*trigger_terms[:24])
    if question_type == "evidence":
        add_group(context_index_name("source_type", "message"), context_index_name("source_type", "feedback"))
    return groups


def secondary_filter_terms_to_fields(groups: list[set[str]]) -> Json:
    fields: Json = {}
    for group in groups:
        for term in sorted(group):
            if ":" not in term:
                continue
            field, value = term.split(":", 1)
            if not field or not value:
                continue
            fields.setdefault(field, [])
            if value not in fields[field]:
                fields[field].append(value)
    return fields


def infer_temporal_window(query: str, question_type: str, *, reference_time_ms: int) -> Json:
    lower = query.lower()
    if re.search(r"\b(current|currently|latest|now|still|today|valid)\b", lower) or question_type == "current_state":
        return {"mode": "latest", "valid_as_of": "now", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(before|prior to|earlier than)\b", lower):
        return {"mode": "before", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(after|since|later than)\b", lower):
        return {"mode": "after", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(as of|valid as of|on)\b", lower):
        return {"mode": "valid_as_of", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(yesterday|tomorrow|last week|next week|last month|next month|last year|next year)\b", lower):
        return {"mode": "relative", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    return {"mode": "unbounded", "valid_as_of": "not_applicable", "reference_time_ms": reference_time_ms}


def build_structured_query_plan(
    query: str,
    *,
    question_type: str,
    secondary_index_filter_groups: list[set[str]],
    secondary_index_filter_mode: str,
    reference_time_ms: int,
) -> Json:
    secondary_filters = secondary_filter_terms_to_fields(secondary_index_filter_groups)
    return {
        "query_type": question_type,
        "secondary_filters": secondary_filters,
        "secondary_filter_groups": [sorted(group) for group in secondary_index_filter_groups],
        "secondary_filter_mode": secondary_index_filter_mode,
        "temporal_window": infer_temporal_window(query, question_type, reference_time_ms=reference_time_ms),
        "execution_order": [
            "query_understanding",
            "scope_filter",
            "secondary_index_prefilter",
            "l0_l1_node_traversal",
            "leaf_candidate_fetch",
            "embedding_similarity_time_decay_business_score",
            "budget_pack_contextpack",
        ],
    }


def oss_encoder_query_type(query: str) -> str:
    ranked = oss_encoder_rank_labels(query, QUERY_TYPE_LABELS, limit=2)
    if not ranked:
        return "fact"
    top = str(ranked[0]["label"])
    if len(ranked) > 1 and top == "fact" and float(ranked[1]["score"]) >= float(ranked[0]["score"]) - 0.015:
        return str(ranked[1]["label"])
    return top


def oss_encoder_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    ranked = oss_encoder_rank_labels(f"{question_type}: {query}", QUERY_INDEX_LABELS, limit=5)
    selected = [str(item["label"]) for item in ranked if float(item["score"]) >= 0.46]
    if not selected and ranked:
        selected = [str(ranked[0]["label"])]
    groups: list[set[str]] = []
    by_prefix: dict[str, set[str]] = {}
    for label in selected:
        prefix = label.split(":", 1)[0]
        by_prefix.setdefault(prefix, set()).add(label)
    for labels in by_prefix.values():
        if labels and labels not in groups:
            groups.append(labels)
    return groups[:4]


def candidate_index_terms(
    record: Json,
    index_terms_by_batch: dict[Any, list[str]],
    index_terms_by_node: dict[Any, list[str]],
    index_terms_by_ref: dict[Any, list[str]] | None = None,
) -> set[str]:
    terms: set[str] = set()
    index_terms_by_ref = index_terms_by_ref or {}
    record_type = record.get("record_type")
    if record_type == "context_event":
        terms.update(index_terms_by_batch.get(record.get("batch_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        extraction = record.get("internal_extraction", {})
        envelope = record.get("envelope", {})
        terms.add(context_index_name("event_type", extraction.get("event_type")))
        if not require_oss_understanding():
            terms.add(context_index_name("event_type", infer_event_type(str(record.get("text", "")))))
        classification = non_default_classification(extraction.get("classification"))
        if classification:
            terms.add(context_index_name("classification", classification))
        terms.add(context_index_name("status", extraction.get("status") or "observed"))
        terms.add(context_index_name("source_type", envelope.get("kind") or "message"))
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
    elif record_type == "resource_chunk":
        terms.update(index_terms_by_ref.get(record.get("chunk_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "resource"))
        terms.add(context_index_name("resource_type", record.get("resource_type")))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    elif record_type == "skill_manifest":
        terms.update(index_terms_by_ref.get(record.get("skill_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.add(context_index_name("skill_name", record.get("name")))
        for trigger in record.get("triggers", [])[:8]:
            terms.add(context_index_name("skill_trigger", trigger))
        for tool in record.get("allowed_tools", [])[:8]:
            terms.add(context_index_name("skill_tool", tool))
    elif record_type == "skill_section":
        terms.update(index_terms_by_ref.get(record.get("section_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    return {term for term in terms if term}


def passes_secondary_index_filters(candidate_terms: set[str], required_groups: list[set[str]], *, mode: str = "all_groups") -> bool:
    if not required_groups:
        return True
    if mode == "any_group":
        return any(bool(candidate_terms.intersection(group)) for group in required_groups)
    return all(bool(candidate_terms.intersection(group)) for group in required_groups)


def passes_applicable_secondary_index_filters(
    candidate_terms: set[str],
    required_groups: list[set[str]],
    *,
    mode: str = "all_groups",
) -> bool:
    """Apply only filter groups whose index prefix is present on this candidate."""
    candidate_prefixes = {term.split(":", 1)[0] for term in candidate_terms if ":" in term}
    candidate_is_context_asset = bool(
        candidate_terms.intersection({"source_type:resource", "source_type:skill"})
    )
    applicable_groups = [
        group
        for group in required_groups
        if candidate_prefixes.intersection({term.split(":", 1)[0] for term in group if ":" in term})
        and not (
            candidate_is_context_asset
            and {term.split(":", 1)[0] for term in group if ":" in term} == {"source_type"}
            and not candidate_terms.intersection(group)
        )
    ]
    return passes_secondary_index_filters(candidate_terms, applicable_groups, mode=mode)



def hybrid_origin_score(query_terms: set[str], text: str, embedding_score: float, node_score: float) -> float:
    dense = normalized_dense_score(embedding_score)
    sparse = sparse_lexical_score(query_terms, text)
    node = normalized_dense_score(node_score)
    return round(clamp01(0.55 * dense + 0.35 * sparse + 0.10 * node), 6)


def time_decay_score(
    record_time_ms: Any,
    *,
    reference_time_ms: int,
    freshness_tolerance_ms: int,
    half_life_ms: int,
) -> float:
    try:
        event_time_ms = int(record_time_ms)
    except (TypeError, ValueError):
        return 0.5
    age_ms = max(0, reference_time_ms - event_time_ms)
    if age_ms <= freshness_tolerance_ms:
        return 1.0
    decay_age = age_ms - freshness_tolerance_ms
    half_life_ms = max(1, half_life_ms)
    # Fast initial decay, then slower long-tail decay for durable memories.
    return round(math.exp(-math.sqrt(decay_age / half_life_ms)), 6)


def business_instance_weight(*sources: Json) -> float | None:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for field in ["business_weight", "business_score", "importance", "priority"]:
            if field in source:
                return clamp01(source.get(field))
    return None


def business_type_score(type_name: str, type_weights: Json) -> float:
    if not type_name:
        return 0.5
    normalized = type_name.lower()
    if normalized in type_weights:
        return clamp01(type_weights[normalized], 0.5)
    if "approval" in normalized or "budget" in normalized:
        return 0.9
    if "correction" in normalized or "confirmation" in normalized:
        return 1.0
    if "preference" in normalized or "plan" in normalized or "status" in normalized:
        return 0.75
    return 0.5


def business_score_for_candidate(candidate: Json, type_weights: Json) -> float:
    instance = business_instance_weight(candidate, candidate.get("metadata", {}), candidate.get("scope", {}))
    if instance is not None:
        return instance
    type_name = str(
        candidate.get("event_type")
        or candidate.get("entity_type")
        or candidate.get("topic")
        or candidate.get("ref_type")
        or ""
    )
    return business_type_score(type_name, type_weights)


def final_recall_score(origin_score: float, time_score: float, business_score: float, weights: Json) -> float:
    time_weight = clamp01(weights.get("time", DEFAULT_TIME_WEIGHT), DEFAULT_TIME_WEIGHT)
    business_weight = clamp01(weights.get("business", DEFAULT_BUSINESS_WEIGHT), DEFAULT_BUSINESS_WEIGHT)
    if time_weight + business_weight > 1.0:
        scale = 1.0 / (time_weight + business_weight)
        time_weight *= scale
        business_weight *= scale
    origin_weight = 1.0 - time_weight - business_weight
    return round(
        origin_weight * origin_score + time_weight * time_score + business_weight * business_score,
        6,
    )


def integer_arg(data: Json, field: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(field, default)
    if not isinstance(value, int):
        raise MatrixArkError(f"{field} must be an integer")
    if value < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    return value


def bounded_max_children_scored_per_parent(value: int) -> int:
    hard_cap = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
    if value > hard_cap:
        raise MatrixArkError(
            "max_children_scored_per_parent must be <= "
            f"{hard_cap}; split over-wide ContextNode children into deeper node layers"
        )
    return value


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    freshness_tolerance_ms = integer_arg(
        ranking,
        "freshness_tolerance_ms",
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        minimum=0,
    )
    half_life_ms = integer_arg(
        ranking,
        "half_life_ms",
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        minimum=1,
    )
    type_weights = {**DEFAULT_BUSINESS_TYPE_WEIGHTS, **optional_object(ranking, "business_type_weights")}
    weights = optional_object(ranking, "weights")
    origin_score = clamp01(candidate.get("origin_score"))
    s_time = time_decay_score(
        candidate.get("updated_at_ms"),
        reference_time_ms=reference_time_ms,
        freshness_tolerance_ms=freshness_tolerance_ms,
        half_life_ms=half_life_ms,
    )
    s_busi = business_score_for_candidate(candidate, type_weights)
    final_score = final_recall_score(origin_score, s_time, s_busi, weights)
    return {
        **candidate,
        "origin_score": origin_score,
        "time_score": s_time,
        "business_score": s_busi,
        "final_score": final_score,
        "score": final_score,
        "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    }


def numeric_field(record: Json, field: str = "value") -> float | None:
    for source in [record, record.get("metadata", {}), record.get("envelope", {}).get("metadata", {})]:
        if not isinstance(source, dict) or field not in source:
            continue
        try:
            return float(source[field])
        except (TypeError, ValueError):
            return None
    return None


def apply_statistical_operator(operator: str, records: list[Json], *, field: str = "value") -> float | int | None:
    values = [value for record in records if (value := numeric_field(record, field)) is not None]
    op = operator.upper()
    if op == "COUNT":
        return len(records)
    if not values:
        return None
    if op == "SUM":
        return round(sum(values), 6)
    if op == "AVG":
        return round(sum(values) / len(values), 6)
    if op == "MAX":
        return max(values)
    raise MatrixArkError(f"unsupported statistical operator: {operator}")


def latest_record(records: list[Json], *, time_field: str = "updated_at_ms") -> Json | None:
    if not records:
        return None
    return max(records, key=lambda record: int(record.get(time_field) or 0))


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    selected: list[Json] = []
    seen: set[tuple[str, Any]] = set()

    def take(items: list[Json], limit: int) -> None:
        for item in items:
            key = (str(item.get("ref_type", "")), item.get("ref_hash"))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= limit:
                return

    auxiliary_quota = max(0, min(auxiliary_quota, total_limit))
    primary_quota = max(0, total_limit - auxiliary_quota)
    take(primary, primary_quota)
    take(auxiliary, total_limit)
    if len(selected) < total_limit:
        take(primary, total_limit)
    return selected[:total_limit]


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    ref_type = str(candidate.get("ref_type", ""))
    context_class = str(candidate.get("context_class") or ref_type)
    text = str(candidate.get("text", "")).lower()
    event_type = str(candidate.get("event_type") or candidate.get("entity_type") or candidate.get("topic") or "").lower()
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    if question_type == "procedure":
        if ref_type == "skill_section":
            return 0.36
        if context_class in {"resource_fact", "resource_entity_fact"} and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", event_type + " " + text):
            return 0.34
        if ref_type == "resource_chunk" and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", text):
            return 0.30
        return 0.0
    if question_type == "broad_exploration":
        if ref_type == "summary":
            return 0.35
        if ref_type in {"segment", "compression"}:
            return 0.16
        return 0.02 if ref_type in {"resource_chunk", "event", "entity"} else 0.0
    if ref_type == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        return 0.32 if source_count >= 2 else 0.18
    if question_type == "current_state":
        if ref_type == "entity":
            return 0.30
        if context_class == "resource_entity_fact":
            return 0.28
        if context_class == "resource_fact" or "correction" in event_type or "confirmation" in event_type:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.10
        return 0.0
    if question_type == "evidence":
        if ref_type == "resource_chunk" and has_citation:
            return 0.30
        if ref_type == "event":
            return 0.24
        return 0.05 if ref_type == "segment" else 0.0
    if question_type == "date":
        if ref_type == "event" and re.search(r"\b(20\d{2}|19\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|monday|tuesday|wednesday|thursday|friday|saturday|sunday|before|after|on)\b", text):
            return 0.28
        return 0.08 if ref_type == "entity" else 0.0
    if question_type == "multi_hop":
        return 0.14 if ref_type in {"entity", "segment"} else 0.04
    if question_type == "why_emotion":
        return 0.18 if re.search(r"\b(because|reason|felt|feel|happy|sad|angry|worried|excited|concerned)\b", text) else 0.0
    if question_type == "fact":
        negated_approval = bool(
            re.search(r"\b(no|not|without|missing|lacks?|lacked)\b.{0,48}\b(approval|approved|decision)\b", text)
            or re.search(r"\b(approval|approved|decision)\b.{0,48}\b(no|not|without|missing|lacks?|lacked)\b", text)
        )
        affirmative_approval = bool(re.search(r"\b(approved|approval granted|approval confirmed|confirmed approval)\b", text))
        if negated_approval and not affirmative_approval:
            return -0.12
        if affirmative_approval:
            return 0.38 if ref_type in {"event", "entity"} else 0.26
        if context_class in {"resource_fact", "resource_entity_fact"}:
            return 0.30
        if ref_type in {"entity", "event"}:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.06
        return 0.03
    return 0.0


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, float, float]:
    score = float(candidate.get("score", 0.0))
    boosted = clamp01(score + question_type_ref_boost(candidate, question_type))
    token_efficiency = boosted / max(1, token_count(str(candidate.get("text", ""))))
    return (boosted, token_efficiency, score)


def context_text_hashes(text: str) -> set[int]:
    compact = " ".join(str(text).split())
    variants = {compact[:512]}
    without_role = re.sub(r"^(user|assistant|tool|system):\s*", "", compact, flags=re.IGNORECASE)
    variants.add(without_role[:512])
    tokenized = tokens(compact)
    if tokenized:
        variants.add(" ".join(tokenized)[:512])
        if tokenized[0] in {"user", "assistant", "tool", "system"}:
            variants.add(" ".join(tokenized[1:])[:512])
    return {stable_hash(variant) for variant in variants if variant}


def local_context_budget(args: Json) -> Json:
    raw_items = args.get("local_context", [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise MatrixArkError("local_context must be an array")
    items: list[Json] = []
    text_hashes: set[int] = set()
    token_total = 0
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            text = item
            source = f"local:{index}"
            ref_type = "local_context"
        elif isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or "")
            source = str(item.get("source") or item.get("ref") or f"local:{index}")
            ref_type = str(item.get("ref_type") or "local_context")
        else:
            raise MatrixArkError("local_context items must be strings or objects")
        text = clip_context_text(text)
        if not text:
            continue
        item_tokens = token_count(text)
        token_total += item_tokens
        text_hashes.update(context_text_hashes(text))
        items.append(
            {
                "ref_type": ref_type,
                "source": source,
                "text": text,
                "token_estimate": item_tokens,
                "text_hash": stable_hash(text[:512]),
            }
        )
    explicit_tokens = args.get("local_context_tokens")
    token_source = "estimated_from_local_context"
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
        token_source = "agent_provided_local_context_tokens"
    raw_safety_margin = args.get("local_context_safety_margin_tokens")
    if raw_safety_margin is None:
        raw_safety_margin = os.environ.get("MATRIXARK_LOCAL_CONTEXT_SAFETY_MARGIN_TOKENS")
    if raw_safety_margin is None:
        raw_max_context = args.get("max_context_tokens", 2048)
        try:
            max_context_tokens = max(0, int(raw_max_context or 2048))
        except (TypeError, ValueError):
            max_context_tokens = 2048
        safety_margin_tokens = min(128, max_context_tokens // 20)
        safety_margin_source = "matrixark_default_5_percent_capped"
    else:
        try:
            safety_margin_tokens = int(raw_safety_margin or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
        safety_margin_source = "agent_provided_safety_margin" if "local_context_safety_margin_tokens" in args else "env_safety_margin"
    if safety_margin_tokens < 0:
        raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
        "token_source": token_source,
        "safety_margin_tokens": safety_margin_tokens,
        "safety_margin_source": safety_margin_source,
    }


def compact_local_context_refs(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
            }
        )
    return refs


def local_context_refs_for_pack(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
                "text": item.get("text", ""),
                "selection_reason": "provided by agent-visible local context before MatrixArk remote retrieval",
            }
        )
    return refs


def serving_ref_for_pack(ref: Json) -> Json:
    """Return only answer-bearing fields for the serving ContextPack payload."""
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    item: Json = {
        "ref_type": ref.get("ref_type", ""),
        "text": ref.get("text", ""),
        "token_estimate": ref.get("token_estimate", 0),
    }
    optional_fields = [
        "context_class",
        "source_ref",
        "citation",
        "resource_type",
        "unit_kind",
        "heading",
        "heading_slug",
        "relative_path",
        "source_locator",
        "entity_type",
        "entity_name",
        "operator",
        "summary_type",
        "resource_version",
        "version_state",
    ]
    for field in optional_fields:
        value = ref.get(field, metadata.get(field))
        if value not in (None, "", [], {}):
            item[field] = value
    return item


def serving_refs_for_pack(refs: list[Json]) -> list[Json]:
    return [serving_ref_for_pack(ref) for ref in refs]


def compact_context_pack_for_serving(pack: Json) -> Json:
    """Strip planner/audit/debug fields from the default returned ContextPack.

    Full retrieval policy, score details, dropped refs, storage mode, model
    fallback flags, and operational visibility live in ContextPackAudit or
    telemetry records when enabled. The serving pack should spend tokens on
    evidence and citations.
    """
    serving_keys = [
        "context_pack_id",
        "context_sources_order",
        "local_context_refs",
        "selected_refs",
        "remote_context_refs",
        "selected_ref_counts",
        "context_assembly_policy",
        "used_context_tokens",
        "used_remote_context_tokens",
        "used_local_context_tokens",
        "total_prompt_context_tokens",
        "remote_context_budget_tokens",
        "requested_max_context_tokens",
        "local_context_safety_margin_tokens",
        "budget_source",
        "quality_warnings",
        "insufficient_context",
        "partial_context_pack",
    ]
    compact = {key: pack[key] for key in serving_keys if key in pack}
    selected_refs = pack.get("selected_refs", [])
    if isinstance(selected_refs, list):
        compact["selected_refs"] = serving_refs_for_pack(selected_refs)
        compact["remote_context_refs"] = compact["selected_refs"]
    local_refs = pack.get("local_context_refs", [])
    if isinstance(local_refs, list):
        compact["local_context_refs"] = [
            {
                key: value
                for key, value in ref.items()
                if key in {"ref_type", "source", "token_estimate", "text"}
                and value not in (None, "", [], {})
            }
            for ref in local_refs
            if isinstance(ref, dict)
        ]
    if "context_assembly_policy" in compact and isinstance(compact["context_assembly_policy"], dict):
        compact["context_assembly_policy"] = {
            "skill_selection": compact["context_assembly_policy"].get("skill_selection", "skill_section_only"),
            "resource_selection": "ranked_facts_entities_chunks",
        }
    return compact


def is_resource_or_skill_candidate(candidate: Json) -> bool:
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or "")
    return ref_type in {"resource_chunk", "skill_section"} or context_class in {"resource_fact", "resource_entity_fact"}


def dropped_candidate_audit_ref(candidate: Json, *, reason: str, token_estimate: int) -> Json:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    return {
        "ref_type": candidate.get("ref_type", ""),
        "ref_hash": candidate.get("ref_hash"),
        "context_class": candidate.get("context_class") or candidate.get("ref_type", ""),
        "drop_reason": reason,
        "reason": reason,
        "score": candidate.get("score", 0.0),
        "origin_score": candidate.get("origin_score", 0.0),
        "packing_score": round(packing_sort_key(candidate, str(candidate.get("packing_policy") or "fact"))[0], 6),
        "token_estimate": token_estimate,
        "token_cost": token_estimate,
        "raw_uri": candidate.get("raw_uri", ""),
        "source_ref": candidate.get("source_ref", ""),
        "citation": candidate.get("citation") or candidate.get("source_ref", ""),
        "resource_type": candidate.get("resource_type", ""),
        "resource_version": candidate.get("resource_version") or metadata.get("resource_version", ""),
        "version_state": candidate.get("version_state", "current"),
        "stale_or_superseded": bool(candidate.get("stale_or_superseded", False)),
        "access_decision": candidate.get("access_decision", "allowed_by_scope"),
        "selection_reason": candidate.get("selection_reason", ""),
        "matched_index_terms": candidate.get("matched_index_terms", []),
        "node_hash": candidate.get("node_hash"),
        "node_path": candidate.get("node_path", []),
    }


def record_dropped_candidate(dropped: Json, candidate: Json, *, reason: str, token_estimate: int) -> None:
    if not is_resource_or_skill_candidate(candidate):
        return
    dropped.setdefault("refs", []).append(dropped_candidate_audit_ref(candidate, reason=reason, token_estimate=token_estimate))


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    if question_type != "multi_hop":
        return candidates[:total_limit]
    selected: list[Json] = []
    deferred: list[Json] = []
    seen_nodes: set[Any] = set()
    for candidate in candidates:
        node_hash = candidate.get("node_hash")
        if node_hash not in seen_nodes:
            selected.append(candidate)
            seen_nodes.add(node_hash)
        else:
            deferred.append(candidate)
        if len(selected) >= total_limit:
            return selected
    selected.extend(deferred)
    return selected[:total_limit]


def select_token_budgeted_refs(
    primary: list[Json],
    auxiliary: list[Json],
    *,
    max_context_tokens: int,
    auxiliary_quota: int,
    question_type: str = "fact",
    reserved_tokens: int = 0,
    max_selected_refs: int | None = None,
    duplicate_text_hashes: set[int] | None = None,
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    selected_ref_cap = max(1, int(max_selected_refs or max(8, min(256, max_context_tokens))))
    candidate_pool_limit = max(selected_ref_cap, max(8, min(256, max_context_tokens)))
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=candidate_pool_limit)
    selected: list[Json] = []
    used_tokens = 0
    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "deadline": "candidate was not packed because the hard retrieval deadline was reached",
            "max_selected_refs": "candidate was relevant but dropped because max_selected_refs was reached",
        },
        "refs": [],
        "deadline_exceeded": False,
        "deadline_reason": "",
    }
    seen_text_hashes: set[int] = set()
    for index, candidate in enumerate(candidates):
        if len(selected) >= selected_ref_cap:
            remaining_candidates = candidates[index:]
            dropped["max_selected_refs"] += len(remaining_candidates)
            for skipped in remaining_candidates:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["max_selected_refs"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="max_selected_refs", token_estimate=skipped_tokens)
            break
        if deadline_exceeded is not None and deadline_exceeded():
            dropped["deadline_exceeded"] = True
            dropped["deadline_reason"] = deadline_reason
            remaining = max(0, len(candidates) - index)
            dropped["deadline"] += remaining
            for skipped in candidates[index:]:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["deadline"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="deadline", token_estimate=skipped_tokens)
            break
        ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
        candidate_text_hashes = context_text_hashes(str(candidate.get("text", "")))
        if candidate_text_hashes.intersection(duplicate_text_hashes):
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        text_hash = stable_hash(str(candidate.get("text", ""))[:512])
        if text_hash in seen_text_hashes:
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        if float(candidate.get("score", 0.0)) < 0.04:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if remote_budget <= 0 or (selected and used_tokens + ref_tokens > remote_budget):
            dropped["over_budget"] += 1
            dropped["estimated_tokens"]["over_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="over_budget", token_estimate=ref_tokens)
            continue
        seen_text_hashes.add(text_hash)
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
            }
        )
        used_tokens += ref_tokens
        if used_tokens >= remote_budget:
            break
    if not selected and candidates and remote_budget > 0:
        first = next(
            (
                candidate
                for candidate in candidates
                if not context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes)
            ),
            None,
        )
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = tokens(str(first.get("text", "")))[:remote_budget]
        selected = [{**first, "text": " ".join(clipped_words), "token_estimate": len(clipped_words)}]
        used_tokens = len(clipped_words)
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped


def selected_context_class_counts(refs: list[Json]) -> Json:
    counts: Json = {
        "event": 0,
        "entity": 0,
        "segment": 0,
        "compression": 0,
        "resource_fact": 0,
        "resource_entity_fact": 0,
        "resource_chunk": 0,
        "skill_section": 0,
        "summary": 0,
    }
    for ref in refs:
        context_class = str(ref.get("context_class") or ref.get("ref_type") or "")
        counts[context_class] = int(counts.get(context_class, 0)) + 1
    return counts


def compact_refs_for_audit(refs: list[Json], *, preview_chars: int = 160) -> list[Json]:
    compact: list[Json] = []
    keep_fields = [
        "ref_type",
        "ref_hash",
        "node_hash",
        "node_path",
        "scope",
        "recall_path",
        "score",
        "origin_score",
        "time_score",
        "business_score",
        "embedding_score",
        "sparse_score",
        "keyword_score",
        "token_estimate",
        "updated_at_ms",
        "selection_reason",
        "matched_index_terms",
        "resource_hash",
        "raw_uri_hash",
        "source_locator",
        "resource_type",
        "resource_version",
        "context_class",
        "source_chunk_hash",
        "access_decision",
        "access_scope",
        "deployment_scope",
        "version_state",
        "stale_or_superseded",
        "citation",
        "operator",
        "source_start_ms",
        "source_end_ms",
        "source_event_ids",
        "summary_type",
    ]
    metadata_keep_fields = [
        "unit_kind",
        "heading",
        "heading_slug",
        "heading_path",
        "relative_path",
        "keywords",
        "source_locator",
        "resource_version",
        "content_hash",
        "row_start",
        "row_end",
        "record_start",
        "record_end",
        "page",
        "page_section",
        "slide_number",
        "sheet_name",
        "row_count",
        "supersedes_chunk_hash",
        "parse_warnings",
    ]
    for ref in refs:
        item = {field: ref[field] for field in keep_fields if field in ref}
        metadata = ref.get("metadata", {})
        if isinstance(metadata, dict):
            compact_metadata = {field: metadata[field] for field in metadata_keep_fields if field in metadata}
            if compact_metadata:
                item["metadata"] = compact_metadata
        text = str(ref.get("text", ""))
        if text:
            item["text_preview"] = clip_context_text(text, max_chars=preview_chars)
        compact.append(item)
    return compact


def summarize_text(text: str, *, limit: int = 220) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def deterministic_time_compression_summary(
    *,
    node_path: list[str],
    source_start_ms: int,
    source_end_ms: int,
    event_texts: list[str],
    max_raw_events_per_node: int,
) -> str:
    snippets = [summarize_text(text, limit=180) for text in event_texts[:5]]
    return (
        f"Temporal compression window [{source_start_ms}, {source_end_ms}] under "
        f"{' / '.join(node_path)} contains {len(event_texts)} source events. "
        f"Normal retrieval should score this synthesis plus the newest {max_raw_events_per_node} raw events. "
        + " | ".join(snippets)
    )


def time_compression_summary_provider_name() -> str:
    provider = TIME_COMPRESSION_SUMMARY_PROVIDER.replace("-", "_")
    if provider in {"", "local", "rules"}:
        return "deterministic"
    return provider


def generate_time_compression_summary(
    *,
    node_path: list[str],
    source_start_ms: int,
    source_end_ms: int,
    event_texts: list[str],
    max_raw_events_per_node: int,
) -> Json:
    fallback = deterministic_time_compression_summary(
        node_path=node_path,
        source_start_ms=source_start_ms,
        source_end_ms=source_end_ms,
        event_texts=event_texts,
        max_raw_events_per_node=max_raw_events_per_node,
    )
    provider = time_compression_summary_provider_name()
    if provider == "deterministic":
        return {
            "summary": fallback,
            "provider": "deterministic",
            "model": "",
            "fallback_used": False,
        }
    if provider not in {"openai", "openai_compatible", "openai_compatible_llm"}:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"unsupported TIME_COMPRESS summary provider: {provider}")
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": "unsupported_time_compression_summary_provider",
        }
    api_key = os.environ.get(TIME_COMPRESSION_SUMMARY_API_KEY_ENV, "")
    if not api_key:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"{TIME_COMPRESSION_SUMMARY_API_KEY_ENV} is required for TIME_COMPRESS summaries")
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": "missing_time_compression_summary_api_key",
        }
    prompt = (
        "Summarize old LLM context events into a compact replayable memory. "
        "Preserve decisions, entities, dates, constraints, and stale/current status. "
        "Do not invent facts. Return only the summary.\n\n"
        f"Node path: {' / '.join(node_path)}\n"
        f"Source time window: {source_start_ms}..{source_end_ms}\n"
        f"Newest raw events kept outside this summary: {max_raw_events_per_node}\n"
        "Source events:\n"
        + "\n".join(f"- {summarize_text(text, limit=400)}" for text in event_texts[:32])
    )
    payload = {
        "model": TIME_COMPRESSION_SUMMARY_MODEL,
        "messages": [
            {"role": "system", "content": "You write concise, factual memory compression summaries for an LLM context system."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": 512,
    }
    request = urllib.request.Request(
        f"{TIME_COMPRESSION_SUMMARY_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TIME_COMPRESSION_SUMMARY_TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode("utf-8"))
        summary = str(data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        if not summary:
            raise MatrixArkError("TIME_COMPRESS summary provider returned empty content")
        return {
            "summary": summarize_text(summary, limit=1200),
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": False,
        }
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, MatrixArkError, OSError, json.JSONDecodeError) as exc:
        if TIME_COMPRESSION_REQUIRE_LLM_SUMMARY:
            raise MatrixArkError(f"TIME_COMPRESS summary provider failed: {exc}") from exc
        return {
            "summary": fallback,
            "provider": provider,
            "model": TIME_COMPRESSION_SUMMARY_MODEL,
            "fallback_used": True,
            "warning": str(exc),
        }


def estimated_context_tokens(text: str) -> int:
    """Cheap token estimate used for summary policy decisions."""
    compact = " ".join(str(text).split())
    if not compact:
        return 0
    return max(1, (len(compact) + 3) // 4)


def node_l1_generation_policy(
    *,
    source_text: str,
    event_count: int,
    child_summary_count: int,
) -> Json:
    """Decide when a node needs a richer L1 overview.

    L0 is mandatory for traversal. L1 is useful once a node has enough local
    content or child summaries that a short abstract would lose routing detail.
    """
    token_estimate = estimated_context_tokens(source_text)
    base = {
        "token_estimate": token_estimate,
        "event_count": event_count,
        "child_summary_count": child_summary_count,
    }
    if child_summary_count > 0:
        return {**base, "generate_l1": True, "reason": "has_child_summaries"}
    if event_count >= 3:
        return {**base, "generate_l1": True, "reason": "event_count_threshold"}
    if token_estimate >= 180:
        return {**base, "generate_l1": True, "reason": "token_threshold"}
    return {**base, "generate_l1": False, "reason": "l0_sufficient"}


def normalized_node_path(envelope: Json, node_hint: list[Any]) -> list[str]:
    return [str(part) for part in node_hint if str(part)]


def node_prefixes(node_path: list[str]) -> list[list[str]]:
    return [node_path[: index + 1] for index in range(len(node_path))]


def node_path_tuple(node_path: Any) -> tuple[str, ...]:
    if not isinstance(node_path, list):
        return ()
    return tuple(str(part) for part in node_path if str(part))


def starts_with_path(path: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return len(path) >= len(prefix) and path[: len(prefix)] == prefix


def top_scored_nodes(nodes: list[Json], limit: int) -> list[Json]:
    return sorted(
        nodes,
        key=lambda item: (-float(item.get("score", 0.0)), int(item.get("depth", 0)), str(item.get("node_path", []))),
    )[:limit]


def tree_first_traversal(
    node_scores: dict[int, Json],
    *,
    top_k_per_layer: int,
    max_children_scored_per_parent: int,
) -> Json:
    """Traverse ContextNode summaries layer by layer and return selected subtrees.

    The current Python runtime infers ContextNode children from node_path prefixes.
    C++ can later replace this with native ContextChildRef/list-children APIs while
    preserving the retrieval contract.
    """
    node_by_path: dict[tuple[str, ...], Json] = {}
    children_by_parent: dict[tuple[str, ...], list[Json]] = {}
    for node in node_scores.values():
        path = node_path_tuple(node.get("node_path", []))
        if not path:
            continue
        current = node_by_path.get(path)
        if current is None or float(node.get("score", 0.0)) > float(current.get("score", 0.0)):
            node_by_path[path] = node
    for path, node in node_by_path.items():
        parent = path[:-1]
        children_by_parent.setdefault(parent, []).append(node)

    roots = children_by_parent.get((), [])
    if not roots:
        return {
            "selected_node_hashes": set(),
            "selected_paths": set(),
            "leaf_paths": set(),
            "trace": [],
            "fallback_to_flat": True,
        }

    frontier = top_scored_nodes(roots[:max_children_scored_per_parent], top_k_per_layer)
    selected_paths: set[tuple[str, ...]] = set()
    selected_node_hashes: set[int] = set()
    leaf_paths: set[tuple[str, ...]] = set()
    trace: list[Json] = []

    while frontier:
        next_frontier: list[Json] = []
        for node in frontier:
            path = node_path_tuple(node.get("node_path", []))
            if not path:
                continue
            selected_paths.add(path)
            try:
                selected_node_hashes.add(int(node.get("node_hash")))
            except (TypeError, ValueError):
                pass
            children = children_by_parent.get(path, [])[:max_children_scored_per_parent]
            picked_children = top_scored_nodes(children, top_k_per_layer) if children else []
            trace.append(
                {
                    "node_hash": node.get("node_hash"),
                    "node_path": list(path),
                    "depth": node.get("depth", len(path)),
                    "score": node.get("score", 0.0),
                    "dense_score": node.get("dense_score", 0.0),
                    "sparse_score": node.get("sparse_score", 0.0),
                    "children_scored": len(children),
                    "children_selected": len(picked_children),
                    "selected": True,
                }
            )
            if picked_children:
                next_frontier.extend(picked_children)
            else:
                leaf_paths.add(path)
        frontier = next_frontier

    if not leaf_paths:
        leaf_paths = set(selected_paths)
    return {
        "selected_node_hashes": selected_node_hashes,
        "selected_paths": selected_paths,
        "leaf_paths": leaf_paths,
        "trace": trace,
        "fallback_to_flat": False,
    }


def scope_matches(record_scope: Json, query_scope: Json) -> bool:
    if not query_scope:
        return True
    explicit_keys = set(query_scope.get("_explicit_scope_keys", []))
    record_scope_key = str(record_scope.get("scope_key") or "")
    if record_scope_key:
        if not scope_key_matches_query(record_scope_key, query_scope, explicit_keys):
            return False
        if set(record_scope.keys()).issubset({"scope_key"}):
            return True
    for key, value in query_scope.items():
        if str(key).startswith("_"):
            continue
        if key == "scope_key":
            continue
        if key == "agent_name" and key not in record_scope:
            continue
        if key in {"team", "project"} and key not in record_scope and record_scope_key:
            continue
        if key in {"agent_name", "team", "project"} and key not in explicit_keys:
            continue
        if key in {"account_id", "tenant_id", "account_hash", "tenant_hash"} and record_scope_key and key not in record_scope:
            continue
        if key in {"user_id", "user_hash"} and "user_id" not in explicit_keys:
            continue
        if key in {"session_id", "session_hash"} and "session_id" not in explicit_keys:
            continue
        if record_scope.get(key) != value:
            return False
    return True


def candidate_access_scope(record: Json) -> Json:
    access_scope = record.get("access_scope")
    if isinstance(access_scope, dict) and access_scope:
        return access_scope
    metadata = record.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("access_scope"), dict):
        return metadata["access_scope"]
    serving_scope = scope_from_serving_record(record)
    if serving_scope:
        return serving_scope
    envelope = record.get("envelope", {})
    if isinstance(envelope, dict):
        return envelope.get("scope", {})
    return {}


def access_scope_matches_before_scoring(record: Json, query_scope: Json) -> bool:
    """Gate candidate eligibility before semantic scoring."""
    return scope_matches(candidate_access_scope(record), query_scope)
