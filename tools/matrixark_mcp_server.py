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
import socket
import subprocess
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
DIRECT_AUDIT_MODE = os.environ.get("MATRIXARK_DIRECT_AUDIT_MODE", "buffered").strip().lower()
DIRECT_AUDIT_BUFFER_MAX_RECORDS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS", "128"))
DIRECT_AUDIT_FLUSH_INTERVAL_MS = int(os.environ.get("MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS", "1000"))
BACKEND_READINESS_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_TIMEOUT_MS", "30000"))
BACKEND_READINESS_BACKOFF_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_BACKOFF_MS", "200"))
BACKEND_READINESS_CONNECT_TIMEOUT_MS = int(os.environ.get("MATRIXARK_BACKEND_READINESS_CONNECT_TIMEOUT_MS", "1000"))
MAX_CONTEXT_REF_CHARS = 4096
DEFAULT_TIME_DECAY_TOLERANCE_MS = 24 * 60 * 60 * 1000
DEFAULT_TIME_DECAY_HALFLIFE_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TIME_WEIGHT = 0.18
DEFAULT_BUSINESS_WEIGHT = 0.22
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
MATRIXARK_ADMIN_SCOPES = {"admin:account", "admin:tenant", "admin:user", "admin:api_key", "admin:sso", "admin:audit"}
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
    }


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
    for record in reversed(all_records):
        if record.get("record_type") != "context_summary" or record.get("summary_type") != "session_l0":
            continue
        if tuple(record.get("context_node_key", [])) == target_key:
            return record
    return None


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
            if session_key(prior_envelope) == key:
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
            if user_key(prior_envelope) == fallback_key:
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
        summary_hash = record.get("node_hash")
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
                "node_hash": summary_hash,
                "text": text,
            }
        )
        refs.append({"ref_type": "summary", "ref_hash": summary_hash, "node_hash": summary_hash})

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
            raw = oss_model_memory_segments(messages, model=model, model_path=model_path, max_new_tokens=max_new_tokens)
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


def oss_model_memory_segments(messages: list[Json], *, model: str, model_path: str = "", max_new_tokens: int = 512) -> Json:
    try:
        import torch  # type: ignore
        from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional OSS stack.
        raise MatrixArkError("torch and transformers are required for segment_provider=oss") from exc

    target = model_path or model
    cache_key = f"{target}:{max_new_tokens}"
    cached = _OSS_SEGMENT_MODEL_CACHE.get(cache_key)
    if cached is None:
        local_only = bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").lower() in {"1", "true", "yes"}
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


def sanitize_resource_metadata(metadata: Json) -> Json:
    sanitized = {
        key: value
        for key, value in metadata.items()
        if key not in RAW_BYTE_METADATA_FIELDS
    }
    sanitized["parse_warnings"] = normalize_parse_warnings(sanitized)
    sanitized["raw_storage_policy"] = "raw_uri_only"
    sanitized["raw_bytes_stored"] = False
    return sanitized


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
    }


def deployment_scope_from_args(args: Json, envelope: Json) -> str:
    value = str(
        args.get("deployment_scope")
        or envelope.get("metadata", {}).get("deployment_scope")
        or os.environ.get("MATRIXARK_DEPLOYMENT_SCOPE")
        or "local"
    ).strip().lower()
    return value if value in {"local", "global", "cloud", "on_prem", "hybrid"} else "local"


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


def metadata_index_terms(metadata: Json) -> list[str]:
    terms: list[str] = []
    for field in ["unit_kind", "heading_slug", "relative_path"]:
        terms.append(context_index_name(field, metadata.get(field)))
    for keyword in metadata.get("keywords", [])[:12]:
        terms.append(context_index_name("keyword", keyword))
    return ordered_unique(terms)


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
        return matches
    if should_extract_resource_fact(text, metadata):
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
    anchor = str(
        chunk_metadata.get("heading")
        or chunk_metadata.get("heading_slug")
        or chunk_metadata.get("relative_path")
        or chunk_metadata.get("source_ref")
        or raw_uri
    )[:80]
    prefix = str(schema.get("entity_prefix") or schema.get("entity_type") or "fact")
    if value and value != anchor:
        return f"{prefix}:{anchor}:{value[:60]}"
    return f"{prefix}:{anchor}"


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
    }
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
        terms.add(context_index_name("classification", extraction.get("classification")))
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
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
    }


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
    duplicate_text_hashes: set[int] | None = None,
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=max(8, min(256, max_context_tokens)),
        auxiliary_quota=auxiliary_quota,
    )
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=max(8, min(256, max_context_tokens)))
    selected: list[Json] = []
    used_tokens = 0
    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
        },
        "refs": [],
    }
    seen_text_hashes: set[int] = set()
    for candidate in candidates:
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
        "raw_uri",
        "source_ref",
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
        "citation",
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
    for key, value in query_scope.items():
        if str(key).startswith("_"):
            continue
        if key in {"agent_name", "team", "project"} and key not in explicit_keys:
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
    if isinstance(access_scope, dict):
        return access_scope
    metadata = record.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("access_scope"), dict):
        return metadata["access_scope"]
    return record.get("scope", record.get("envelope", {}).get("scope", {}))


def access_scope_matches_before_scoring(record: Json, query_scope: Json) -> bool:
    """Gate candidate eligibility before semantic scoring."""
    return scope_matches(candidate_access_scope(record), query_scope)


@dataclass
class MatrixArkLocalAdapter:
    event_log: Path

    def __post_init__(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        return {
            "status": "ready",
            "backend": "local",
            "reason": reason,
            "probe": bool(probe),
            "attempts": 1,
            "topology": {"mode": "local-jsonl", "event_log": str(self.event_log)},
            "checks": {
                "mcp_process_started": True,
                "namespace_table_opened": True,
                "slot_coverage_verified_by_warmup_hset_hget": True,
            },
        }

    def backend_metrics(self) -> Json:
        return {
            "backend": getattr(self, "_backend_label", lambda: "local")(),
            "metrics_format": "json",
            "metrics": {
                "mode": "local-jsonl",
                "event_log": str(self.event_log),
            },
        }

    def append(self, record: Json) -> None:
        with self.event_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def append_many(self, records: list[Json]) -> None:
        if not records:
            return
        with self.event_log.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")

    def append_audit(self, record: Json) -> None:
        self.append(record)

    def flush_audits(self) -> None:
        return

    def read_all(self) -> list[Json]:
        if not self.event_log.exists():
            return []
        records = []
        with self.event_log.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        for record in reversed(self.read_all()):
            if record.get("record_type") == "context_entity" and record.get("entity_hash") == entity_hash:
                return record
        return None

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        key = session_buffer_key_from_scope(scope)
        committed: set[int] = set()
        records = self.read_all()
        for record in records:
            if record.get("record_type") == "context_batch_commit" and session_buffer_key_from_scope(record.get("scope", {})) == key:
                for ref in record.get("source_event_ids", []):
                    try:
                        committed.add(int(ref))
                    except (TypeError, ValueError):
                        continue
        events: list[Json] = []
        for record in records:
            if record.get("record_type") == "context_event" and session_buffer_key(record.get("envelope", {})) == key:
                try:
                    event_hash = int(record.get("event_id_hash"))
                except (TypeError, ValueError):
                    continue
                if event_hash not in committed:
                    events.append(record)
        if limit is not None:
            return events[:limit]
        return events

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        key = session_buffer_key(envelope)
        self.append(
            {
                "record_type": "session_buffer_event",
                "buffer_key_hash": stable_hash(":".join(key)),
                "buffer_key": list(key),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                "agent_hook": hook,
                "created_at_ms": envelope["ingestion_time_ms"],
            }
        )

    def default_session_node_path(self, scope: Json) -> list[str]:
        tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
        user_id = str(scope.get("user_id") or local_account_user_id())
        session_id = str(scope.get("session_id") or user_id or "default_session")
        return [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}"]

    def ensure_context_node_path(self, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
        prefixes = node_prefixes(node_path)
        if not prefixes:
            return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

        records = self.read_all()
        existing_nodes = {
            int(record.get("node_hash"))
            for record in records
            if record.get("record_type") == "context_node" and record.get("node_hash") is not None
        }
        existing_child_refs = {
            int(record.get("child_ref_hash"))
            for record in records
            if record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None
        }
        node_hashes: list[int] = []
        nodes_created = 0
        child_refs_created = 0
        for prefix in prefixes:
            node_hash = stable_hash("/".join(prefix))
            node_hashes.append(node_hash)
            parent_path = prefix[:-1]
            parent_hash = stable_hash("/".join(parent_path)) if parent_path else 0
            if node_hash not in existing_nodes:
                self.append(
                    {
                        "record_type": "context_node",
                        "node_hash": node_hash,
                        "parent_hash": parent_hash,
                        "node_name": prefix[-1],
                        "node_path": prefix,
                        "depth": len(prefix),
                        "scope": scope,
                        "status": "active",
                        "created_at_ms": updated_at_ms,
                        "updated_at_ms": updated_at_ms,
                    }
                )
                existing_nodes.add(node_hash)
                nodes_created += 1
            if parent_path:
                child_ref_hash = stable_hash(f"child:{parent_hash}:{node_hash}")
                if child_ref_hash not in existing_child_refs:
                    self.append(
                        {
                            "record_type": "context_child_ref",
                            "child_ref_hash": child_ref_hash,
                            "parent_hash": parent_hash,
                            "child_hash": node_hash,
                            "child_name": prefix[-1],
                            "parent_path": parent_path,
                            "child_path": prefix,
                            "depth": len(prefix),
                            "scope": scope,
                            "status": "active",
                            "created_at_ms": updated_at_ms,
                            "updated_at_ms": updated_at_ms,
                        }
                    )
                    existing_child_refs.add(child_ref_hash)
                    child_refs_created += 1
        return {
            "nodes_created": nodes_created,
            "child_refs_created": child_refs_created,
            "node_hashes": node_hashes,
        }

    def session_commit(self, args: Json, *, hook: Json | None = None) -> Json:
        scope = optional_object(args, "scope")
        threshold = args.get("threshold_messages", 20)
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        force = bool(args.get("force", True))
        commit_reason = optional_string(args, "commit_reason") or ("manual_api" if force else "threshold")
        idle_timeout_ms = args.get("idle_timeout_ms")
        if idle_timeout_ms is not None and (not isinstance(idle_timeout_ms, int) or idle_timeout_ms < 0):
            raise MatrixArkError("idle_timeout_ms must be a non-negative integer")
        max_messages = args.get("max_messages")
        if max_messages is not None and (not isinstance(max_messages, int) or max_messages <= 0):
            raise MatrixArkError("max_messages must be a positive integer")
        pending_all = self.pending_session_events(scope)
        pending_event_count = len(pending_all)
        idle_elapsed_ms = 0
        idle_ready = False
        if pending_all and idle_timeout_ms is not None:
            latest_event_time = max(
                int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
                for record in pending_all
            )
            idle_elapsed_ms = max(0, now_ms() - latest_event_time)
            idle_ready = idle_elapsed_ms >= idle_timeout_ms
        threshold_ready = pending_event_count >= threshold
        if not force and not threshold_ready and not idle_ready:
            return {
                "status": "deferred",
                "pending_event_count": pending_event_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "reason": "session buffer below extraction threshold and idle timeout not reached",
            }
        if max_messages is not None:
            commit_limit = max_messages
        elif force or idle_ready:
            commit_limit = None
        else:
            commit_limit = threshold
        pending = pending_all[:commit_limit] if commit_limit is not None else pending_all
        messages = []
        source_event_ids = []
        for record in pending:
            message = message_from_event_record(record)
            if not message:
                continue
            messages.append(message)
            source_event_ids.append(record["event_id_hash"])
        if not messages:
            return {
                "status": "empty",
                "pending_event_count": pending_event_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
            }
        metadata = optional_object(args, "metadata")
        if "node_path" not in metadata:
            metadata = {**metadata, "node_path": self.default_session_node_path(scope)}
        batch_result = self.batch_extract(
            {
                "messages": messages,
                "scope": scope,
                "metadata": metadata,
                "threshold_messages": threshold,
                "force": True,
                "derive_from_existing_events": True,
                "source_event_ids": source_event_ids,
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
            },
            hook=hook,
        )
        commit_id_hash = stable_hash(f"commit:{scope}:{source_event_ids}:{now_ms()}")
        self.append(
            {
                "record_type": "context_batch_commit",
                "commit_id_hash": commit_id_hash,
                "batch_id_hash": batch_result.get("batch_id_hash"),
                "node_hash": batch_result.get("node_hash"),
                "node_path": metadata["node_path"],
                "source_event_ids": source_event_ids,
                "scope": scope,
                "message_count": len(messages),
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
                "pending_event_count_before_commit": pending_event_count,
                "committed_event_count": len(source_event_ids),
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        return {
            **batch_result,
            "status": "committed",
            "commit_id_hash": commit_id_hash,
            "pending_event_count": pending_event_count,
            "committed_event_count": len(source_event_ids),
            "source_event_ids": source_event_ids,
            "commit_reason": commit_reason,
            "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "raw_events_duplicated": False,
        }

    def node_summary_source_records(
        self,
        *,
        records: list[Json],
        node_path: list[str],
        scope: Json,
        max_events: int = 8,
        max_child_summaries: int = 8,
    ) -> tuple[list[Json], list[Json]]:
        prefix = node_path_tuple(node_path)
        child_summaries: list[Json] = []
        events: list[Json] = []
        seen_summary_keys: set[tuple[int, str]] = set()
        for record in reversed(records):
            if not scope_matches(record.get("scope", record.get("envelope", {}).get("scope", {})), scope):
                continue
            record_path = node_path_tuple(record.get("node_path", []))
            if not record_path or not starts_with_path(record_path, prefix):
                continue
            if record.get("record_type") == "context_summary" and record.get("summary_type") in {"node_l0", "node_l1", "batch_l0", "session_l0", "resource_l0", "skill_l0"}:
                if len(child_summaries) >= max_child_summaries:
                    continue
                try:
                    node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    continue
                key = (node_hash, str(record.get("summary_type", "")))
                if key in seen_summary_keys:
                    continue
                if node_path_tuple(record.get("node_path", [])) == prefix:
                    continue
                seen_summary_keys.add(key)
                child_summaries.append(record)
            elif record.get("record_type") == "context_event":
                if len(events) >= max_events:
                    continue
                events.append(record)
        return list(reversed(events[:max_events])), list(reversed(child_summaries[:max_child_summaries]))

    def mark_node_summary_dirty(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
    ) -> list[int]:
        prefixes = node_prefixes(node_path)
        if propagate_depth is not None and propagate_depth >= 0:
            prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
        dirty_hashes: list[int] = []
        for depth, prefix in enumerate(prefixes, start=1):
            node_hash = stable_hash("/".join(prefix))
            dirty_hash = stable_hash(
                f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
            )
            dirty_hashes.append(dirty_hash)
            self.append(
                {
                    "record_type": "context_summary_dirty",
                    "dirty_hash": dirty_hash,
                    "node_hash": node_hash,
                    "node_path": prefix,
                    "depth": len(prefix),
                    "dirty_reason": dirty_reason,
                    "source_ref_type": source_ref_type,
                    source_hash_field: source_hash,
                    "changed_ref_count": 1,
                    "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                    "scope": scope,
                    "status": "pending",
                    "created_at_ms": updated_at_ms,
                    "updated_at_ms": updated_at_ms,
                }
            )
        return dirty_hashes

    def refresh_dirty_node_summaries(
        self,
        *,
        scope: Json,
        limit: int = 64,
        refreshed_at_ms: int | None = None,
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        records = self.read_all()
        completed_dirty_hashes = {
            int(record.get("dirty_hash"))
            for record in records
            if record.get("record_type") == "context_summary_refresh_audit"
            and record.get("status") == "refreshed"
            and record.get("dirty_hash") is not None
        }
        pending_by_node: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_summary_dirty":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            try:
                dirty_hash = int(record.get("dirty_hash"))
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if dirty_hash in completed_dirty_hashes:
                continue
            current = pending_by_node.get(node_hash)
            if current is None or int(record.get("updated_at_ms") or 0) >= int(current.get("updated_at_ms") or 0):
                pending_by_node[node_hash] = record
        refreshed = []
        for dirty in sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]:
            node_path = [str(part) for part in dirty.get("node_path", [])]
            if not node_path:
                continue
            node_hash = int(dirty["node_hash"])
            events, child_summaries = self.node_summary_source_records(
                records=records,
                node_path=node_path,
                scope=dirty.get("scope", scope),
            )
            event_texts = [str(record.get("text", "")) for record in events if record.get("text")]
            child_summary_texts = [
                str(record.get("summary_text", ""))
                for record in child_summaries
                if record.get("summary_text")
            ]
            source_text = " ".join(child_summary_texts + event_texts)
            if not source_text:
                source_text = " ".join(node_path)
            prefix_label = " / ".join(node_path)
            l0_summary = summarize_text(f"{prefix_label} :: {source_text}", limit=220)
            l1_summary = summarize_text(
                f"Context node {prefix_label}. Overview: {source_text}. "
                f"This node belongs to path {prefix_label} and should be used for tree-first retrieval before leaf event/entity recall.",
                limit=1200,
            )
            source_event_ids = [int(record["event_id_hash"]) for record in events if record.get("event_id_hash") is not None]
            source_summary_hashes = [
                int(record.get("summary_hash") or record.get("node_hash"))
                for record in child_summaries
                if record.get("summary_hash") is not None or record.get("node_hash") is not None
            ]
            version_hash = stable_hash(
                f"summary_version:{node_hash}:{dirty.get('dirty_hash')}:{source_event_ids}:{source_summary_hashes}:{refreshed_at_ms}"
            )
            for level, summary_text, embedding_type in [
                ("node_l0", l0_summary, "node_l0"),
                ("node_l1", l1_summary, "node_l1"),
            ]:
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": level,
                        "summary_version_hash": version_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "summary_text": summary_text,
                        "source_event_ids": source_event_ids,
                        "source_summary_hashes": source_summary_hashes,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": embedding_type,
                        "ref_type": "node",
                        "ref_hash": node_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "dim": len(embedding_for_text(summary_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(summary_text),
                        "summary_version_hash": version_hash,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
            self.append(
                {
                    "record_type": "context_summary_refresh_audit",
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": version_hash,
                    "source_event_ids": source_event_ids,
                    "source_summary_hashes": source_summary_hashes,
                    "status": "refreshed",
                    "worker": "matrixark-local-async-summary-worker",
                    "refreshed_at_ms": refreshed_at_ms,
                    "scope": dirty.get("scope", scope),
                }
            )
            refreshed.append(
                {
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": version_hash,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                }
            )
        return {
            "status": "ok",
            "refreshed_count": len(refreshed),
            "refreshed": refreshed,
        }

    def append_node_summary_embeddings(
        self,
        *,
        node_path: list[str],
        source_text: str,
        scope: Json,
        updated_at_ms: int,
        source_hash_field: str,
        source_hash: int,
    ) -> Json:
        dirty_hashes = self.mark_node_summary_dirty(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_hash_field.removeprefix("source_").removesuffix("_hash"),
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason="new_event",
        )
        return {
            "status": "dirty_marked",
            "dirty_hashes": dirty_hashes,
            "refresh_result": None,
            "async_required": True,
        }

    def refresh_summaries(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 64)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        refreshed_at_ms = args.get("refreshed_at_ms")
        if refreshed_at_ms is not None and not isinstance(refreshed_at_ms, int):
            raise MatrixArkError("refreshed_at_ms must be an integer")
        return self.refresh_dirty_node_summaries(scope=scope, limit=limit, refreshed_at_ms=refreshed_at_ms)

    def latest_skill_controls(self, records: list[Json] | None = None) -> dict[int, Json]:
        controls: dict[int, Json] = {}
        for record in reversed(records if records is not None else self.read_all()):
            if record.get("record_type") != "skill_registry_update":
                continue
            try:
                skill_hash = int(record.get("skill_hash"))
            except (TypeError, ValueError):
                continue
            if skill_hash not in controls:
                controls[skill_hash] = record
        return controls

    def list_resources(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        resource_type_filter = optional_string(args, "resource_type", "")
        resources: dict[int, Json] = {}
        for record in reversed(self.read_all()):
            if record.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if resource_type_filter and record.get("resource_type") != resource_type_filter:
                continue
            resource_hash = int(record.get("resource_hash") or 0)
            if resource_hash in resources:
                continue
            resources[resource_hash] = {
                "resource_hash": resource_hash,
                "raw_uri": record.get("raw_uri", ""),
                "resource_type": record.get("resource_type", ""),
                "resource_version": record.get("resource_version", ""),
                "content_hash": record.get("content_hash", ""),
                "chunk_count": record.get("chunk_count", 0),
                "original_chunk_count": record.get("original_chunk_count", record.get("chunk_count", 0)),
                "deduped_chunk_count": record.get("deduped_chunk_count", 0),
                "superseded_chunk_count": record.get("superseded_chunk_count", 0),
                "superseded_chunk_hashes": record.get("superseded_chunk_hashes", []),
                "raw_storage_policy": record.get("raw_storage_policy", "raw_uri_only"),
                "raw_bytes_stored": bool(record.get("raw_bytes_stored", False)),
                "parse_warnings": record.get("parse_warnings", []),
                "parse_warning_count": record.get("parse_warning_count", 0),
                "async_parent_summary_required": bool(record.get("async_parent_summary_required", False)),
                "access_scope": record.get("access_scope", registry_access_scope(record.get("scope", {}))),
                "deployment_scope": record.get("deployment_scope", "local"),
                "import_task_hash": record.get("import_task_hash", 0),
                "token_estimate": record.get("token_estimate", 0),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", 0),
            }
            if len(resources) >= limit:
                break
        return {"status": "ok", "resources": list(resources.values()), "count": len(resources)}

    def list_skills(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        include_disabled = bool(args.get("include_disabled", False))
        controls = self.latest_skill_controls()
        skills: dict[int, Json] = {}
        for record in reversed(self.read_all()):
            if record.get("record_type") != "skill_manifest":
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            skill_hash = int(record.get("skill_hash") or 0)
            if skill_hash in skills:
                continue
            control = controls.get(skill_hash, {})
            status = str(control.get("status") or record.get("status") or "active")
            if status == "disabled" and not include_disabled:
                continue
            skills[skill_hash] = {
                "skill_hash": skill_hash,
                "name": record.get("name", ""),
                "description": record.get("description", ""),
                "raw_uri": record.get("raw_uri", ""),
                "owner_scope": control.get("owner_scope", record.get("owner_scope", "user")),
                "version": control.get("version", record.get("version", "1")),
                "status": status,
                "precedence": control.get("precedence", record.get("precedence", "normal")),
                "triggers": control.get("triggers", record.get("triggers", [])),
                "allowed_tools": control.get("allowed_tools", record.get("allowed_tools", [])),
                "examples": record.get("examples", record.get("metadata", {}).get("examples", [])),
                "permissions": record.get("permissions", record.get("metadata", {}).get("permissions", [])),
                "inputs": record.get("inputs", record.get("metadata", {}).get("inputs", [])),
                "outputs": record.get("outputs", record.get("metadata", {}).get("outputs", [])),
                "access_scope": record.get("access_scope", registry_access_scope(record.get("scope", {}))),
                "deployment_scope": record.get("deployment_scope", "local"),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": record.get("scope", {}),
                "updated_at_ms": control.get("updated_at_ms", record.get("updated_at_ms", 0)),
            }
            if len(skills) >= limit:
                break
        return {"status": "ok", "skills": list(skills.values()), "count": len(skills)}

    def update_skill(self, args: Json) -> Json:
        skill_hash = args.get("skill_hash")
        if not isinstance(skill_hash, int) or skill_hash <= 0:
            raise MatrixArkError("skill_hash must be a positive integer")
        status = optional_string(args, "status", "")
        if status and status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        precedence = optional_string(args, "precedence", "")
        if precedence and precedence not in {"low", "normal", "high", "critical"}:
            raise MatrixArkError("precedence must be low, normal, high, or critical")
        current = None
        for record in reversed(self.read_all()):
            if record.get("record_type") == "skill_manifest" and record.get("skill_hash") == skill_hash:
                current = record
                break
        if current is None:
            raise MatrixArkError("skill_hash not found")
        update = {
            "record_type": "skill_registry_update",
            "skill_hash": skill_hash,
            "status": status or current.get("status", "active"),
            "precedence": precedence or current.get("precedence", "normal"),
            "owner_scope": optional_string(args, "owner_scope", str(current.get("owner_scope") or "user")),
            "version": optional_string(args, "version", str(current.get("version") or "1")),
            "triggers": optional_string_list(args, "triggers", list(current.get("triggers", []))),
            "allowed_tools": optional_string_list(args, "allowed_tools", list(current.get("allowed_tools", []))),
            "scope": current.get("scope", {}),
            "node_hash": current.get("node_hash", 0),
            "node_path": current.get("node_path", []),
            "updated_at_ms": now_ms(),
        }
        self.append(update)
        return {"status": "updated", **update}

    def _run_background_resource_import(self, args: Json, hook: Json | None) -> None:
        task_hash = args.get("_resource_import_task_hash", 0)
        try:
            self.ingest(args, hook=hook)
        except Exception as exc:  # pragma: no cover - background failure path is validated via records.
            scope = optional_object(args, "scope")
            metadata = optional_object(args, "metadata")
            node_hint = metadata.get("node_path") or self.default_session_node_path(scope)
            node_path = [str(part) for part in node_hint if str(part)]
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": task_hash,
                    "status": "failed",
                    "kind": str(args.get("kind") or "resource"),
                    "raw_uri": str(args.get("raw_uri") or metadata.get("raw_uri") or "inline-resource"),
                    "resource_type": str(args.get("resource_type") or metadata.get("resource_type") or ""),
                    "error": str(exc),
                    "node_hash": stable_hash("/".join(node_path)),
                    "node_path": node_path,
                    "scope": normalize_scope(scope),
                    "updated_at_ms": now_ms(),
                }
            )

    def ingest(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        idle_commit_result: Json | None = None
        idle_commit_timeout_ms = args.get("idle_commit_timeout_ms")
        if idle_commit_timeout_ms is not None:
            if not isinstance(idle_commit_timeout_ms, int) or idle_commit_timeout_ms < 0:
                raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
            idle_commit_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": args.get("session_buffer_threshold", 20),
                    "force": False,
                    "idle_timeout_ms": idle_commit_timeout_ms,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                },
                hook=hook,
            )
        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction = compact_internal_extraction(
            envelope,
            prior_context=prior_context,
        )
        text = text_from_messages(envelope["messages"])
        event_id_hash = stable_hash(
            f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        resource_chunk_hashes: list[int] = []
        resource_dirty_hashes: list[int] = []
        resource_parse_error = ""
        resource_import_task_hash = 0
        resource_import_task_status = "not_applicable"
        resource_import_wait = True
        resource_import_metrics: Json = {}
        resource_fact_event_hashes: list[int] = []
        resource_fact_entity_hashes: list[int] = []
        skill_hash = None
        if envelope["kind"] in {"resource", "skill"}:
            raw_uri = str(envelope.get("raw_uri") or envelope["metadata"].get("raw_uri") or "inline-resource")
            resource_type = str(envelope.get("resource_type") or envelope["metadata"].get("resource_type") or "")
            resource_import_wait = bool(args.get("wait", True))
            resource_import_background = bool(args.get("_background_resource_import", False))
            deployment_scope = deployment_scope_from_args(args, envelope)
            access_scope = registry_access_scope(envelope["scope"])
            provided_task_hash = args.get("_resource_import_task_hash")
            resource_import_task_hash = (
                int(provided_task_hash)
                if isinstance(provided_task_hash, int) and provided_task_hash > 0
                else stable_hash(f"resource_import_task:{envelope['kind']}:{raw_uri}:{node_hash}:{envelope['ingestion_time_ms']}")
            )
            import_started_perf = time.perf_counter()
            if not resource_import_background:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "kind": envelope["kind"],
                        "raw_uri": raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_policy": "raw_uri_only",
                        "raw_bytes_stored": False,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "wait": resource_import_wait,
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if not resource_import_wait:
                background_args = {
                    **args,
                    "wait": True,
                    "_background_resource_import": True,
                    "_resource_import_task_hash": resource_import_task_hash,
                }
                thread = threading.Thread(
                    target=self._run_background_resource_import,
                    args=(background_args, hook),
                    daemon=True,
                )
                thread.start()
                return {
                    "status": "queued",
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "resource_import_task": {
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "wait": False,
                        "background_started": True,
                        "raw_uri": raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_policy": "raw_uri_only",
                        "raw_bytes_stored": False,
                    },
                    "node_materialization": node_materialization,
                }
            resource_import_task_status = "running"
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "running",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type,
                    "raw_storage_policy": "raw_uri_only",
                    "raw_bytes_stored": False,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "updated_at_ms": now_ms(),
                }
            )
            resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
            parse_text = resource_text
            if raw_uri != "inline-resource" and Path(raw_uri).exists():
                parse_text = None
            try:
                if envelope["kind"] == "skill" or (resource_type or "").lower() == "skill":
                    parsed_skill = parse_skill(
                        raw_uri,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                    )
                    skill_hash = parsed_skill.skill_hash
                    self.append(
                        {
                            "record_type": "skill_manifest",
                            "skill_hash": parsed_skill.skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "raw_uri": raw_uri,
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "text": parsed_skill.text,
                            "token_estimate": parsed_skill.token_estimate,
                            "metadata": parsed_skill.metadata,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    self.append(
                        {
                            "record_type": "skill_registry",
                            "registry_hash": stable_hash(f"skill_registry:{parsed_skill.skill_hash}:{deployment_scope}"),
                            "skill_hash": parsed_skill.skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "raw_uri": raw_uri,
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_summary",
                            "ref_type": "skill",
                            "ref_hash": parsed_skill.skill_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(skill_vector),
                            "model": embedding_model_name(),
                            "vector": skill_vector,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    parsed_chunks = parsed_skill.chunks
                else:
                    parsed_chunks = parse_resource(
                        raw_uri,
                        resource_type=resource_type or None,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                        resource_version=args.get("resource_version") if isinstance(args.get("resource_version"), str) else None,
                        supersedes_chunk_hashes=args.get("supersedes_chunk_hashes") if isinstance(args.get("supersedes_chunk_hashes"), dict) else None,
                    )
            except ResourceParserError as exc:
                resource_parse_error = str(exc)
                parsed_chunks = []
            if not parsed_chunks:
                resource_import_task_status = "failed"
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_policy": "raw_uri_only",
                        "raw_bytes_stored": False,
                        "error": resource_parse_error or "resource ingestion produced no chunks",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "updated_at_ms": now_ms(),
                    }
                )
                raise MatrixArkError(resource_parse_error or "resource ingestion produced no chunks")
            original_chunk_count = len(parsed_chunks)
            deduped_source_refs: list[str] = []
            seen_content_hashes: set[str] = set()
            unique_chunks = []
            for chunk in parsed_chunks:
                chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
                if chunk_content_hash in seen_content_hashes:
                    deduped_source_refs.append(chunk.source_ref)
                    continue
                seen_content_hashes.add(chunk_content_hash)
                unique_chunks.append(chunk)
            parsed_chunks = unique_chunks
            deduped_chunk_count = original_chunk_count - len(parsed_chunks)
            if not parsed_chunks:
                raise MatrixArkError("resource ingestion produced only duplicate chunks")
            resource_version_value = str(parsed_chunks[0].metadata.get("resource_version") or "")
            resource_content_hash = content_hash("\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in parsed_chunks))
            superseded_chunk_count = sum(1 for chunk in parsed_chunks if chunk.metadata.get("supersedes_chunk_hash"))
            superseded_chunk_hashes = [
                int(chunk.metadata["supersedes_chunk_hash"])
                for chunk in parsed_chunks
                if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
            ]
            parse_warnings = aggregate_parse_warnings_from_chunks(parsed_chunks)
            chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
            resource_kind = "skill" if skill_hash is not None else "resource"
            resource_l0_text = summarize_text(
                summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
                limit=700,
            )
            resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
            resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": f"{resource_kind}_l0",
                    "summary_hash": resource_summary_hash,
                    "import_task_hash": resource_import_task_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "raw_uri": raw_uri,
                    "summary_text": resource_l0_text,
                    "source_chunk_hashes": [chunk.chunk_hash for chunk in parsed_chunks],
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": f"{resource_kind}_l0",
                    "ref_type": "summary",
                    "ref_hash": resource_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(resource_summary_vector),
                    "model": embedding_model_name(),
                    "vector": resource_summary_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            resource_dirty_hashes = self.mark_node_summary_dirty(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
                source_ref_type=f"{resource_kind}_summary",
                source_hash_field="source_summary_hash",
                source_hash=resource_summary_hash,
                dirty_reason=f"{resource_kind}_update",
            )
            resource_indexes = ordered_unique(
                [
                    context_index_name("source_type", envelope["kind"]),
                    context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                ]
                + (
                    [
                        context_index_name("skill_name", parsed_skill.name),
                    ]
                    + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                    + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                    if skill_hash is not None
                    else []
                )
            )
            for index_name in resource_indexes:
                self.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "index_hash": stable_hash(f"{index_name}:{resource_summary_hash}"),
                        "summary_hash": resource_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if envelope["kind"] == "resource":
                manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
                self.append(
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "raw_storage_policy": "raw_uri_only",
                        "raw_bytes_stored": False,
                        "parse_warnings": parse_warnings[:100],
                        "parse_warning_count": len(parse_warnings),
                        "chunk_count": len(parsed_chunks),
                        "original_chunk_count": original_chunk_count,
                        "deduped_chunk_count": deduped_chunk_count,
                        "deduped_source_refs": deduped_source_refs[:50],
                        "superseded_chunk_count": superseded_chunk_count,
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "summary_dirty_hashes": resource_dirty_hashes,
                        "async_parent_summary_required": bool(resource_dirty_hashes),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "token_estimate": sum(chunk.token_estimate for chunk in parsed_chunks),
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "resource_registry",
                        "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version_value}:{deployment_scope}"),
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "raw_uri": raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "chunk_count": len(parsed_chunks),
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            for chunk, vector in zip(parsed_chunks, chunk_vectors):
                resource_chunk_hashes.append(chunk.chunk_hash)
                chunk_metadata = sanitize_resource_metadata(chunk.metadata)
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "skill_section",
                            "import_task_hash": resource_import_task_hash,
                            "skill_hash": skill_hash,
                            "section_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "source_ref": chunk.source_ref,
                            "heading": chunk_metadata.get("heading", ""),
                            "text": chunk.text,
                            "token_estimate": chunk.token_estimate,
                            "metadata": chunk_metadata,
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "resource_chunk",
                        "import_task_hash": resource_import_task_hash,
                        "chunk_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "resource_type": chunk_metadata.get("resource_type") or resource_type,
                        "source_ref": chunk.source_ref,
                        "text": chunk.text,
                        "token_estimate": chunk.token_estimate,
                        "metadata": chunk_metadata,
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "resource_chunk",
                        "ref_type": "resource_chunk",
                        "ref_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(vector),
                        "model": embedding_model_name(),
                        "vector": vector,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_section",
                            "ref_type": "skill_section",
                            "ref_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(vector),
                            "model": embedding_model_name(),
                            "vector": vector,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                chunk_index_terms = ordered_unique(
                    [
                        context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ]
                    + metadata_index_terms(chunk_metadata)
                    + (
                        [context_index_name("skill_name", parsed_skill.name)]
                        + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                        + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                        if skill_hash is not None and parsed_skill is not None
                        else []
                    )
                )
                for index_name in chunk_index_terms:
                    self.append(
                        {
                            "record_type": "context_index",
                            "index_name": index_name,
                            "index_hash": stable_hash(f"{index_name}:{chunk.chunk_hash}"),
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "source_ref": chunk.source_ref,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
            resource_fact_records: list[Json] = []
            fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:32]
            for chunk in fact_chunks:
                chunk_metadata = sanitize_resource_metadata(chunk.metadata)
                for fact_extraction in extract_resource_facts(
                    chunk,
                    chunk_metadata=chunk_metadata,
                    envelope=envelope,
                    raw_uri=raw_uri,
                    resource_version=resource_version_value,
                ):
                    fact_event_type = str(fact_extraction["event_type"])
                    fact_entity_type = str(fact_extraction["entity_type"])
                    fact_value = str(fact_extraction.get("value", ""))
                    fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version_value}")
                    resource_fact_event_hashes.append(fact_event_hash)
                    fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
                    resource_fact_records.append(
                        {
                            "record_type": "context_event",
                            "event_id_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "text": chunk.text,
                            "summary_text": fact_summary,
                            "envelope": {**envelope, "kind": "resource_fact"},
                            "internal_extraction": fact_extraction,
                            "source_chunk_hash": chunk.chunk_hash,
                            "source_ref": chunk.source_ref,
                            "resource_version": resource_version_value,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "event_text",
                            "ref_type": "event",
                            "ref_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(fact_vector),
                            "model": embedding_model_name(),
                            "vector": fact_vector,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
                    entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
                    resource_fact_entity_hashes.append(entity_hash)
                    entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
                    resource_fact_records.append(
                        {
                            "record_type": "context_entity",
                            "entity_hash": entity_hash,
                            "batch_id_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": envelope["scope"],
                            "entity_type": fact_entity_type,
                            "entity_name": entity_name,
                            "state": entity_state,
                            "confidence": fact_extraction.get("confidence", 0.78),
                            "operator": "LATEST",
                            "source_refs": [chunk.source_ref],
                            "source_event_ids": [fact_event_hash],
                            "source_chunk_hash": chunk.chunk_hash,
                            "source_ref": chunk.source_ref,
                            "resource_version": resource_version_value,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "entity_state",
                            "ref_type": "entity",
                            "ref_hash": entity_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(entity_vector),
                            "model": embedding_model_name(),
                            "vector": entity_vector,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    for index_name in ordered_unique([
                        context_index_name("source_type", "resource_fact"),
                        context_index_name("event_type", fact_event_type),
                        context_index_name("entity_type", fact_entity_type),
                        context_index_name("entity_type", "resource_fact"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ] + metadata_index_terms(chunk_metadata)):
                        resource_fact_records.append(
                            {
                                "record_type": "context_index",
                                "index_name": index_name,
                                "index_hash": stable_hash(f"{index_name}:{fact_event_hash}"),
                                "batch_id_hash": resource_import_task_hash,
                                "ref_type": "resource_fact",
                                "ref_hash": fact_event_hash,
                                "chunk_hash": chunk.chunk_hash,
                                "node_hash": node_hash,
                                "node_path": node_path,
                                "scope": envelope["scope"],
                                "updated_at_ms": envelope["ingestion_time_ms"],
                            }
                        )
            if resource_fact_records:
                self.append_many(resource_fact_records)
            resource_import_metrics = {
                "duration_ms": round((time.perf_counter() - import_started_perf) * 1000.0, 3),
                "parser_chunk_count": original_chunk_count,
                "chunk_count": len(parsed_chunks),
                "dedupe_count": deduped_chunk_count,
                "embedding_count": len(chunk_vectors) + 1 + len(resource_fact_event_hashes) + len(resource_fact_entity_hashes),
                "resource_fact_count": len(resource_fact_event_hashes),
                "resource_entity_count": len(resource_fact_entity_hashes),
                "parse_warning_count": len(parse_warnings),
                "parse_warnings": parse_warnings[:100],
                "raw_storage_policy": "raw_uri_only",
                "raw_bytes_stored": False,
                "summary_dirty_count": len(resource_dirty_hashes),
            }
            resource_import_task_status = "completed"
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "completed",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "raw_storage_policy": "raw_uri_only",
                    "raw_bytes_stored": False,
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "resource_fact_count": len(resource_fact_event_hashes),
                    "resource_entity_count": len(resource_fact_entity_hashes),
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "metrics": resource_import_metrics,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "updated_at_ms": now_ms(),
                }
            )
            self.append(
                {
                    "record_type": "matrixark_metric",
                    "metric_name": "resource_import",
                    "task_hash": resource_import_task_hash,
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "metrics": resource_import_metrics,
                    "scope": envelope["scope"],
                    "created_at_ms": now_ms(),
                }
            )
        summary_text = summarize_text(text)
        event_embedding = embedding_for_text(text)
        summary_embedding = embedding_for_text(" ".join(node_path + [summary_text]))
        session_key_parts = [str(part) for part in context_node_key(envelope)]
        if any(session_key_parts):
            session_summary_source = " ".join(
                [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                + [text]
            )
            session_summary_text = summarize_text(session_summary_source, limit=512)
            session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": "session_l0",
                    "summary_hash": session_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "context_node_key": session_key_parts,
                    "summary_text": session_summary_text,
                    "source_event_hash": event_id_hash,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "session_l0",
                    "ref_type": "summary",
                    "ref_hash": session_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(session_summary_text)),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(session_summary_text),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(event_embedding),
                "model": embedding_model_name(),
                "vector": event_embedding,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        record = {
            "record_type": "context_event",
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "text": text,
            "summary_text": summary_text,
            "summary_embedding": summary_embedding,
            "envelope": envelope,
            "internal_extraction": extraction,
            "prior_context": prior_context,
            "agent_hook": hook,
        }
        self.append(record)
        self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=text,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_event_hash",
            source_hash=event_id_hash,
        )
        pending_event_count = len(self.pending_session_events(envelope["scope"]))
        auto_batch_result: Json | None = None
        auto_batch_extract = bool(args.get("auto_batch_extract", False))
        session_buffer_threshold = args.get("session_buffer_threshold", 20)
        if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
            raise MatrixArkError("session_buffer_threshold must be a positive integer")
        if auto_batch_extract and pending_event_count >= session_buffer_threshold:
            auto_batch_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": False,
                    "max_messages": session_buffer_threshold,
                    "commit_reason": "threshold",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                },
                hook=hook,
            )
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
            "hook_captured": hook is not None,
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "extraction_mode": extraction["mode"],
            "classification": extraction.get("classification", "UNCLASSIFIED"),
            "prior_context": extraction.get("prior_context", ""),
            "prior_refs": extraction.get("prior_refs", []),
            "prior_message_count": extraction.get("prior_message_count", 0),
            "prior_summary_count": extraction.get("prior_summary_count", 0),
            "quality_warning": extraction.get("quality_warning", ""),
            "summary_refresh": summary_refresh,
            "resource_summary_refresh": {
                "status": "dirty_marked" if resource_dirty_hashes else "not_applicable",
                "dirty_hashes": resource_dirty_hashes,
                "refresh_result": None,
                "async_required": bool(resource_dirty_hashes),
            },
            "resource_import_task": {
                "task_hash": resource_import_task_hash,
                "status": resource_import_task_status,
                "wait": resource_import_wait,
                "metrics": resource_import_metrics,
                "raw_storage_policy": "raw_uri_only" if resource_import_task_hash else "",
                "raw_bytes_stored": False if resource_import_task_hash else None,
            },
            "node_materialization": node_materialization,
            "resource_chunks": resource_chunk_hashes,
            "resource_chunk_count": len(resource_chunk_hashes),
            "resource_original_chunk_count": original_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_chunk_count": deduped_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_source_refs": deduped_source_refs[:20] if envelope["kind"] in {"resource", "skill"} else [],
            "resource_version": resource_version_value if envelope["kind"] in {"resource", "skill"} else "",
            "resource_content_hash": resource_content_hash if envelope["kind"] in {"resource", "skill"} else "",
            "resource_parse_warnings": parse_warnings if envelope["kind"] in {"resource", "skill"} else [],
            "resource_parse_warning_count": len(parse_warnings) if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_raw_storage_policy": "raw_uri_only" if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_bytes_stored": False if envelope["kind"] in {"resource", "skill"} else None,
            "resource_superseded_chunk_count": superseded_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_superseded_chunk_hashes": superseded_chunk_hashes if envelope["kind"] in {"resource", "skill"} else [],
            "resource_fact_events": resource_fact_event_hashes,
            "resource_fact_event_count": len(resource_fact_event_hashes),
            "resource_fact_entities": resource_fact_entity_hashes,
            "resource_fact_entity_count": len(resource_fact_entity_hashes),
            "skill_hash": skill_hash,
            "session_buffer": {
                "buffer_key": list(session_buffer_key(envelope)),
                "pending_event_count": pending_event_count,
                "threshold_messages": session_buffer_threshold,
                "auto_batch_extract": auto_batch_extract,
            },
            "idle_commit_result": idle_commit_result,
            "auto_batch_extract_result": auto_batch_result,
        }

    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        threshold = args.get("threshold_messages", 20)
        force = bool(args.get("force", False))
        derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
        source_event_ids = [int(ref) for ref in args.get("source_event_ids", [])] if isinstance(args.get("source_event_ids", []), list) else []
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        if len(envelope["messages"]) < threshold and not force:
            return {
                "status": "deferred",
                "message_count": len(envelope["messages"]),
                "threshold_messages": threshold,
                "reason": "logical batch below extraction threshold",
            }

        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction = one_pass_memory_extraction(envelope, prior_context=prior_context)
        batch_text = text_from_messages(envelope["messages"])
        batch_id_hash = stable_hash(
            f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        batch_summary = extraction["batch_summary"]

        event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
        records_to_append: list[Json] = []
        if not derive_from_existing_events:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                records_to_append.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": event_text,
                        "summary_text": summarize_text(event_text),
                        "envelope": {
                            **envelope,
                            "messages": [message],
                        },
                        "internal_extraction": {
                            "mode": extraction["mode"],
                            "classification": extraction["classification"],
                            "event_type": extraction["event_type"],
                            "batch_id_hash": batch_id_hash,
                        },
                        "prior_context": prior_context,
                        "agent_hook": hook,
                    }
                )
                records_to_append.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(event_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(event_text),
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )

        entity_hashes = []
        for entity in extraction["entities"]:
            entity_hash = stable_hash(
                f"{node_hash}:{entity['entity_type']}:{entity['entity_name']}"
            )
            previous_entity = self.find_latest_entity(
                node_hash=node_hash,
                entity_type=entity["entity_type"],
                entity_name=entity["entity_name"],
            )
            updated_entity = apply_entity_patches(previous_entity, entity)
            entity_hashes.append(entity_hash)
            records_to_append.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "state": updated_entity["state"],
                    "previous_state": updated_entity.get("previous_state", ""),
                    "confidence": updated_entity["confidence"],
                    "operator": updated_entity["operator"],
                    "source_refs": updated_entity["source_refs"],
                    "source_event_ids": source_event_ids,
                    "field_patches": updated_entity.get("field_patches", []),
                    "patch_results": updated_entity.get("patch_results", []),
                    "update_mode": updated_entity.get("update_mode", ""),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            if updated_entity.get("patch_results"):
                records_to_append.append(
                    {
                        "record_type": "context_entity_update_audit",
                        "entity_hash": entity_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "entity_type": updated_entity["entity_type"],
                        "entity_name": updated_entity["entity_name"],
                        "previous_state": updated_entity.get("previous_state", ""),
                        "new_state": updated_entity["state"],
                        "patch_results": updated_entity.get("patch_results", []),
                        "llm_calls": 0,
                        "update_mode": "deterministic_eua",
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(updated_entity["entity_type"] + " " + updated_entity["state"])),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(updated_entity["entity_type"] + " " + updated_entity["state"]),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        segment_hashes = []
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            segment_hashes.append(segment_hash)
            records_to_append.append(
                {
                    "record_type": "context_segment",
                    "segment_hash": segment_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "topic": segment["topic"],
                    "coordinate_tuples": segment["coordinate_tuples"],
                    "message_indexes": segment["message_indexes"],
                    "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                    "saliency_score": segment["saliency_score"],
                    "summary_text": segment["summary_text"],
                    "text": segment["text"],
                    "non_contiguous": segment["non_contiguous"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(segment["topic"] + " " + segment["summary_text"])),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(segment["topic"] + " " + segment["summary_text"]),
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
        records_to_append.append(
            {
                "record_type": "context_summary",
                "summary_type": "batch_l0",
                "summary_hash": summary_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "summary_text": batch_summary,
                "source_entity_hashes": entity_hashes,
                "source_segment_hashes": segment_hashes,
                "source_event_ids": event_hashes,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(embedding_for_text(" ".join(node_path + [batch_summary]))),
                "model": embedding_model_name(),
                "vector": embedding_for_text(" ".join(node_path + [batch_summary])),
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        for index_name in extraction["indexes"]:
            records_to_append.append(
                {
                    "record_type": "context_index",
                    "index_name": index_name,
                    "index_hash": stable_hash(f"{index_name}:{batch_id_hash}"),
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        records_to_append.append(
            {
                "record_type": "context_extraction_audit",
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "schema": extraction["schema"],
                "message_count": extraction["message_count"],
                "token_count_estimate": extraction["token_count_estimate"],
                "outputs": {
                    "events": 0 if derive_from_existing_events else len(envelope["messages"]),
                    "source_events": len(event_hashes),
                    "entities": len(entity_hashes),
                    "segments": len(segment_hashes),
                    "summaries": 1,
                    "indexes": len(extraction["indexes"]),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        self.append_many(records_to_append)
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=batch_summary,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
        )
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "segment_provider": extraction.get("segment_provider", {}),
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "events_written": 0 if derive_from_existing_events else len(envelope["messages"]),
            "source_event_count": len(event_hashes),
            "raw_events_duplicated": not derive_from_existing_events,
            "entities_written": len(entity_hashes),
            "segments_written": len(segment_hashes),
            "summary_hash": summary_hash,
            "summary_refresh": summary_refresh,
            "node_materialization": node_materialization,
            "indexes_written": len(extraction["indexes"]),
            "one_pass": True,
            "threshold_messages": threshold,
        }

    def write_time_compression(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        source_start_ms: int,
        source_end_ms: int,
        compressed_time_ms: int,
        max_source_events: int = 32,
        min_confidence: float = 0.0,
        min_importance: float = 0.0,
        summary: str = "",
    ) -> Json:
        if source_start_ms > source_end_ms:
            raise MatrixArkError("source_start_ms must be <= source_end_ms")
        if max_source_events <= 0:
            raise MatrixArkError("max_source_events must be positive")
        source_events = []
        for record in self.read_all():
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            event_scope = record.get("envelope", {}).get("scope", {})
            if not scope_matches(event_scope, scope):
                continue
            event_time = int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            if event_time < source_start_ms or event_time > source_end_ms:
                continue
            extraction = record.get("internal_extraction", {})
            confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
            importance = float(record.get("envelope", {}).get("metadata", {}).get("importance", record.get("importance", 1.0)) or 1.0)
            if confidence < min_confidence or importance < min_importance:
                continue
            source_events.append(record)
        source_events.sort(key=lambda record: int(record.get("envelope", {}).get("ingestion_time_ms") or 0))
        selected = source_events[:max_source_events]
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        truncated = len(source_events) > len(selected)
        source_event_ids = [int(record["event_id_hash"]) for record in selected]
        compression_scope = selected[0].get("envelope", {}).get("scope", scope)
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
        record = {
            "record_type": "context_compression_event",
            "compression_id_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": compression_scope,
            "source_start_ms": source_start_ms,
            "source_end_ms": source_end_ms,
            "compressed_time_ms": compressed_time_ms,
            "summary_text": summarize_text(summary, limit=1200),
            "source_event_ids": source_event_ids,
            "source_event_count": len(selected),
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(embedding_for_text(record["summary_text"])),
                "model": embedding_model_name(),
                "vector": embedding_for_text(record["summary_text"]),
                "scope": compression_scope,
                "updated_at_ms": compressed_time_ms,
            }
        )
        return record

    def query_time_compressions(
        self, *, scope: Json, node_hashes: set[int], start_time_ms: int, end_time_ms: int, limit: int = 16
    ) -> list[Json]:
        matches = []
        for record in self.read_all():
            if record.get("record_type") != "context_compression_event":
                continue
            if node_hashes and int(record.get("node_hash") or 0) not in node_hashes:
                continue
            if not scope_matches(record.get("scope", {}), scope):
                continue
            if int(record.get("source_end_ms") or 0) >= start_time_ms and int(record.get("source_start_ms") or 0) <= end_time_ms:
                matches.append(record)
        matches.sort(key=lambda record: (int(record.get("source_end_ms") or 0), int(record.get("compressed_time_ms") or 0)), reverse=True)
        return matches[:limit]

    def deadline_fallback_pack(
        self,
        *,
        query: str,
        scope: Json,
        question_type: str,
        max_context_tokens: int,
        local_budget: Json,
        deadline_ms: int,
        elapsed_ms: float,
        records: list[Json],
        reason: str,
    ) -> Json:
        selected = []
        used_context_tokens = 0
        remote_budget = max(0, max_context_tokens - int(local_budget.get("token_estimate", 0)))
        for record in reversed(records):
            record_type = record.get("record_type")
            record_scope = record.get("scope", record.get("envelope", {}).get("scope", {}))
            if record_type not in {"context_summary", "context_entity", "context_event", "context_segment"}:
                continue
            if not scope_matches(record_scope, scope):
                continue
            if record_type == "context_summary":
                text = str(record.get("summary_text", ""))
                ref_type = "summary"
                ref_hash = record.get("summary_hash") or record.get("node_hash")
            elif record_type == "context_entity":
                text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                ref_type = "entity"
                ref_hash = record.get("entity_hash")
            elif record_type == "context_segment":
                text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
                ref_type = "segment"
                ref_hash = record.get("segment_hash")
            else:
                text = str(record.get("summary_text") or record.get("text") or "")
                ref_type = "event"
                ref_hash = record.get("event_id_hash")
            if not text or ref_hash is None:
                continue
            item_tokens = token_count(text)
            if used_context_tokens + item_tokens > remote_budget:
                continue
            selected.append(
                {
                    "ref_type": ref_type,
                    "ref_hash": ref_hash,
                    "node_hash": record.get("node_hash"),
                    "node_path": record.get("node_path", []),
                    "score": 0.0,
                    "recall_path": "deadline_fallback_recent_context",
                    "updated_at_ms": record.get("updated_at_ms", record.get("envelope", {}).get("ingestion_time_ms", now_ms())),
                    "text": clip_context_text(text),
                }
            )
            used_context_tokens += item_tokens
            if len(selected) >= 8:
                break
        context_pack_id = str(stable_hash(f"deadline:{query}:{selected}:{now_ms()}"))
        pack = {
            "context_pack_id": context_pack_id,
            "selected_refs": selected,
            "layer_scores": [],
            "question_type": question_type,
            "packing_policy": f"deadline_fallback:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "deadline_ms": deadline_ms,
                "elapsed_ms": elapsed_ms,
                "partial_context_pack": True,
                "fallback_reason": reason,
            },
            "primary_candidate_count": 0,
            "auxiliary_candidate_count": 0,
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_budget["token_estimate"],
            "total_prompt_context_tokens": used_context_tokens + local_budget["token_estimate"],
            "remote_context_budget_tokens": remote_budget,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_budget["token_estimate"],
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": [],
            "quality_warnings": [f"retrieval_deadline_exceeded:{reason}"],
            "insufficient_context": not selected,
            "partial_context_pack": True,
        }
        self.append_audit(
            {
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id,
                "query": query,
                "scope": scope,
                "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected), limit=512),
                "selected_refs": compact_refs_for_audit(selected),
                "question_type": question_type,
                "packing_policy": pack["packing_policy"],
                "recall_policy": pack["recall_policy"],
                "local_context_policy": pack["local_context_policy"],
                "used_local_context_tokens": pack["used_local_context_tokens"],
                "used_remote_context_tokens": pack["used_remote_context_tokens"],
                "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                "primary_candidate_count": 0,
                "auxiliary_candidate_count": 0,
                "created_at_ms": now_ms(),
            }
        )
        return pack

    def retrieve(self, args: Json) -> Json:
        started_perf = time.perf_counter()
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        ranking = optional_object(args, "ranking")
        raw_deadline_ms = args.get("deadline_ms", ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)))
        try:
            deadline_ms = int(raw_deadline_ms or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("deadline_ms must be an integer")

        def deadline_exceeded() -> bool:
            return deadline_ms > 0 and (time.perf_counter() - started_perf) * 1000.0 >= deadline_ms

        question_type = str(args.get("question_type") or infer_query_type(query))
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        max_context_tokens = args.get("max_context_tokens", 2048)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        query_terms = {term for term in tokens(query) if len(term) > 2}
        query_embedding = embedding_for_text(query)
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        records = self.read_all()
        skill_controls = self.latest_skill_controls(records)
        include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
        latest_resource_version_by_uri: dict[str, str] = {}
        for manifest in reversed(records):
            if manifest.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(manifest.get("scope", {}), scope):
                continue
            raw_uri_key = str(manifest.get("raw_uri") or "")
            resource_version_key = str(manifest.get("resource_version") or "")
            if raw_uri_key and resource_version_key and raw_uri_key not in latest_resource_version_by_uri:
                latest_resource_version_by_uri[raw_uri_key] = resource_version_key
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_record_load",
            )
        node_scores: dict[int, Json] = {}
        event_embedding_vectors: dict[int, list[float]] = {}
        entity_embedding_vectors: dict[int, list[float]] = {}
        segment_embedding_vectors: dict[int, list[float]] = {}
        compression_embedding_vectors: dict[int, list[float]] = {}
        resource_embedding_vectors: dict[int, list[float]] = {}
        skill_embedding_vectors: dict[int, list[float]] = {}
        index_terms_by_batch: dict[Any, list[str]] = {}
        index_terms_by_node: dict[Any, list[str]] = {}
        index_terms_by_ref: dict[Any, list[str]] = {}
        node_summary_text_by_hash: dict[int, str] = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(record.get("scope", {}), scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                    ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                    if ref_hash is not None:
                        index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                    else:
                        index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            if record_type == "context_summary" and scope_matches(record.get("scope", {}), scope):
                summary_type = str(record.get("summary_type", ""))
                if summary_type in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                    try:
                        node_hash_for_summary = int(record.get("node_hash"))
                    except (TypeError, ValueError):
                        continue
                    existing = node_summary_text_by_hash.get(node_hash_for_summary, "")
                    summary_text = str(record.get("summary_text", ""))
                    if len(summary_text) > len(existing):
                        node_summary_text_by_hash[node_hash_for_summary] = summary_text
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_embedding" and not scope_matches(record.get("scope", {}), scope):
                continue
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
                dense_score = cosine(query_embedding, record.get("vector", []))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": record.get("embedding_type"),
                    }
            elif record_type == "context_embedding" and record.get("embedding_type") == "event_text":
                event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "entity_state":
                entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "segment_text":
                segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "compression_summary":
                compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "resource_chunk":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_section":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_summary":
                skill_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_embedding_index_scan",
            )

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", 8, minimum=1)
        max_children_scored_per_parent = integer_arg(ranking, "max_children_scored_per_parent", 10000, minimum=1)
        traversal = tree_first_traversal(
            node_scores,
            top_k_per_layer=top_k_per_layer,
            max_children_scored_per_parent=max_children_scored_per_parent,
        )
        selected_paths = traversal["selected_paths"]
        selected_leaf_paths = traversal["leaf_paths"]
        selected_node_hashes = traversal["selected_node_hashes"]

        def selected_by_tree(record: Json) -> bool:
            if traversal.get("fallback_to_flat"):
                return True
            path = node_path_tuple(record.get("node_path", []))
            if path and path in selected_paths:
                return True
            if path and any(
                starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
                for leaf_path in selected_leaf_paths
            ):
                return True
            try:
                return int(record.get("node_hash")) in selected_node_hashes
            except (TypeError, ValueError):
                return False

        layer_scores = sorted(
            traversal["trace"] or node_scores.values(),
            key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
        )
        primary_matches = []
        auxiliary_matches = []
        if question_type == "broad_exploration":
            for record in reversed(records):
                if record.get("record_type") != "context_summary":
                    continue
                if not access_scope_matches_before_scoring(record, scope):
                    continue
                if not selected_by_tree(record):
                    continue
                summary_type = str(record.get("summary_type") or "")
                if summary_type not in {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0"}:
                    continue
                index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                    secondary_index_dropped_count += 1
                    continue
                secondary_index_matched_count += 1
                text = str(record.get("summary_text", ""))
                if not text:
                    continue
                sparse_score = sparse_lexical_score(query_terms, text)
                keyword_score = len(query_terms.intersection(tokens(text)))
                embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
                node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                origin_score = min(1.0, 0.06 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
                if origin_score <= 0:
                    continue
                primary_matches.append(
                    score_recall_candidate(
                        {
                            "ref_type": "summary",
                            "ref_hash": record.get("summary_hash") or record.get("node_hash"),
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": origin_score,
                            "keyword_score": keyword_score,
                            "sparse_score": sparse_score,
                            "embedding_score": embedding_score,
                            "node_score": node_score,
                            "matched_index_terms": sorted(index_terms),
                            "selection_reason": "selected by tree path and L0/L1 summary relevance",
                            "event_type": summary_type,
                            "context_class": "summary",
                            "summary_type": summary_type,
                            "access_decision": "allowed_by_registry_scope_before_scoring",
                            "access_scope": candidate_access_scope(record),
                            "scope": record.get("scope", {}),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "primary_summary",
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for record in reversed(records):
            if record.get("record_type") != "context_event":
                continue
            envelope = record.get("envelope", {})
            record_scope = envelope.get("scope", {})
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = str(record.get("text", ""))
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
            extraction = record.get("internal_extraction", {})
            event_type = str(extraction.get("event_type") or extraction.get("classification") or "")
            candidate = {
                "ref_type": "event",
                "ref_hash": record["event_id_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource fact/event hybrid score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and event hybrid score"
                ),
                "event_type": event_type,
                "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": envelope.get("metadata", {}),
                "scope": record_scope,
                "updated_at_ms": envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_event_scan",
            )
        for record in reversed(records):
            if record.get("record_type") != "context_entity":
                continue
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.12 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "entity",
                "ref_hash": record["entity_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource entity state score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and entity state score"
                ),
                "entity_type": record.get("entity_type", ""),
                "entity_name": record.get("entity_name", ""),
                "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": record.get("metadata", {}),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_entity_scan",
            )
        for record in reversed(records):
            if record.get("record_type") != "context_segment":
                continue
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            saliency_score = float(record.get("saliency_score", 0.0))
            origin_score = min(
                1.0,
                0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
            )
            candidate = {
                "ref_type": "segment",
                "ref_hash": record["segment_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
                "saliency_score": saliency_score,
                "topic": record.get("topic", ""),
                "coordinate_tuples": record.get("coordinate_tuples", []),
                "non_contiguous": record.get("non_contiguous", False),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_segment_scan",
            )
        for record in reversed(records):
            if record.get("record_type") not in {"resource_chunk", "skill_section"}:
                continue
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if record.get("record_type") == "skill_section":
                ref_type = "skill_section"
                ref_hash = int(record.get("section_hash") or 0)
                parent_skill_hash = int(record.get("skill_hash") or 0)
                control = skill_controls.get(parent_skill_hash, {})
                if str(control.get("status") or "active") != "active":
                    continue
                text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = "skill"
                metadata = {**record.get("metadata", {}), "skill_registry": control}
            else:
                ref_type = "resource_chunk"
                ref_hash = int(record.get("chunk_hash") or 0)
                metadata = record.get("metadata", {})
                raw_uri_value = str(record.get("raw_uri") or "")
                resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
                latest_version = latest_resource_version_by_uri.get(raw_uri_value, resource_version_value)
                is_superseded_version = bool(
                    resource_version_value
                    and latest_version
                    and resource_version_value != latest_version
                )
                if is_superseded_version and not include_superseded_resources:
                    secondary_index_dropped_count += 1
                    continue
                text = f"resource {raw_uri_value} {record.get('source_ref', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = str(record.get("resource_type") or "resource")
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            if origin_score <= 0:
                continue
            primary_matches.append(
                score_recall_candidate(
                    {
                        "ref_type": ref_type,
                        "ref_hash": ref_hash,
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(index_terms),
                        "selection_reason": (
                            "selected by tree path, secondary indexes, and resource/skill hybrid score"
                            if index_terms
                            else "selected by tree path and resource/skill hybrid score"
                        ),
                        "event_type": business_type,
                        "context_class": ref_type,
                        "raw_uri": record.get("raw_uri", ""),
                        "source_ref": record.get("source_ref", ""),
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": metadata.get("resource_version", ""),
                        "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                        "version_state": "historical" if ref_type == "resource_chunk" and metadata.get("resource_version") != latest_resource_version_by_uri.get(str(record.get("raw_uri") or ""), metadata.get("resource_version", "")) else "current",
                        "stale_or_superseded": bool(ref_type == "resource_chunk" and metadata.get("resource_version") != latest_resource_version_by_uri.get(str(record.get("raw_uri") or ""), metadata.get("resource_version", ""))),
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "deployment_scope": record.get("deployment_scope", "local"),
                        "citation": record.get("source_ref", ""),
                        "metadata": metadata,
                        "scope": record.get("scope", {}),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_resource_skill",
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )

        for record in reversed(records):
            if record.get("record_type") != "context_compression_event":
                continue
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            text = f"TIME_COMPRESS: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            compression_hash = int(record.get("compression_id_hash") or 0)
            embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "compression",
                "ref_hash": compression_hash,
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "event_type": "time_compress",
                "operator": "TIME_COMPRESS",
                "source_event_ids": record.get("source_event_ids", []),
                "source_start_ms": record.get("source_start_ms"),
                "source_end_ms": record.get("source_end_ms"),
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_time_compression"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_compression_scan",
            )
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=max_context_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=local_budget["token_estimate"],
            duplicate_text_hashes=local_budget["text_hashes"],
        )
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        selected_context_counts = selected_context_class_counts(selected)
        pack = {
            "context_pack_id": str(context_pack_id),
            "selected_refs": selected,
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": {
                "access_scope_before_scoring": True,
                "skill_selection": "skill_section_only",
                "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
            },
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                    "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
                },
                "secondary_index_filter": {
                    "enabled": bool(secondary_index_filter_groups),
                    "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                    "matched_candidate_count": secondary_index_matched_count,
                    "dropped_candidate_count": secondary_index_dropped_count,
                    "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                    "effective_mode": secondary_index_filter_mode,
                    "applied_before_embedding_scoring": True,
                },
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS),
                    "half_life_ms": ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS),
                },
                "weights": {
                    "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                    "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
                },
                "auxiliary_quota": auxiliary_quota,
            },
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_budget["token_estimate"],
            "total_prompt_context_tokens": used_context_tokens + local_budget["token_estimate"],
            "remote_context_budget_tokens": max(0, max_context_tokens - local_budget["token_estimate"]),
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_budget["token_estimate"],
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": dropped_over_budget,
            "quality_warnings": [],
            "insufficient_context": not selected,
        }
        self.append_audit(
            {
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id_text,
                "query": query,
                "scope": scope,
                "summary_text": pack_summary,
                "selected_refs": compact_refs_for_audit(selected),
                "selected_ref_counts": selected_context_counts,
                "context_assembly_policy": pack["context_assembly_policy"],
                "dropped_refs": dropped_over_budget,
                "layer_scores": layer_scores[:24],
                "tree_traversal": pack["recall_policy"]["tree_traversal"],
                "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
                "question_type": question_type,
                "packing_policy": pack["packing_policy"],
                "recall_policy": pack["recall_policy"],
                "local_context_policy": pack["local_context_policy"],
                "used_local_context_tokens": pack["used_local_context_tokens"],
                "used_remote_context_tokens": pack["used_remote_context_tokens"],
                "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                "primary_candidate_count": len(primary_matches),
                "auxiliary_candidate_count": len(auxiliary_matches),
                "created_at_ms": now_ms(),
            }
        )
        return pack

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        context_pack_id = require_string(args, "context_pack_id")
        self.flush_audits()
        return {
            "context_pack_id": context_pack_id,
            "events": self.read_all(),
        }


class MatrixArkTemporalStoreDirectAdapter(MatrixArkLocalAdapter):
    """MatrixArk storage adapter backed by the native C++ TemporalStore SDK.

    The MCP extraction, node/summary/event mapping, traversal scoring, feedback,
    and replay logic still live in this process. Only the record log boundary is
    replaced: every MatrixArk record is persisted as a TemporalStore hash field.
    New prefixes use a compact sharded append log: hash field = zero-padded
    sequence within a shard, hash key = records:<shard>, and a tiny string key
    stores the global record count. Older prefixes that still have a JSON
    record_index are read through the legacy path.
    """

    def __init__(
        self,
        *,
        metaserver: str,
        namespace: str,
        table: str,
        library_path: str = "",
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        super().__init__(Path("/tmp/matrixark-mcp-unused-direct.jsonl"))
        sdk_root = Path(__file__).resolve().parents[1] / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=metaserver,
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        )
        self._client = Client(options, library_path=library_path or None)
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)

    def __post_init__(self) -> None:
        # Direct adapter does not use the inherited JSONL path.
        return

    def _backend_label(self) -> str:
        return "temporalstore-cpp"

    def backend_metrics(self) -> Json:
        return {
            "backend": self._backend_label(),
            "metrics_format": "json",
            "metrics": {
                "mode": "direct-sdk",
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "entry_count_cache": self._entry_count_cache,
                "records_cache_ready": self._records_cache is not None,
            },
        }

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        with self._readiness_lock:
            if self._readiness_cache and self._readiness_cache.get("status") == "ready":
                cached = dict(self._readiness_cache)
                cached["cached"] = True
                cached["reason"] = reason
                return cached
            timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
            deadline = time.monotonic() + timeout / 1000.0
            attempts: list[Json] = []
            attempt = 0
            warmup_key = f"{self._storage_prefix}:readiness"
            warmup_field = f"{stable_hash(f'{self._storage_prefix}:{reason}'):020d}"
            warmup_value = json.dumps(
                {
                    "probe": "matrixark_backend_ready",
                    "backend": self._backend_label(),
                    "reason": reason,
                    "ts_ms": now_ms(),
                },
                sort_keys=True,
            )
            while True:
                attempt += 1
                checks: Json = {
                    "mcp_process_started": True,
                    "metaserver_reachable": metaserver_reachable(self._metaserver),
                    "namespace_table_opened": False,
                    "slot_coverage_verified_by_warmup_hset_hget": False,
                }
                try:
                    if not checks["metaserver_reachable"].get("ok"):
                        raise MatrixArkError(checks["metaserver_reachable"].get("error", "metaserver is not reachable"))
                    if probe:
                        self._client.hset(warmup_key, warmup_field, warmup_value)
                        checks["namespace_table_opened"] = True
                        readback = self._client.hget(warmup_key, warmup_field)
                        if readback != warmup_value:
                            raise MatrixArkError("readiness warmup readback mismatch")
                        checks["slot_coverage_verified_by_warmup_hset_hget"] = True
                    else:
                        checks["namespace_table_opened"] = True
                    result: Json = {
                        "status": "ready",
                        "backend": self._backend_label(),
                        "reason": reason,
                        "probe": bool(probe),
                        "attempts": attempt,
                        "attempt_log": attempts,
                        "topology": {
                            "metaserver": self._metaserver,
                            "namespace": self._namespace,
                            "table": self._table,
                            "storage_prefix": self._storage_prefix,
                            "warmup_key": warmup_key,
                            "warmup_field": warmup_field,
                        },
                        "checks": checks,
                    }
                    self._readiness_cache = result
                    return dict(result)
                except Exception as exc:
                    retryable = is_retryable_temporalstore_error(exc)
                    attempts.append({"attempt": attempt, "ok": False, "retryable": retryable, "error": str(exc), "checks": checks})
                    if not retryable or time.monotonic() >= deadline:
                        return {
                            "status": "topology_not_ready",
                            "backend": self._backend_label(),
                            "reason": reason,
                            "probe": bool(probe),
                            "attempts": attempt,
                            "attempt_log": attempts,
                            "error": str(exc),
                            "topology": {
                                "metaserver": self._metaserver,
                                "namespace": self._namespace,
                                "table": self._table,
                                "storage_prefix": self._storage_prefix,
                                "warmup_key": warmup_key,
                                "warmup_field": warmup_field,
                            },
                            "checks": checks,
                        }
                    time.sleep(max(0.05, BACKEND_READINESS_BACKOFF_MS / 1000.0))

    def _get_index(self) -> list[str]:
        try:
            raw = self._client.get_string(self._index_key)
        except Exception:
            return []
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(value, list):
            return []
        return [str(item) for item in value]

    def _get_count(self) -> int:
        try:
            raw = self._client.get_string(self._count_key)
        except Exception:
            return 0
        if not raw:
            return 0
        try:
            value = int(raw)
        except ValueError:
            return 0
        return max(0, value)

    def append(self, record: Json) -> None:
        self.append_many([record])

    def append_many(self, records: list[Json]) -> None:
        if not records:
            return
        with self._records_lock:
            if self._records_cache is None:
                self.read_all()
            assert self._records_cache is not None
            if self._legacy_index_mode:
                if self._index_cache is None:
                    self._index_cache = self._get_index()
                entries: list[Json] = []
                for record in records:
                    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                    record_id = (
                        f"{len(self._index_cache):020d}:"
                        f"{record.get('record_type', 'record')}:"
                        f"{stable_hash(json.dumps(record, sort_keys=True))}"
                    )
                    entries.append({"key": self._record_hash_key, "field": record_id, "value": payload})
                    self._index_cache.append(record_id)
                self._hset_many_with_backoff(entries)
                self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                self._records_cache.extend(records)
                self._put_direct_record_cache(len(self._records_cache), self._records_cache)
                return

            sequence = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
            entries = []
            for bundle in self._record_bundles(records):
                record_key, record_id = self._record_location(sequence)
                payload_value: Json
                payload_value = bundle[0] if len(bundle) == 1 else {"record_bundle": bundle}
                payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
                entries.append({"key": record_key, "field": record_id, "value": payload})
                sequence += 1
            self._hset_many_with_backoff(entries)
            self._put_string_with_backoff(self._count_key, str(sequence))
            self._entry_count_cache = sequence
            self._records_cache.extend(records)
            self._put_direct_record_cache(self._entry_count_cache, self._records_cache)

    def append_audit(self, record: Json) -> None:
        if self._audit_mode == "drop":
            _mcp_debug_log("matrixark audit record dropped by MATRIXARK_DIRECT_AUDIT_MODE=drop")
            return
        if self._audit_mode == "sync":
            self.append(record)
            return
        with self._audit_lock:
            self._audit_buffer.append(record)
            if self._audit_mode == "buffered":
                self._ensure_audit_flusher_locked()
            max_pending = self._audit_buffer_max_records * 4
            if len(self._audit_buffer) > max_pending:
                dropped = len(self._audit_buffer) - max_pending
                self._audit_buffer = self._audit_buffer[-max_pending:]
                _mcp_debug_log(f"matrixark audit buffer dropped {dropped} oldest records after flush lag")

    def _hset_with_backoff(self, key: str, field: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.hset(key, field, value), op="hset")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _hset_many_with_backoff(self, entries: list[Json]) -> None:
        if not entries:
            return
        batch_hset = getattr(self._client, "batch_hset", None)
        if callable(batch_hset):
            self._write_with_backoff(lambda: batch_hset(entries), op="batch_hset")
            if self._write_throttle_s > 0:
                time.sleep(self._write_throttle_s)
            return
        for entry in entries:
            self._hset_with_backoff(str(entry["key"]), str(entry["field"]), str(entry["value"]))

    def _put_string_with_backoff(self, key: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.put_string(key, value), op="put_string")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _write_with_backoff(self, fn: Any, *, op: str) -> None:
        attempt = 0
        while True:
            try:
                fn()
                return
            except Exception:
                if attempt >= self._write_retries:
                    raise
                sleep_s = self._write_backoff_s * (2**attempt)
                if sleep_s > 0:
                    time.sleep(sleep_s)
                attempt += 1

    def flush_audits(self) -> None:
        with self._audit_lock:
            if not self._audit_buffer:
                return
            records = self._audit_buffer
            self._audit_buffer = []
        try:
            self.append_many(records)
        except Exception as exc:
            with self._audit_lock:
                self._audit_flush_failures += 1
                remaining_capacity = max(0, self._audit_buffer_max_records * 2 - len(self._audit_buffer))
                if remaining_capacity:
                    self._audit_buffer = records[-remaining_capacity:] + self._audit_buffer
            _mcp_debug_log(f"matrixark audit flush failed: {exc}")

    def _ensure_audit_flusher_locked(self) -> None:
        if self._audit_flusher_started:
            return
        self._audit_flusher_started = True
        thread = threading.Thread(target=self._audit_flush_loop, name="matrixark-audit-flusher", daemon=True)
        thread.start()

    def _audit_flush_loop(self) -> None:
        while True:
            time.sleep(self._audit_flush_interval_s)
            try:
                self.flush_audits()
            except Exception as exc:
                _mcp_debug_log(f"matrixark audit flush loop failed: {exc}")

    def _record_bundles(self, records: list[Json]) -> list[list[Json]]:
        bundles: list[list[Json]] = []
        current: list[Json] = []
        current_bytes = 0
        max_bytes = max(8192, DIRECT_RECORD_BUNDLE_MAX_BYTES)
        for record in records:
            record_bytes = len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            if current and current_bytes + record_bytes > max_bytes:
                bundles.append(current)
                current = []
                current_bytes = 0
            current.append(record)
            current_bytes += record_bytes
        if current:
            bundles.append(current)
        return bundles

    def read_all(self) -> list[Json]:
        with self._records_lock:
            if self._records_cache is not None:
                return list(self._records_cache)
            count = self._get_count()
            if count > 0:
                self._legacy_index_mode = False
                self._entry_count_cache = count
                cached = self._get_direct_record_cache(count)
                if cached is not None:
                    self._records_cache = cached
                    return list(self._records_cache)
                with self._direct_record_load_lock():
                    cached = self._get_direct_record_cache(count)
                    if cached is not None:
                        self._records_cache = cached
                        return list(self._records_cache)
                    self._records_cache = self._load_records_by_count(count)
                    self._put_direct_record_cache(count, self._records_cache)
                    return list(self._records_cache)
            index = self._get_index()
            self._index_cache = index
            self._legacy_index_mode = bool(index)
            self._entry_count_cache = None
            self._records_cache = self._load_records(index)
            return list(self._records_cache)

    def _direct_record_load_lock(self) -> threading.RLock:
        with _DIRECT_RECORD_CACHE_LOCK:
            lock = _DIRECT_RECORD_LOAD_LOCKS.get(self._storage_prefix)
            if lock is None:
                lock = threading.RLock()
                _DIRECT_RECORD_LOAD_LOCKS[self._storage_prefix] = lock
            return lock

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        with _DIRECT_RECORD_CACHE_LOCK:
            cached = _DIRECT_RECORD_CACHE.get(self._storage_prefix)
            if cached is None:
                return None
            cached_count, records = cached
            if cached_count != count:
                return None
            return list(records)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        with _DIRECT_RECORD_CACHE_LOCK:
            if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and self._storage_prefix not in _DIRECT_RECORD_CACHE:
                oldest = next(iter(_DIRECT_RECORD_CACHE))
                _DIRECT_RECORD_CACHE.pop(oldest, None)
            _DIRECT_RECORD_CACHE[self._storage_prefix] = (count, list(records))

    def _load_records_by_count(self, count: int) -> list[Json]:
        records = []
        batch_hget = getattr(self._client, "batch_hget", None)
        if callable(batch_hget):
            entries = []
            for sequence in range(count):
                record_key, record_id = self._record_location(sequence)
                entries.append({"key": record_key, "field": record_id})
            try:
                read_records = batch_hget(entries)
            except Exception:
                read_records = []
            for item in read_records:
                if not isinstance(item, dict):
                    continue
                payload = item.get("value", "")
                if not payload:
                    continue
                decoded = json.loads(str(payload))
                if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                    records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
                elif isinstance(decoded, dict):
                    records.append(decoded)
            if records or count == 0:
                return records
        for sequence in range(count):
            record_key, record_id = self._record_location(sequence)
            try:
                payload = self._client.hget(record_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            decoded = json.loads(payload)
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _record_location(self, sequence: int) -> tuple[str, str]:
        shard = sequence // self._shard_size
        offset = sequence % self._shard_size
        return f"{self._record_hash_key}:{shard:06d}", f"{offset:020d}"

    def _load_records(self, index: list[str]) -> list[Json]:
        records = []
        for record_id in index:
            try:
                payload = self._client.hget(self._record_hash_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            records.append(json.loads(payload))
        return records


class MatrixArkRustCliClient:
    """Persistent process boundary around the Rust TemporalStore SDK.

    The Rust binary owns direct SDK linkage and runs in JSON-lines serve mode.
    Keeping one process alive avoids spawning the CLI and reconnecting the Rust
    SDK for every hset/hget, which was the main Rust MCP latency source.
    """

    def __init__(
        self,
        *,
        cli_path: str,
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
    ) -> None:
        if not cli_path:
            raise MatrixArkError("--rust-cli or MATRIXARK_TEMPORALSTORE_RUST_CLI is required for temporalstore-rust")
        self.cli_path = cli_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self._lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None

    def close(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    def _ensure_proc(self) -> subprocess.Popen[str]:
        if self._proc is not None and self._proc.poll() is None:
            return self._proc
        self.close()
        self._proc = subprocess.Popen(
            [self.cli_path, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        return self._proc

    def _read_json_line(self, proc: subprocess.Popen[str], op: str) -> Json:
        assert proc.stdout is not None
        deadline = time.monotonic() + max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise MatrixArkError(f"Rust TemporalStore {op} process exited ({proc.returncode}): {stderr[-1000:]}")
            ready, _, _ = select.select([proc.stdout], [], [], 0.05)
            if not ready:
                continue
            line = proc.stdout.readline()
            if not line:
                continue
            if not line.strip().startswith("{"):
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise MatrixArkError(f"Rust TemporalStore {op} returned invalid JSON: {line[:200]!r}") from exc
        raise MatrixArkError(
            f"Rust TemporalStore {op} timed out waiting for response from {self.cli_path} "
            f"after {max(2.0, self.request_timeout_ms / 1000.0 + 2.0):.1f}s"
        )

    def _call_json(self, op: str, **kwargs: Any) -> Json:
        command = {
            "op": op,
            "metaserver": self.metaserver,
            "namespace": self.namespace,
            "table": self.table,
            "request_timeout_ms": self.request_timeout_ms,
            "io_timeout_ms": self.io_timeout_ms,
            **kwargs,
        }
        payload = json.dumps(command, separators=(",", ":")) + "\n"
        with self._lock:
            proc = self._ensure_proc()
            assert proc.stdin is not None
            try:
                proc.stdin.write(payload)
                proc.stdin.flush()
            except BrokenPipeError as exc:
                self.close()
                raise MatrixArkError(f"Rust TemporalStore {op} pipe closed") from exc
            response = self._read_json_line(proc, op)
        if not response.get("ok"):
            raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
        return response

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def put_string(self, key: str, value: str) -> None:
        self._call("put_string", key=key, value=value)

    def get_string(self, key: str) -> str:
        return self._call("get_string", key=key)

    def hset(self, key: str, field: str, value: str) -> None:
        self._call("hset", key=key, field=field, value=value)

    def hget(self, key: str, field: str) -> str:
        return self._call("hget", key=key, field=field)

    def batch_hset(self, entries: list[Json]) -> None:
        if not entries:
            return
        self._call_json("batch_hset", entries=entries)

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        response = self._call_json("batch_hget", entries=entries)
        records = response.get("records", [])
        return records if isinstance(records, list) else []

    def scan_hash(self, key: str) -> Json:
        return self._call_json("scan_hash", key=key)

    def metrics_prometheus(self) -> str:
        return str(self._call_json("metrics_prometheus").get("prometheus", ""))

    def health(self) -> Json:
        return self._call_json("health")

    def readiness(self) -> Json:
        return self._call_json("readiness")

    def shutdown(self) -> None:
        try:
            self._call_json("shutdown")
        finally:
            self.close()


class MatrixArkTemporalStoreRustAdapter(MatrixArkTemporalStoreDirectAdapter):
    """MatrixArk record-log adapter backed by the Rust TemporalStore SDK."""

    def __init__(
        self,
        *,
        rust_cli: str,
        metaserver: str,
        namespace: str,
        table: str,
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        MatrixArkLocalAdapter.__init__(self, Path("/tmp/matrixark-mcp-unused-rust.jsonl"))
        self._client = MatrixArkRustCliClient(
            cli_path=rust_cli,
            metaserver=metaserver,
            namespace=namespace,
            table=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
        )
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)

    def _backend_label(self) -> str:
        return "temporalstore-rust"

    def backend_metrics(self) -> Json:
        health: Json
        readiness: Json
        try:
            health = self._client.health()
        except Exception as exc:
            health = {"ok": False, "error": str(exc)}
        try:
            readiness = self._client.readiness()
        except Exception as exc:
            readiness = {"ok": False, "error": str(exc)}
        try:
            prometheus = self._client.metrics_prometheus()
        except Exception as exc:
            prometheus = f"# matrixark_rust_gateway_metrics_error {json.dumps(str(exc))}\n"
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": "long_lived_stdio_gateway",
            "health": health,
            "readiness": readiness,
            "prometheus": prometheus,
            "metrics": {
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
            },
        }



class MatrixArkAccessManager:
    """Small MatrixArk product access layer over the same storage adapter.

    It is deliberately simple: API keys authenticate the calling app/service;
    account_id + tenant_id + user_id + session_id isolate context records.
    """

    def __init__(self, adapter: MatrixArkLocalAdapter, *, mode: str = "dev") -> None:
        if mode not in {"dev", "enforced"}:
            raise MatrixArkError("access mode must be dev or enforced")
        self.adapter = adapter
        self.mode = mode

    def authenticate(self, tool_name: str, args: Json) -> Json:
        api_key = optional_string(args, "api_key")
        scope = optional_object(args, "scope")
        required_scopes = MATRIXARK_TOOL_SCOPES.get(tool_name, set())
        if api_key:
            key_record = self.find_active_api_key(api_key)
            if not key_record:
                raise MatrixArkError("invalid or revoked MatrixArk API key")
            scopes = set(key_record.get("scopes", []))
            if not required_scopes.issubset(scopes):
                raise MatrixArkError(f"API key lacks required scope(s): {sorted(required_scopes)}")
            account_id = str(key_record["account_id"])
            tenant_id = str(key_record["tenant_id"])
            requested_account = str(scope.get("account_id", ""))
            requested_tenant = str(scope.get("tenant_id", ""))
            if requested_account and requested_account != account_id:
                raise MatrixArkError("scope.account_id does not match API key account")
            if requested_tenant and requested_tenant != tenant_id:
                raise MatrixArkError("scope.tenant_id does not match API key tenant")
            if required_scopes.intersection(MATRIXARK_CONTEXT_SCOPES):
                self.ensure_account_tenant_active(account_id, tenant_id)
            allowed_user_ids = set(key_record.get("allowed_user_ids", []))
            allowed_session_ids = set(key_record.get("allowed_session_ids", []))
            requested_user = str(scope.get("user_id") or (next(iter(allowed_user_ids)) if len(allowed_user_ids) == 1 else ""))
            requested_session = str(scope.get("session_id") or (next(iter(allowed_session_ids)) if len(allowed_session_ids) == 1 else ""))
            if allowed_user_ids and not requested_user:
                raise MatrixArkError("scope.user_id is required by API key")
            if allowed_session_ids and not requested_session:
                raise MatrixArkError("scope.session_id is required by API key")
            if allowed_user_ids and requested_user not in allowed_user_ids:
                raise MatrixArkError("scope.user_id is not allowed by API key")
            if allowed_session_ids and requested_session not in allowed_session_ids:
                raise MatrixArkError("scope.session_id is not allowed by API key")
            self.ensure_user_active(account_id, tenant_id, requested_user)
            return {
                "mode": "api_key",
                "api_key_id": key_record["api_key_id"],
                "account_id": account_id,
                "tenant_id": tenant_id,
                "scopes": sorted(scopes),
                "role": key_record.get("role", "service"),
                "user_id": requested_user,
                "session_id": requested_session,
                "allowed_user_ids": sorted(allowed_user_ids),
                "allowed_session_ids": sorted(allowed_session_ids),
            }
        if self.mode == "enforced" and required_scopes:
            raise MatrixArkError("MatrixArk API key is required")
        defaults = local_identity_defaults(args, scope)
        account_id = str(defaults["account_id"])
        tenant_id = str(defaults["tenant_id"])
        return {
            "mode": "dev",
            "api_key_id": "dev",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": sorted(MATRIXARK_ALL_SCOPES),
            "role": "dev_admin",
            "user_id": str(defaults["user_id"]),
            "session_id": str(defaults["session_id"]),
            "agent_name": str(defaults["agent_name"]),
        }

    def authorize_and_enrich(self, tool_name: str, args: Json) -> Json:
        identity = self.authenticate(tool_name, args)
        scope = optional_object(args, "scope")
        args["scope"] = enrich_scope_with_identity(scope, identity)
        args["_matrixark_auth"] = {
            "mode": identity["mode"],
            "api_key_id": identity["api_key_id"],
            "account_id": identity["account_id"],
            "tenant_id": identity["tenant_id"],
            "role": identity["role"],
        }
        if identity["mode"] == "api_key":
            self.append_api_key_usage(tool_name, identity, args["scope"])
        return identity

    def find_active_api_key(self, api_key: str) -> Json | None:
        hashed = secret_hash(api_key)
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") != "matrixark_api_key":
                continue
            if record.get("api_key_hash") == hashed:
                if record.get("status") != "active":
                    return None
                expires_at_ms = record.get("expires_at_ms")
                if isinstance(expires_at_ms, int) and expires_at_ms <= now_ms():
                    return None
                return record
        return None

    def latest_account_record(self, account_id: str) -> Json | None:
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") == "matrixark_account" and record.get("account_id") == account_id:
                return record
        return None

    def latest_tenant_record(self, account_id: str, tenant_id: str) -> Json | None:
        for record in reversed(self.adapter.read_all()):
            if (
                record.get("record_type") == "matrixark_tenant"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
            ):
                return record
        return None

    def ensure_account_tenant_active(self, account_id: str, tenant_id: str) -> None:
        account = self.latest_account_record(account_id)
        if account and account.get("status") != "active":
            raise MatrixArkError("account is disabled")
        tenant = self.latest_tenant_record(account_id, tenant_id)
        if tenant and tenant.get("status") != "active":
            raise MatrixArkError("tenant is disabled")

    def latest_api_key_record(self, api_key_id: str) -> Json | None:
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") == "matrixark_api_key" and record.get("api_key_id") == api_key_id:
                return record
        return None

    def latest_user_record(self, account_id: str, tenant_id: str, user_id: str) -> Json | None:
        if not user_id:
            return None
        for record in reversed(self.adapter.read_all()):
            if (
                record.get("record_type") == "matrixark_user"
                and record.get("account_id") == account_id
                and record.get("tenant_id") == tenant_id
                and record.get("user_id") == user_id
            ):
                return record
        return None

    def ensure_user_active(self, account_id: str, tenant_id: str, user_id: str) -> None:
        record = self.latest_user_record(account_id, tenant_id, user_id)
        if record and record.get("status") != "active":
            raise MatrixArkError("scope.user_id is disabled")

    def append_audit(self, action: str, identity: Json, *, status: str, details: Json | None = None) -> None:
        self.adapter.append(
            {
                "record_type": "matrixark_audit_log",
                "audit_id_hash": stable_hash(f"{action}:{identity.get('api_key_id')}:{now_ms()}"),
                "action": action,
                "status": status,
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "api_key_id": identity.get("api_key_id", ""),
                "role": identity.get("role", ""),
                "details": details or {},
                "created_at_ms": now_ms(),
            }
        )

    def append_api_key_usage(self, action: str, identity: Json, scope: Json) -> None:
        self.adapter.append(
            {
                "record_type": "matrixark_api_key_usage",
                "usage_id_hash": stable_hash(
                    f"{identity.get('api_key_id')}:{action}:{scope.get('user_id', '')}:{scope.get('session_id', '')}:{now_ms()}"
                ),
                "action": action,
                "api_key_id": identity.get("api_key_id", ""),
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "role": identity.get("role", ""),
                "user_id": scope.get("user_id", ""),
                "session_id": scope.get("session_id", ""),
                "tenant_hash": scope.get("tenant_hash", 0),
                "user_hash": scope.get("user_hash", 0),
                "session_hash": scope.get("session_hash", 0),
                "used_at_ms": now_ms(),
            }
        )

    def ensure_identity_can_manage(self, identity: Json, account_id: str, tenant_id: str) -> None:
        if identity.get("mode") == "dev":
            return
        if identity.get("account_id") != account_id or identity.get("tenant_id") != tenant_id:
            raise MatrixArkError("admin operation account/tenant does not match API key")

    def create_account(self, args: Json, identity: Json) -> Json:
        account_id = canonical_account_id(optional_string(args, "account_id") or f"acct_{stable_hash(optional_string(args, 'account_name', 'account'))}")
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or "tenant_default")
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        account_name = optional_string(args, "account_name", account_id)
        tenant_name = optional_string(args, "tenant_name", tenant_id)
        self.adapter.append(
            {
                "record_type": "matrixark_account",
                "account_id": account_id,
                "account_name": account_name,
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.adapter.append(
            {
                "record_type": "matrixark_tenant",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                **identity_hashes(account_id, tenant_id),
                "status": "active",
                "created_by_api_key_id": identity.get("api_key_id", ""),
                "created_at_ms": now_ms(),
            }
        )
        self.append_audit("admin.create_account", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id})
        return {"status": "created", "account_id": account_id, "tenant_id": tenant_id}

    def update_account(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        current_account = self.latest_account_record(account_id) or {}
        current_tenant = self.latest_tenant_record(account_id, tenant_id) or {}
        account_status = optional_string(args, "account_status", str(current_account.get("status") or "active"))
        tenant_status = optional_string(args, "tenant_status", str(current_tenant.get("status") or "active"))
        if account_status not in {"active", "disabled"}:
            raise MatrixArkError("account_status must be active or disabled")
        if tenant_status not in {"active", "disabled"}:
            raise MatrixArkError("tenant_status must be active or disabled")
        account_name = optional_string(args, "account_name", str(current_account.get("account_name") or account_id))
        tenant_name = optional_string(args, "tenant_name", str(current_tenant.get("tenant_name") or tenant_id))
        account_record = {
            "record_type": "matrixark_account",
            "account_id": account_id,
            "account_name": account_name,
            "status": account_status,
            "created_by_api_key_id": current_account.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current_account.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        tenant_record = {
            "record_type": "matrixark_tenant",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            **identity_hashes(account_id, tenant_id),
            "status": tenant_status,
            "created_by_api_key_id": current_tenant.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current_tenant.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        self.adapter.append(account_record)
        self.adapter.append(tenant_record)
        self.append_audit(
            "admin.update_account",
            identity,
            status="ok",
            details={"account_id": account_id, "tenant_id": tenant_id, "account_status": account_status, "tenant_status": tenant_status},
        )
        return {
            "status": "updated",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "account_status": account_status,
            "tenant_status": tenant_status,
            "tenant_hash": tenant_record["tenant_hash"],
        }

    def list_accounts(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        requested_account = optional_string(args, "account_id", "")
        requested_tenant = optional_string(args, "tenant_id", "")
        if identity.get("mode") != "dev":
            requested_account = identity["account_id"]
            requested_tenant = requested_tenant or identity["tenant_id"]
        latest_accounts: dict[str, Json] = {}
        latest_tenants: dict[tuple[str, str], Json] = {}
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") == "matrixark_account":
                account_id = str(record.get("account_id", ""))
                if not account_id or account_id in latest_accounts:
                    continue
                if requested_account and account_id != requested_account:
                    continue
                latest_accounts[account_id] = record
            elif record.get("record_type") == "matrixark_tenant":
                account_id = str(record.get("account_id", ""))
                tenant_id = str(record.get("tenant_id", ""))
                key = (account_id, tenant_id)
                if not account_id or not tenant_id or key in latest_tenants:
                    continue
                if requested_account and account_id != requested_account:
                    continue
                if requested_tenant and tenant_id != requested_tenant:
                    continue
                latest_tenants[key] = record
        rows = []
        for (account_id, tenant_id), tenant in latest_tenants.items():
            account = latest_accounts.get(account_id) or self.latest_account_record(account_id) or {}
            rows.append(
                {
                    "account_id": account_id,
                    "account_name": account.get("account_name", ""),
                    "account_status": account.get("status", ""),
                    "tenant_id": tenant_id,
                    "tenant_name": tenant.get("tenant_name", ""),
                    "tenant_status": tenant.get("status", ""),
                    "tenant_hash": tenant.get("tenant_hash", 0),
                    "created_at_ms": tenant.get("created_at_ms", 0),
                    "updated_at_ms": tenant.get("updated_at_ms", 0),
                }
            )
            if len(rows) >= limit:
                break
        return {"status": "ok", "accounts": rows, "count": len(rows)}

    def create_user(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        user_id = require_string(args, "user_id")
        display_name = optional_string(args, "display_name", user_id)
        external_subject = optional_string(args, "external_subject", "")
        status = optional_string(args, "status", "active")
        if status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        record = {
            "record_type": "matrixark_user",
            "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "display_name": display_name,
            "external_subject": external_subject,
            **identity_hashes(account_id, tenant_id, user_id),
            "status": status,
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit("admin.create_user", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id})
        return {
            "status": "created",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "user_hash": record["user_hash"],
        }

    def update_user(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        user_id = require_string(args, "user_id")
        current = self.latest_user_record(account_id, tenant_id, user_id) or {}
        status = optional_string(args, "status", str(current.get("status") or "active"))
        if status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        display_name = optional_string(args, "display_name", str(current.get("display_name") or user_id))
        external_subject = optional_string(args, "external_subject", str(current.get("external_subject") or ""))
        record = {
            "record_type": "matrixark_user",
            "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "display_name": display_name,
            "external_subject": external_subject,
            **identity_hashes(account_id, tenant_id, user_id),
            "status": status,
            "created_by_api_key_id": current.get("created_by_api_key_id", identity.get("api_key_id", "")),
            "created_at_ms": current.get("created_at_ms", now_ms()),
            "updated_by_api_key_id": identity.get("api_key_id", ""),
            "updated_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit("admin.update_user", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "user_status": status})
        return {"status": "updated", "account_id": account_id, "tenant_id": tenant_id, "user_id": user_id, "user_status": status, "user_hash": record["user_hash"]}

    def list_users(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        status_filter = optional_string(args, "status", "")
        if status_filter and status_filter not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        latest: dict[str, Json] = {}
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") != "matrixark_user":
                continue
            if record.get("account_id") != account_id or record.get("tenant_id") != tenant_id:
                continue
            user_id = str(record.get("user_id", ""))
            if not user_id or user_id in latest:
                continue
            if status_filter and record.get("status") != status_filter:
                continue
            latest[user_id] = {
                "user_id": user_id,
                "display_name": record.get("display_name", ""),
                "external_subject": record.get("external_subject", ""),
                "status": record.get("status", ""),
                "user_hash": record.get("user_hash", 0),
                "created_at_ms": record.get("created_at_ms", 0),
                "updated_at_ms": record.get("updated_at_ms", 0),
            }
            if len(latest) >= limit:
                break
        return {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "users": list(latest.values()), "count": len(latest)}

    def create_api_key(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        scopes = optional_string_list(args, "scopes", ["context:ingest", "context:retrieve", "context:feedback", "context:replay"])
        if not scopes:
            raise MatrixArkError("scopes must not be empty")
        unknown_scopes = sorted(set(scopes) - MATRIXARK_ALL_SCOPES)
        if unknown_scopes:
            raise MatrixArkError(f"unknown MatrixArk scope(s): {unknown_scopes}")
        role = optional_string(args, "role", "service")
        display_name = optional_string(args, "display_name", role)
        allowed_user_ids = sorted(set(optional_string_list(args, "allowed_user_ids", [])))
        allowed_session_ids = sorted(set(optional_string_list(args, "allowed_session_ids", [])))
        expires_at_ms = args.get("expires_at_ms")
        if expires_at_ms is not None:
            if not isinstance(expires_at_ms, int) or expires_at_ms <= now_ms():
                raise MatrixArkError("expires_at_ms must be a future unix timestamp in milliseconds")
        key_prefix = optional_string(args, "key_prefix", "mk_test")
        api_key = make_api_key(key_prefix)
        api_key_id = f"key_{stable_hash(api_key)}"
        record = {
            "record_type": "matrixark_api_key",
            "api_key_id": api_key_id,
            "api_key_hash": secret_hash(api_key),
            "account_id": account_id,
            "tenant_id": tenant_id,
            **identity_hashes(account_id, tenant_id),
            "scopes": sorted(set(scopes)),
            "role": role,
            "display_name": display_name,
            "allowed_user_ids": allowed_user_ids,
            "allowed_session_ids": allowed_session_ids,
            "expires_at_ms": expires_at_ms,
            "status": "active",
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit(
            "admin.create_api_key",
            identity,
            status="ok",
            details={
                "api_key_id": api_key_id,
                "account_id": account_id,
                "tenant_id": tenant_id,
                "allowed_user_count": len(allowed_user_ids),
                "allowed_session_count": len(allowed_session_ids),
                "expires_at_ms": expires_at_ms,
            },
        )
        return {
            "status": "created",
            "api_key": api_key,
            "api_key_id": api_key_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": record["scopes"],
            "role": role,
            "allowed_user_ids": allowed_user_ids,
            "allowed_session_ids": allowed_session_ids,
            "expires_at_ms": expires_at_ms,
            "warning": "Store api_key now. MatrixArk only stores its hash.",
        }

    def apply_api_key(self, args: Json, identity: Json) -> Json:
        """One-call local application flow for agent/API-key setup.

        In local/dev mode this lets Codex, Claude, Cursor, or another host agent
        ask for a usable MatrixArk key without first hand-creating account,
        tenant, and user records. Enforced deployments still require an admin
        key because this tool is protected by admin scopes.
        """

        scope = optional_object(args, "scope")
        defaults = local_identity_defaults(args, scope)
        account_id = canonical_account_id(optional_string(args, "account_id") or str(defaults["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(defaults["tenant_id"]))
        user_id = optional_string(args, "user_id") or str(scope.get("user_id") or defaults["user_id"])
        agent_name = safe_identifier(optional_string(args, "agent_name") or str(defaults["agent_name"]), default="local_agent")
        self.ensure_identity_can_manage(identity, account_id, tenant_id)

        created_records: list[str] = []
        if self.latest_account_record(account_id) is None:
            self.adapter.append(
                {
                    "record_type": "matrixark_account",
                    "account_id": account_id,
                    "account_name": optional_string(args, "account_name", account_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("account")
        if self.latest_tenant_record(account_id, tenant_id) is None:
            self.adapter.append(
                {
                    "record_type": "matrixark_tenant",
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "tenant_name": optional_string(args, "tenant_name", agent_name),
                    "agent_name": agent_name,
                    **identity_hashes(account_id, tenant_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("tenant")
        if user_id and self.latest_user_record(account_id, tenant_id, user_id) is None:
            self.adapter.append(
                {
                    "record_type": "matrixark_user",
                    "user_record_hash": stable_hash(f"{account_id}:{tenant_id}:user:{user_id}"),
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "display_name": optional_string(args, "display_name", user_id),
                    "external_subject": optional_string(args, "external_subject", f"local:{user_id}"),
                    **identity_hashes(account_id, tenant_id, user_id),
                    "status": "active",
                    "created_by_api_key_id": identity.get("api_key_id", ""),
                    "created_at_ms": now_ms(),
                }
            )
            created_records.append("user")

        allow_all_users = bool(args.get("allow_all_users", False))
        key_args: Json = {
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scopes": optional_string_list(
                args,
                "scopes",
                ["context:ingest", "context:retrieve", "context:feedback", "context:replay", "resource:read", "skill:read"],
            ),
            "role": optional_string(args, "role", "local_agent"),
            "display_name": optional_string(args, "key_display_name", f"{agent_name} local key"),
            "allowed_user_ids": []
            if allow_all_users
            else sorted(set(optional_string_list(args, "allowed_user_ids", [user_id] if user_id else []))),
            "allowed_session_ids": sorted(set(optional_string_list(args, "allowed_session_ids", []))),
            "expires_at_ms": args.get("expires_at_ms"),
            "key_prefix": optional_string(args, "key_prefix", "mk_local"),
        }
        created_key = self.create_api_key(key_args, identity)
        local_scope = enrich_scope_with_identity(
            {
                **scope,
                "agent_name": agent_name,
                "user_id": user_id,
            },
            {
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "session_id": str(scope.get("session_id") or ""),
                "agent_name": agent_name,
            },
        )
        self.append_audit(
            "admin.apply_api_key",
            identity,
            status="ok",
            details={
                "api_key_id": created_key["api_key_id"],
                "account_id": account_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "agent_name": agent_name,
                "created_records": created_records,
            },
        )
        return {
            **created_key,
            "status": "applied",
            "created_records": created_records,
            "local_scope": local_scope,
            "default_node_path": self.adapter.default_session_node_path(local_scope),
        }

    def revoke_api_key(self, args: Json, identity: Json, *, action: str = "admin.revoke_api_key") -> Json:
        api_key_id = require_string(args, "api_key_id")
        record = self.latest_api_key_record(api_key_id)
        if not record or record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        revoked = {
            **record,
            "record_type": "matrixark_api_key",
            "status": "revoked",
            "revoked_by_api_key_id": identity.get("api_key_id", ""),
            "revoked_at_ms": now_ms(),
        }
        self.adapter.append(revoked)
        self.append_audit(action, identity, status="ok", details={"api_key_id": api_key_id})
        return {"status": "revoked", "api_key_id": api_key_id}

    def rotate_api_key(self, args: Json, identity: Json) -> Json:
        old_api_key_id = require_string(args, "api_key_id")
        old_record = self.latest_api_key_record(old_api_key_id)
        if old_record is None or old_record.get("status") != "active":
            raise MatrixArkError("active api_key_id not found")
        self.revoke_api_key({"api_key_id": old_api_key_id}, identity, action="admin.rotate_api_key.revoke_old")
        created = self.create_api_key(
            {
                "account_id": old_record["account_id"],
                "tenant_id": old_record["tenant_id"],
                "scopes": list(old_record.get("scopes", [])),
                "role": old_record.get("role", "service"),
                "display_name": old_record.get("display_name", old_record.get("role", "service")),
                "allowed_user_ids": list(old_record.get("allowed_user_ids", [])),
                "allowed_session_ids": list(old_record.get("allowed_session_ids", [])),
                "expires_at_ms": old_record.get("expires_at_ms"),
                "key_prefix": optional_string(args, "key_prefix", "mk_test"),
            },
            identity,
        )
        self.append_audit("admin.rotate_api_key", identity, status="ok", details={"old_api_key_id": old_api_key_id, "new_api_key_id": created["api_key_id"]})
        return {"status": "rotated", "old_api_key_id": old_api_key_id, **created}

    def list_api_keys(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        include_revoked = bool(args.get("include_revoked", False))
        latest: dict[str, Json] = {}
        for record in reversed(self.adapter.read_all()):
            if record.get("record_type") != "matrixark_api_key":
                continue
            if record.get("account_id") != account_id or record.get("tenant_id") != tenant_id:
                continue
            api_key_id = str(record.get("api_key_id", ""))
            if not api_key_id or api_key_id in latest:
                continue
            if record.get("status") == "revoked" and not include_revoked:
                continue
            expires_at_ms = record.get("expires_at_ms")
            effective_status = record.get("status", "")
            if effective_status == "active" and isinstance(expires_at_ms, int) and expires_at_ms <= now_ms():
                effective_status = "expired"
            latest[api_key_id] = {
                "api_key_id": api_key_id,
                "status": effective_status,
                "role": record.get("role", ""),
                "display_name": record.get("display_name", ""),
                "scopes": record.get("scopes", []),
                "allowed_user_ids": record.get("allowed_user_ids", []),
                "allowed_session_ids": record.get("allowed_session_ids", []),
                "expires_at_ms": expires_at_ms,
                "created_at_ms": record.get("created_at_ms", 0),
                "revoked_at_ms": record.get("revoked_at_ms", 0),
            }
            if len(latest) >= limit:
                break
        return {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "api_keys": list(latest.values()), "count": len(latest)}

    def map_sso_user(self, args: Json, identity: Json) -> Json:
        provider = require_string(args, "provider")
        external_user_id = require_string(args, "external_user_id")
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        matrixark_user_id = optional_string(args, "matrixark_user_id") or f"mu_{stable_hash(f'{account_id}:{tenant_id}:{provider}:{external_user_id}')}"
        record = {
            "record_type": "matrixark_sso_user_mapping",
            "mapping_id_hash": stable_hash(f"{account_id}:{tenant_id}:{provider}:{external_user_id}"),
            "account_id": account_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "external_user_id": external_user_id,
            "matrixark_user_id": matrixark_user_id,
            **identity_hashes(account_id, tenant_id, matrixark_user_id),
            "status": "active",
            "created_by_api_key_id": identity.get("api_key_id", ""),
            "created_at_ms": now_ms(),
        }
        self.adapter.append(record)
        self.append_audit("admin.map_sso_user", identity, status="ok", details={"provider": provider, "matrixark_user_id": matrixark_user_id})
        return {"status": "mapped", "matrixark_user_id": matrixark_user_id, "provider": provider, "external_user_id": external_user_id}

    def audit(self, args: Json, identity: Json) -> Json:
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        account_id = optional_string(args, "account_id", identity["account_id"])
        tenant_id = optional_string(args, "tenant_id", identity["tenant_id"])
        rows = [
            record
            for record in reversed(self.adapter.read_all())
            if record.get("record_type") in {"matrixark_audit_log", "matrixark_api_key_usage"}
            and (not account_id or record.get("account_id") == account_id)
            and (not tenant_id or record.get("tenant_id") == tenant_id)
        ][:limit]
        return {"status": "ok", "audit_logs": rows, "count": len(rows)}


MESSAGE_SCHEMA: Json = {
    "type": "object",
    "description": "One chat/tool/system message. Only role and content are required.",
    "required": ["role", "content"],
    "properties": {
        "role": {"type": "string", "enum": ["user", "assistant", "tool", "system"]},
        "content": {"type": "string", "minLength": 1},
        "name": {"type": "string", "description": "Optional human/tool/agent name."},
        "created_at_ms": {
            "type": "integer",
            "description": "Optional source event time. MatrixArk still uses server ingestion time as the primary write key.",
        },
    },
    "additionalProperties": True,
}

SCOPE_SCHEMA: Json = {
    "type": "object",
    "description": "Optional memory scope. Local mode defaults account_id to acct_local, tenant_id to the agent name, and user_id to the local OS account when omitted. Send user_id or session_id when the host agent knows them; both together give the best user and thread grouping.",
    "properties": {
        "account_id": {"type": "string"},
        "tenant_id": {"type": "string", "description": "Tenant/workspace id. In local mode this defaults to tenant_<agent_name>."},
        "agent_name": {"type": "string", "description": "Optional host agent name such as codex, claude, cursor, or local test. Used to derive the local tenant when tenant_id is omitted."},
        "user_id": {
            "type": "string",
            "description": "Optional user memory scope. In local mode this defaults to the local OS account. Useful alone, and stronger when paired with session_id.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional thread/run/session scope. Useful alone, and strongest when paired with user_id.",
        },
        "team": {"type": "string"},
        "project": {"type": "string"},
    },
    "additionalProperties": True,
}

METADATA_SCHEMA: Json = {
    "type": "object",
    "description": "Optional routing and evidence hints. MatrixArk can infer or fill these internally.",
    "properties": {
        "source": {"type": "string", "description": "Optional source name such as cursor, codex, tool, or resource parser."},
        "node_path": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional tree/path hint. MatrixArk may choose its own internal ContextNode path.",
        },
        "reply_to_message_id": {"type": "string", "description": "Optional message linkage for feedback."},
        "reply_to_context_pack_id": {
            "type": "string",
            "description": "Optional ContextPack linkage for confirmation/correction inference.",
        },
    },
    "additionalProperties": True,
}

AGENT_HOOK_SCHEMA: Json = {
    "type": "object",
    "description": "Optional auto-capture metadata from a host-agent hook.",
    "required": ["source", "hook_type", "hook_id", "observed_at_ms", "auto_captured"],
    "properties": {
        "source": {"type": "string", "minLength": 1},
        "hook_type": {
            "type": "string",
            "enum": [
                "before_llm",
                "after_llm",
                "tool_result",
                "resource_added",
                "feedback",
                "session_commit",
            ],
        },
        "hook_id": {"type": "string", "minLength": 1},
        "observed_at_ms": {"type": "integer"},
        "idempotency_key": {"type": "string"},
        "trigger": {"type": "string"},
        "auto_captured": {"type": "boolean"},
    },
    "additionalProperties": True,
}

API_KEY_SCHEMA: Json = {
    "type": "string",
    "description": "Optional MatrixArk API key. Required when access mode is enforced; dev mode allows omitted keys for local testing.",
}

ADMIN_ACCOUNT_PROPERTIES: Json = {
    "api_key": API_KEY_SCHEMA,
    "account_id": {"type": "string", "description": "MatrixArk account/customer id. Generated or defaulted when omitted in dev mode."},
    "account_name": {"type": "string"},
    "tenant_id": {"type": "string", "description": "MatrixArk tenant/workspace id. Defaults to tenant_default for account creation."},
    "tenant_name": {"type": "string"},
    "scope": SCOPE_SCHEMA,
}


TOOLS: list[Json] = [
    {
        "name": "matrixark_ingest",
        "description": "Ingest chat, business, tool, or resource context into MatrixArk.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["message", "feedback", "resource", "skill", "business_data"],
                    "default": "message",
                    "description": "Optional envelope kind. Defaults to message.",
                },
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. The only required top-level field for ingest.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "raw_uri": {"type": "string", "description": "Optional resource URI/path when kind=resource."},
                "resource_type": {"type": "string", "description": "Optional resource type such as md, txt, pdf, url."},
                "wait": {"type": "boolean", "default": True, "description": "For resource/skill imports, wait for parsing and record writes in the local runtime. wait=false records a queued ResourceImportTask."},
                "resource_version": {"type": "string", "description": "Optional caller-supplied resource version. Defaults to parser content version."},
                "supersedes_chunk_hashes": {"type": "object", "description": "Optional map from source_ref or content_hash to the older chunk hash this import supersedes."},
                "deployment_scope": {
                    "type": "string",
                    "enum": ["local", "global", "cloud", "on_prem", "hybrid"],
                    "description": "Optional deployment visibility marker for resource/skill registry records. Defaults to local.",
                },
                "auto_batch_extract": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, commit the same-session buffer once session_buffer_threshold pending events accumulate.",
                },
                "session_buffer_threshold": {
                    "type": "integer",
                    "default": 20,
                    "description": "Pending same-session raw event threshold for automatic one-pass batch extraction.",
                },
                "idle_commit_timeout_ms": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional idle timeout. If previous pending same-session messages are older than this, MatrixArk commits that window before ingesting the new message.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_list_resources",
        "description": "List governed MatrixArk resources visible to the current account/tenant/user/session scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "resource_type": {"type": "string", "description": "Optional filter such as md, txt, pdf."},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_list_skills",
        "description": "List governed MatrixArk skills visible to the current account/tenant/user/session scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "include_disabled": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_update_skill",
        "description": "Update a skill registry entry without rewriting the original SKILL.md manifest.",
        "inputSchema": {
            "type": "object",
            "required": ["skill_hash"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "skill_hash": {"type": "integer"},
                "status": {"type": "string", "enum": ["active", "disabled"]},
                "precedence": {"type": "string", "enum": ["low", "normal", "high", "critical"]},
                "owner_scope": {"type": "string"},
                "version": {"type": "string"},
                "triggers": {"type": "array", "items": {"type": "string"}},
                "allowed_tools": {"type": "array", "items": {"type": "string"}},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_session_commit",
        "description": "Commit pending same-session raw ContextEvents into derived entities, segments, summaries, and indexes without duplicating raw events.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Minimum pending raw events unless force=true. Explicit session_commit defaults to force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": True,
                    "description": "Force commit even below threshold for session end/task complete hooks.",
                },
                "commit_reason": {
                    "type": "string",
                    "enum": ["threshold", "hook_boundary", "idle_timeout", "manual_api"],
                    "description": "Why the session window is being committed.",
                },
                "idle_timeout_ms": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Commit when pending messages are below threshold but the session has been idle for at least this long.",
                },
                "max_messages": {
                    "type": "integer",
                    "description": "Optional cap for how many pending raw events to commit in this batch. Threshold/rolling commits default to threshold_messages.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_refresh_summaries",
        "description": "Background worker entrypoint: refresh dirty ContextNode L0/L1 summaries and embeddings asynchronously from recent events, entities, segments, and child summaries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "limit": {"type": "integer", "default": 64},
                "refreshed_at_ms": {
                    "type": "integer",
                    "description": "Optional deterministic refresh timestamp for tests/replay.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_retrieve",
        "description": "Retrieve a token-budgeted MatrixArk context pack for a raw query.",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "description": "Required raw user or agent query."},
                "scope": SCOPE_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "max_context_tokens": {
                    "type": "integer",
                    "default": 2048,
                    "description": "Optional shared prompt context budget for local plus MatrixArk remote context. Defaults to 2048.",
                },
                "include_superseded_resources": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, retrieval may include older resource versions for historical replay.",
                },
                "local_context": {
                    "type": "array",
                    "description": "Optional local context already selected by Codex/Cursor, such as file snippets, open-buffer summaries, or tool output. MatrixArk dedupes remote refs against this and only fills the remaining budget.",
                    "items": {
                        "oneOf": [
                            {"type": "string"},
                            {
                                "type": "object",
                                "properties": {
                                    "text": {"type": "string"},
                                    "content": {"type": "string"},
                                    "source": {"type": "string"},
                                    "ref": {"type": "string"},
                                    "ref_type": {"type": "string"},
                                },
                                "additionalProperties": True,
                            },
                        ]
                    },
                },
                "local_context_tokens": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Optional token count for local context when the caller already counted it. MatrixArk reserves at least this many tokens before adding remote refs.",
                },
                "reference_time_ms": {
                    "type": "integer",
                    "description": "Optional retrieval clock for deterministic tests and replay.",
                },
                "ranking": {
                    "type": "object",
                    "description": "Optional multi-path recall config: time decay, business weights, and auxiliary quota.",
                    "properties": {
                        "weights": {
                            "type": "object",
                            "properties": {
                                "time": {"type": "number", "minimum": 0, "maximum": 1},
                                "business": {"type": "number", "minimum": 0, "maximum": 1},
                            },
                            "additionalProperties": True,
                        },
                        "freshness_tolerance_ms": {"type": "integer", "minimum": 0},
                        "half_life_ms": {"type": "integer", "minimum": 1},
                        "business_type_weights": {
                            "type": "object",
                            "additionalProperties": {"type": "number"},
                        },
                        "auxiliary_quota": {"type": "integer", "minimum": 0},
                    },
                    "additionalProperties": True,
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_batch_extract",
        "description": "Run schema-driven one-pass memory extraction over a logical session batch.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Session/message batch. Best quality is usually >= 20 messages or explicit force/session_commit.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "agent_hook": AGENT_HOOK_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "threshold_messages": {
                    "type": "integer",
                    "default": 20,
                    "description": "Default extraction threshold. Below this, extraction is deferred unless force=true.",
                },
                "force": {
                    "type": "boolean",
                    "default": False,
                    "description": "Force one-pass extraction even when the batch is below threshold.",
                },
                "segment_provider": {
                    "type": "string",
                    "enum": ["deterministic", "oss", "oss-fallback"],
                    "default": "deterministic",
                    "description": "Segment boundary detector. oss uses a local transformers model and emits the same ContextSegment JSON shape.",
                },
                "segment_model": {
                    "type": "string",
                    "default": "Qwen/Qwen2.5-0.5B-Instruct",
                    "description": "OSS instruct model name for segment_provider=oss.",
                },
                "segment_model_path": {
                    "type": "string",
                    "description": "Optional local model path for offline OSS segmentation.",
                },
                "segment_max_new_tokens": {
                    "type": "integer",
                    "default": 512,
                    "minimum": 64,
                    "description": "Maximum tokens generated by the OSS segment boundary model.",
                },
                "segment_provider_fallback": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, fall back to deterministic segmentation when the OSS model cannot load or returns invalid JSON.",
                },
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_feedback",
        "description": "Capture final answer feedback, confirmations, corrections, and accepted refs.",
        "inputSchema": {
            "type": "object",
            "required": ["messages"],
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": 1,
                    "items": MESSAGE_SCHEMA,
                    "description": "Required. Feedback text or final answer messages.",
                },
                "scope": SCOPE_SCHEMA,
                "metadata": METADATA_SCHEMA,
                "api_key": API_KEY_SCHEMA,
                "context_pack_id": {
                    "type": "string",
                    "description": "Optional but strongly recommended for confirmation/correction inference.",
                },
                "accepted_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent accepted from the prior ContextPack.",
                },
                "rejected_refs": {
                    "type": "array",
                    "description": "Optional refs the user/agent rejected from the prior ContextPack.",
                },
                "agent_hook": AGENT_HOOK_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_replay",
        "description": "Replay locally captured MatrixArk events for a context pack.",
        "inputSchema": {
            "type": "object",
            "required": ["context_pack_id"],
            "properties": {"context_pack_id": {"type": "string"}, "scope": SCOPE_SCHEMA, "api_key": API_KEY_SCHEMA},
        },
    },
    {
        "name": "matrixark_backend_ready",
        "description": "Run a low-cost backend topology/storage readiness probe before ingestion or parity tests.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "reason": {"type": "string", "default": "manual"},
                "probe": {"type": "boolean", "default": True},
                "timeout_ms": {"type": "integer", "default": BACKEND_READINESS_TIMEOUT_MS},
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_backend_metrics",
        "description": "Return backend health, readiness, and low-cost metrics for the active MatrixArk storage adapter.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_account",
        "description": "Create a MatrixArk account and default tenant.",
        "inputSchema": {
            "type": "object",
            "properties": ADMIN_ACCOUNT_PROPERTIES,
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_account",
        "description": "Update account or tenant metadata and active/disabled status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "account_status": {"type": "string", "enum": ["active", "disabled"]},
                "tenant_status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_accounts",
        "description": "List account and tenant metadata visible to the caller.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_user",
        "description": "Create or register a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"], "default": "active"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_update_user",
        "description": "Update, enable, or disable a MatrixArk user under an account/tenant.",
        "inputSchema": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "user_id": {"type": "string"},
                "display_name": {"type": "string"},
                "external_subject": {"type": "string"},
                "status": {"type": "string", "enum": ["active", "disabled"]},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_users",
        "description": "List MatrixArk users for an account/tenant without exposing context data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "status": {"type": "string", "enum": ["active", "disabled"]},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_create_api_key",
        "description": "Create a MatrixArk API key for an account/tenant. The raw key is returned once.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes such as context:ingest, context:retrieve, admin:api_key.",
                },
                "role": {"type": "string", "default": "service"},
                "display_name": {"type": "string"},
                "allowed_user_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional user allow-list. Empty means any user in the key tenant.",
                },
                "allowed_session_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional session allow-list. Empty means any session in the key tenant.",
                },
                "expires_at_ms": {
                    "type": "integer",
                    "description": "Optional future unix timestamp in milliseconds when this key expires.",
                },
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_apply_api_key",
        "description": "One-call local agent onboarding: create or reuse account, agent-derived tenant, local user, and return a scoped MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "scope": SCOPE_SCHEMA,
                "account_id": {"type": "string", "description": "Optional account/customer id. Defaults to acct_local in local mode."},
                "tenant_id": {"type": "string", "description": "Optional tenant/workspace id. Defaults to tenant_<agent_name> in local mode."},
                "agent_name": {"type": "string", "description": "Agent name used for the local tenant, e.g. codex, claude, cursor."},
                "user_id": {"type": "string", "description": "Optional MatrixArk user id. Defaults to the local OS account."},
                "account_name": {"type": "string"},
                "tenant_name": {"type": "string"},
                "display_name": {"type": "string", "description": "Display name for the local MatrixArk user."},
                "external_subject": {"type": "string", "description": "Optional external subject such as local:<user>, okta:<id>, google:<id>."},
                "key_display_name": {"type": "string"},
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowed scopes for the new key. Defaults to context ingest/retrieve/feedback/replay plus resource and skill read.",
                },
                "role": {"type": "string", "default": "local_agent"},
                "allowed_user_ids": {"type": "array", "items": {"type": "string"}},
                "allowed_session_ids": {"type": "array", "items": {"type": "string"}},
                "allow_all_users": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, do not restrict the key to the derived local user.",
                },
                "expires_at_ms": {"type": "integer"},
                "key_prefix": {"type": "string", "default": "mk_local"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_list_api_keys",
        "description": "List MatrixArk API key metadata for an account/tenant. Raw keys and hashes are never returned.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "include_revoked": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_rotate_api_key",
        "description": "Revoke an active MatrixArk API key and create a replacement with the same scopes.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "api_key_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "key_prefix": {"type": "string", "default": "mk_test"},
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_revoke_api_key",
        "description": "Revoke a MatrixArk API key.",
        "inputSchema": {
            "type": "object",
            "required": ["api_key_id"],
            "properties": {"api_key": API_KEY_SCHEMA, "api_key_id": {"type": "string"}, "scope": SCOPE_SCHEMA},
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_map_sso_user",
        "description": "Map an external Okta/Google/Azure AD user id to a MatrixArk user id.",
        "inputSchema": {
            "type": "object",
            "required": ["provider", "external_user_id"],
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "provider": {"type": "string", "description": "okta, google, azure_ad, or another IdP name."},
                "external_user_id": {"type": "string"},
                "matrixark_user_id": {"type": "string"},
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
            },
            "additionalProperties": True,
        },
    },
    {
        "name": "matrixark_admin_audit",
        "description": "List MatrixArk access-management audit records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "api_key": API_KEY_SCHEMA,
                "account_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "scope": SCOPE_SCHEMA,
                "limit": {"type": "integer", "default": 100},
            },
            "additionalProperties": True,
        },
    },
]


class MatrixArkMcpServer:
    def __init__(self, adapter: MatrixArkLocalAdapter, *, line_json: bool = False, access_mode: str = "dev") -> None:
        self.adapter = adapter
        self.line_json = line_json
        self.access = MatrixArkAccessManager(adapter, mode=access_mode)

    def handle(self, request: Json) -> Json | None:
        method = request.get("method")
        request_id = request.get("id")
        try:
            if method == "initialize":
                requested_protocol = (request.get("params") or {}).get("protocolVersion") or "2025-06-18"
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": requested_protocol,
                        "serverInfo": {"name": "matrixark-context", "version": "0.1.0"},
                        "capabilities": {"tools": {"listChanged": False}},
                    },
                }
            if method == "notifications/initialized":
                return None
            if method == "tools/list":
                return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
            if method == "tools/call":
                params = request.get("params", {})
                name = params.get("name")
                args = params.get("arguments", {})
                if not isinstance(args, dict):
                    raise MatrixArkError("tool arguments must be an object")
                result = self.call_tool(name, args)
                return {"jsonrpc": "2.0", "id": request_id, "result": json_text(result)}
            raise MatrixArkError(f"unsupported method {method!r}")
        except Exception as exc:  # MCP errors should stay JSON-RPC shaped.
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    def call_tool(self, name: str, args: Json) -> Json:
        hook = args.pop("agent_hook", None)
        identity = self.access.authorize_and_enrich(name, args)
        if name == "matrixark_backend_ready":
            result = self.adapter.ensure_backend_ready(
                reason=str(args.get("reason") or "manual"),
                probe=bool(args.get("probe", True)),
                timeout_ms=args.get("timeout_ms"),
            )
            status = "ok" if result.get("status") == "ready" else "topology_not_ready"
            self.access.append_audit(
                "backend.ready",
                identity,
                status=status,
                details={"backend": result.get("backend"), "attempts": result.get("attempts")},
            )
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_backend_metrics":
            result = self.adapter.backend_metrics()
            self.access.append_audit(
                "backend.metrics",
                identity,
                status="ok",
                details={"backend": result.get("backend"), "metrics_format": result.get("metrics_format")},
            )
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_ingest":
            result = self.adapter.ingest(args, hook=hook)
            self.access.append_audit("context.ingest", identity, status="ok", details={"event_id_hash": result.get("event_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_batch_extract":
            result = self.adapter.batch_extract(args, hook=hook)
            self.access.append_audit("context.batch_extract", identity, status="ok", details={"batch_id_hash": result.get("batch_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_session_commit":
            result = self.adapter.session_commit(args, hook=hook)
            self.access.append_audit("context.session_commit", identity, status="ok", details={"commit_id_hash": result.get("commit_id_hash"), "batch_id_hash": result.get("batch_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_refresh_summaries":
            result = self.adapter.refresh_summaries(args)
            self.access.append_audit("context.refresh_summaries", identity, status="ok", details={"refreshed_count": result.get("refreshed_count")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_retrieve":
            result = self.adapter.retrieve(args)
            self.access.append_audit("context.retrieve", identity, status="ok", details={"context_pack_id": result.get("context_pack_id")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_list_resources":
            result = self.adapter.list_resources(args)
            self.access.append_audit("resource.list", identity, status="ok", details={"count": result.get("count")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_list_skills":
            result = self.adapter.list_skills(args)
            self.access.append_audit("skill.list", identity, status="ok", details={"count": result.get("count")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_update_skill":
            result = self.adapter.update_skill(args)
            self.access.append_audit("skill.update", identity, status="ok", details={"skill_hash": result.get("skill_hash"), "skill_status": result.get("status")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_feedback":
            result = self.adapter.feedback(args, hook=hook)
            self.access.append_audit("context.feedback", identity, status="ok", details={"event_id_hash": result.get("event_id_hash")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_replay":
            result = self.adapter.replay(args)
            self.access.append_audit("context.replay", identity, status="ok", details={"context_pack_id": args.get("context_pack_id")})
            return {**result, "access": args.get("_matrixark_auth", {})}
        if name == "matrixark_admin_create_account":
            return self.access.create_account(args, identity)
        if name == "matrixark_admin_update_account":
            return self.access.update_account(args, identity)
        if name == "matrixark_admin_list_accounts":
            return self.access.list_accounts(args, identity)
        if name == "matrixark_admin_create_user":
            return self.access.create_user(args, identity)
        if name == "matrixark_admin_update_user":
            return self.access.update_user(args, identity)
        if name == "matrixark_admin_list_users":
            return self.access.list_users(args, identity)
        if name == "matrixark_admin_create_api_key":
            return self.access.create_api_key(args, identity)
        if name == "matrixark_admin_apply_api_key":
            return self.access.apply_api_key(args, identity)
        if name == "matrixark_admin_list_api_keys":
            return self.access.list_api_keys(args, identity)
        if name == "matrixark_admin_rotate_api_key":
            return self.access.rotate_api_key(args, identity)
        if name == "matrixark_admin_revoke_api_key":
            return self.access.revoke_api_key(args, identity)
        if name == "matrixark_admin_map_sso_user":
            return self.access.map_sso_user(args, identity)
        if name == "matrixark_admin_audit":
            return self.access.audit(args, identity)
        raise MatrixArkError(f"unsupported tool {name!r}")

    def read_message(self) -> Json | None:
        if self.line_json:
            line = sys.stdin.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                return {}
            if not line.lstrip().startswith("{"):
                return {}
            return json.loads(line)

        _mcp_debug_log("read_message: waiting for first header")
        first = sys.stdin.buffer.readline()
        _mcp_debug_log(f"read_message: first={first[:80]!r}")
        if not first:
            return None
        if not first.strip():
            return {}
        if first.lstrip().startswith(b"{"):
            # Codex CLI currently speaks newline-delimited JSON over stdio for
            # configured MCP servers. Auto-detect it so responses use the same
            # framing and do not trigger parse-error ping-pong.
            self.line_json = True
            return json.loads(first.decode("utf-8"))

        headers = [first]
        while True:
            header = sys.stdin.buffer.readline()
            if header in {b"\r\n", b"\n", b""}:
                break
            headers.append(header)

        length = None
        for header in headers:
            if header.lower().startswith(b"content-length:"):
                length = int(header.split(b":", 1)[1].strip())
                break
        if length is None:
            raise MatrixArkError("invalid MCP frame: missing Content-Length header")
        body = sys.stdin.buffer.read(length)
        _mcp_debug_log(f"read_message: body_len={len(body)}")
        return json.loads(body.decode("utf-8"))

    def write_response(self, response: Json) -> None:
        payload = json.dumps(response, sort_keys=True)
        if self.line_json:
            sys.stdout.write(payload + "\n")
            sys.stdout.flush()
            return
        body = payload.encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()
        _mcp_debug_log(f"write_response: bytes={len(body)} id={response.get('id')!r} keys={list(response.keys())}")

    def serve(self) -> None:
        while True:
            request = self.read_message()
            if request is None:
                return
            if not request:
                continue
            response = self.handle(request)
            if response is not None:
                self.write_response(response)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        choices=["local", "temporalstore-local", "temporalstore-direct", "temporalstore-rust"],
        default=os.environ.get("MATRIXARK_MCP_BACKEND", "local"),
        help="Storage backend. local uses JSONL; temporalstore-local uses a no-metaserver local TemporalStore-shaped record log; temporalstore-direct uses the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--event-log",
        type=Path,
        default=Path("/tmp/matrixark-mcp-events.jsonl"),
        help="JSONL event log used by the local adapter.",
    )
    parser.add_argument(
        "--local-store",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", "/tmp/matrixark-mcp-temporalstore-local.jsonl")),
        help="Persistent local record log for --backend temporalstore-local. This mode does not require metaserver.",
    )
    parser.add_argument(
        "--line-json",
        action="store_true",
        help="Use newline-delimited JSON for simple shell debugging instead of MCP framing.",
    )
    parser.add_argument(
        "--access-mode",
        choices=["dev", "enforced"],
        default=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"),
        help="dev allows omitted API keys for local testing; enforced requires scoped MatrixArk API keys.",
    )
    parser.add_argument(
        "--metaserver",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
        help="C++ TemporalStore metaserver address for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"),
        help="TemporalStore namespace for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"),
        help="TemporalStore table for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--temporalstore-lib",
        default=os.environ.get("TEMPORALSTORE_LIB", ""),
        help="Path to libbcache2.so for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--storage-prefix",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:mcp"),
        help="TemporalStore key prefix for MatrixArk records.",
    )
    parser.add_argument(
        "--rust-cli",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", ""),
        help="Path to the Rust matrixark_gateway or matrixark_record_log binary for --backend temporalstore-rust.",
    )
    parser.add_argument(
        "--request-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "20000")),
        help="Per-request timeout for the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--io-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "20000")),
        help="BRPC I/O timeout for the native C++ TemporalStore SDK.",
    )
    args = parser.parse_args()
    _mcp_debug_log(f"main: parsed backend={args.backend} metaserver={args.metaserver}")
    if args.backend == "temporalstore-direct":
        adapter = MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-rust":
        adapter = MatrixArkTemporalStoreRustAdapter(
            rust_cli=args.rust_cli,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-local":
        adapter = MatrixArkLocalAdapter(args.local_store)
    else:
        adapter = MatrixArkLocalAdapter(args.event_log)
    _mcp_debug_log("main: adapter ready; serving")
    MatrixArkMcpServer(adapter, line_json=args.line_json, access_mode=args.access_mode).serve()
    _mcp_debug_log("main: serve returned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
