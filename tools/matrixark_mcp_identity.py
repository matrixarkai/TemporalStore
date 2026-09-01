#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk MCP identity, scope, and API-key helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import time
from typing import Any


Json = dict[str, Any]


def safe_identifier(value: str, *, default: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()).strip("._-").lower()
    return compact or default


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
    # A read about the store's own encoding state, gated like any other read of it.
    "matrixark_embedding_status": {"context:retrieve"},
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


def identity_hashes(account_id: str, tenant_id: str, user_id: str = "", session_id: str = "", agent_id: str = "") -> Json:
    tenant_hash = stable_hash(f"{account_id}:{tenant_id}")
    user_hash = stable_hash(f"{tenant_hash}:user:{user_id}") if user_id else 0
    session_hash = stable_hash(f"{tenant_hash}:session:{session_id}") if session_id else 0
    # agent_id (mem0 identity dimension): only participates when supplied, so the
    # returned dict and scope_key are byte-identical for callers without an agent.
    agent_hash = stable_hash(f"{tenant_hash}:agent:{agent_id}") if agent_id else 0
    hashes: Json = {
        "tenant_hash": tenant_hash,
        "user_hash": user_hash,
        "session_hash": session_hash,
        "scope_key": scope_key_from_hashes(tenant_hash, user_hash, session_hash, agent_hash),
    }
    if agent_hash:
        hashes["agent_hash"] = agent_hash
    return hashes


def scope_key_from_hashes(tenant_hash: int, user_hash: int = 0, session_hash: int = 0, agent_hash: int = 0) -> str:
    parts = [f"t={int(tenant_hash)}"]
    if user_hash:
        parts.append(f"u={int(user_hash)}")
    if session_hash:
        parts.append(f"s={int(session_hash)}")
    if agent_hash:
        parts.append(f"a={int(agent_hash)}")
    return "|".join(parts) + "|"


def scope_key_prefix_for_query(query_scope: Json) -> str:
    explicit_keys = set(query_scope.get("_explicit_scope_keys", []))
    tenant_hash = int(query_scope.get("tenant_hash") or 0)
    if not tenant_hash:
        return ""
    user_hash = int(query_scope.get("user_hash") or 0) if "user_id" in explicit_keys or query_scope.get("user_hash") else 0
    session_hash = int(query_scope.get("session_hash") or 0) if "session_id" in explicit_keys or query_scope.get("session_hash") else 0
    agent_hash = int(query_scope.get("agent_hash") or 0) if "agent_id" in explicit_keys or query_scope.get("agent_hash") else 0
    return scope_key_from_hashes(tenant_hash, user_hash, session_hash, agent_hash)


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


def session_scope_mode(query_scope: Json) -> str:
    mode = str(
        query_scope.get("_session_scope")
        or query_scope.get("session_scope")
        or query_scope.get("_session_filter_mode")
        or "only"
    ).strip().lower()
    if mode in {"prefer", "preferred", "soft", "continuity"}:
        return "prefer"
    return "only"


def scope_key_matches_query(record_scope_key: str, query_scope: Json, explicit_keys: set[str]) -> bool:
    record_parts = parse_scope_key(record_scope_key)
    tenant_hash = int(query_scope.get("tenant_hash") or 0)
    if tenant_hash and record_parts.get("t") != tenant_hash:
        return False
    user_hash = int(query_scope.get("user_hash") or 0)
    if "user_id" in explicit_keys or "user_hash" in explicit_keys or user_hash:
        if user_hash and record_parts.get("u") != user_hash:
            return False
    # agent_id isolation (mem0 dimension): enforced only when the QUERY carries an
    # agent, mirroring user/session. No agent in the query -> no agent filtering.
    agent_hash = int(query_scope.get("agent_hash") or 0)
    if "agent_id" in explicit_keys or "agent_hash" in explicit_keys or agent_hash:
        if agent_hash and record_parts.get("a") != agent_hash:
            return False
    session_hash = int(query_scope.get("session_hash") or 0)
    if "session_id" in explicit_keys or "session_hash" in explicit_keys or session_hash:
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
        int(scope.get("agent_hash") or 0),
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

