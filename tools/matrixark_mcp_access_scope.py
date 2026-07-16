#!/usr/bin/env python3
"""Access scope and session-continuity helpers for MatrixArk recall."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_identity import (
        parse_scope_key,
        scope_from_serving_record,
        scope_key_matches_query,
        session_scope_mode,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import (
        parse_scope_key,
        scope_from_serving_record,
        scope_key_matches_query,
        session_scope_mode,
    )


Json = dict[str, Any]


def sharing_scope_from_candidate(candidate: Json) -> str:
    for source in [candidate, candidate.get("access_scope", {}), candidate.get("metadata", {}), candidate.get("scope", {})]:
        if isinstance(source, dict):
            value = str(source.get("sharing_scope") or "").strip().lower()
            if value:
                return value
    node_path = [str(part).lower() for part in candidate.get("node_path", []) if str(part)]
    if node_path[:2] == ["global", "shared"]:
        return "global_shared"
    if len(node_path) >= 2 and node_path[0].startswith("tenant:") and node_path[1] == "shared":
        return "tenant_shared"
    return "private_user"


def scope_matches(record_scope: Json, query_scope: Json) -> bool:
    if not query_scope:
        return True
    sharing_scope = str(record_scope.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
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
        if key in {"session_id", "session_hash"}:
            if "session_id" not in explicit_keys or session_scope_mode(query_scope) == "prefer":
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
    record_scope = candidate_access_scope(record)
    sharing_scope = str(record_scope.get("sharing_scope") or record.get("sharing_scope") or "").strip().lower()
    if sharing_scope == "global_shared":
        return True
    if sharing_scope == "tenant_shared":
        for field in ["account_id", "account_hash", "tenant_id", "tenant_hash"]:
            query_value = query_scope.get(field)
            record_value = record_scope.get(field)
            if query_value and record_value and query_value != record_value:
                return False
        return True
    return scope_matches(record_scope, query_scope)


def session_continuity_status(record_scope: Json, query_scope: Json) -> str:
    query_session = str(query_scope.get("session_id") or "")
    if not query_session:
        return "unscoped"
    record_session = str(record_scope.get("session_id") or "")
    if record_session == query_session:
        return "same_session"
    record_key = str(record_scope.get("scope_key") or "")
    query_session_hash = int(query_scope.get("session_hash") or 0)
    if record_key and query_session_hash and parse_scope_key(record_key).get("s") == query_session_hash:
        return "same_session"
    if record_session or record_key:
        return "cross_session"
    return "unscoped"


def session_continuity_boost(candidate: Json, question_type: str) -> float:
    status = str(candidate.get("session_continuity") or "")
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    if status == "same_session":
        if ref_type in {"event", "segment"}:
            return 0.16
        if ref_type == "summary":
            return 0.12
        if ref_type == "entity":
            return 0.10
        return 0.08
    if status == "cross_session":
        if ref_type == "entity" or context_class in {"resource_entity_fact", "resource_fact"}:
            return 0.11
        if question_type in {"multi_hop", "current_state"} and ref_type in {"event", "segment", "compression"}:
            return 0.06
    return 0.0


def cross_session_rerank_adjustment(candidate: Json, question_type: str) -> float:
    if str(candidate.get("session_continuity") or "") != "cross_session":
        return 0.0
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or ref_type)
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    if ref_type == "entity":
        return 0.10 if question_type in {"current_state", "latest", "multi_hop"} else 0.06
    if context_class in {"resource_fact", "resource_entity_fact"}:
        return 0.06 if has_citation else 0.04
    if ref_type == "resource_chunk" and has_citation:
        return 0.04
    if ref_type in {"event", "segment"} and question_type in {"multi_hop", "why_emotion", "fact", "evidence"}:
        return 0.01
    if ref_type == "compression":
        return 0.05
    if ref_type == "summary":
        return 0.05 if question_type == "broad_exploration" else 0.02
    return 0.0
