#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Candidate payload builders for MatrixArk local retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, clip_context_text, now_ms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, clip_context_text, now_ms


def event_candidate(
    record: Json,
    *,
    envelope: Json,
    record_scope: Json,
    index_terms: set[str],
    event_type: str,
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    metadata: Json,
    text: str,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    return {
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
        "classification": record.get("classification", ""),
        "extraction_status": record.get("extraction_status", ""),
        "extraction_mode": record.get("extraction_mode", ""),
        "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
        "source_chunk_hash": record.get("source_chunk_hash"),
        "source_ref": record.get("source_ref", ""),
        "source_roles": record.get("source_roles", []),
        "source_role_counts": record.get("source_role_counts", {}),
        "source_hook_types": record.get("source_hook_types", []),
        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
        "source_codex_events": record.get("source_codex_events", []),
        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
        "profile_memory_class": record.get("profile_memory_class", ""),
        "profile_memory_kind": record.get("profile_memory_kind", ""),
        "source_profile_memory_classes": record.get("source_profile_memory_classes", []),
        "source_profile_memory_kinds": record.get("source_profile_memory_kinds", []),
        "source_memory_scopes": record.get("source_memory_scopes", []),
        "source_session_continuities": record.get("source_session_continuities", []),
        "source_extraction_phases": record.get("source_extraction_phases", []),
        "memory_scope": record.get("memory_scope", ""),
        "session_continuity": record.get("session_continuity", ""),
        "extraction_phase": record.get("extraction_phase", ""),
        "final_session_boundary": bool(record.get("final_session_boundary", False)),
        "metadata": metadata,
        "scope": record_scope,
        "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
        "text": clip_context_text(text),
    }


def entity_candidate(
    record: Json,
    *,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    source_entity_hashes = record.get("source_entity_hashes", [])
    source_session_ids = record.get("source_session_ids", [])
    is_profile_entity_bridge = (
        str(record.get("memory_scope") or "") == "user_profile"
        and str(record.get("session_continuity") or "") == "cross_session"
    )
    return {
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
            "selected as cross-session user-profile entity bridge"
            if str(record.get("memory_scope") or "") == "user_profile"
            and str(record.get("session_continuity") or "") == "cross_session"
            else
            "selected by tree path, secondary indexes, and resource entity state score"
            if record.get("source_chunk_hash")
            else "selected by tree path, secondary indexes, and entity state score"
        ),
        "entity_type": record.get("entity_type", ""),
        "entity_name": record.get("entity_name", ""),
        "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
        "source_chunk_hash": record.get("source_chunk_hash"),
        "source_ref": record.get("source_ref", ""),
        "source_roles": record.get("source_roles", []),
        "source_role_counts": record.get("source_role_counts", {}),
        "source_hook_types": record.get("source_hook_types", []),
        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
        "source_codex_events": record.get("source_codex_events", []),
        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
        "source_memory_scopes": record.get("source_memory_scopes", []),
        "source_session_continuities": record.get("source_session_continuities", []),
        "source_extraction_phases": record.get("source_extraction_phases", []),
        "source_session_ids": source_session_ids,
        "source_entity_hashes": source_entity_hashes,
        "profile_current_state_representative": is_profile_entity_bridge,
        "current_state_source_session_count": len(source_session_ids) if isinstance(source_session_ids, list) else 0,
        "current_state_source_entity_count": len(source_entity_hashes) if isinstance(source_entity_hashes, list) else 0,
        "current_state_policy": (
            "profile_entity_bridge_preferred_over_session_local_history"
            if is_profile_entity_bridge
            else ""
        ),
        "memory_scope": record.get("memory_scope", ""),
        "session_continuity": record.get("session_continuity", ""),
        "extraction_phase": record.get("extraction_phase", ""),
        "final_session_boundary": bool(record.get("final_session_boundary", False)),
        "metadata": record.get("metadata", {}),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(text),
    }


def segment_candidate(
    record: Json,
    *,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    saliency_score: float,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    return {
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
        "source_roles": record.get("source_roles", []),
        "source_role_counts": record.get("source_role_counts", {}),
        "source_hook_types": record.get("source_hook_types", []),
        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
        "source_codex_events": record.get("source_codex_events", []),
        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
        "source_memory_scopes": record.get("source_memory_scopes", []),
        "source_session_continuities": record.get("source_session_continuities", []),
        "source_extraction_phases": record.get("source_extraction_phases", []),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(str(record.get("summary_text", ""))),
    }


def compression_candidate(
    record: Json,
    *,
    compression_hash: int,
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    return {
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
        "source_roles": record.get("source_roles", []),
        "source_role_counts": record.get("source_role_counts", {}),
        "source_hook_types": record.get("source_hook_types", []),
        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
        "source_codex_events": record.get("source_codex_events", []),
        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
        "source_memory_scopes": record.get("source_memory_scopes", []),
        "source_session_continuities": record.get("source_session_continuities", []),
        "source_extraction_phases": record.get("source_extraction_phases", []),
        "source_final_session_boundary_count": record.get("source_final_session_boundary_count", 0),
        "memory_scope": record.get("memory_scope", ""),
        "session_continuity": record.get("session_continuity", ""),
        "extraction_phase": record.get("extraction_phase", ""),
        "final_session_boundary": bool(record.get("final_session_boundary", False)),
        "source_start_ms": record.get("source_start_ms"),
        "source_end_ms": record.get("source_end_ms"),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
        "text": clip_context_text(text),
    }


def summary_candidate(
    record: Json,
    *,
    summary_type: str,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    return {
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
        "source_roles": record.get("source_roles", []),
        "source_role_counts": record.get("source_role_counts", {}),
        "source_hook_types": record.get("source_hook_types", []),
        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
        "source_codex_events": record.get("source_codex_events", []),
        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
        "source_profile_promotion_policies": record.get("source_profile_promotion_policies", []),
        "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers", []),
        "source_profile_memory_classes": record.get("source_profile_memory_classes", []),
        "source_profile_memory_kinds": record.get("source_profile_memory_kinds", []),
        "source_memory_scopes": record.get("source_memory_scopes", []),
        "source_session_continuities": record.get("source_session_continuities", []),
        "source_extraction_phases": record.get("source_extraction_phases", []),
        "source_entity_types": record.get("source_entity_types", []),
        "source_final_session_boundary_count": record.get("source_final_session_boundary_count", 0),
        "memory_scope": record.get("memory_scope", ""),
        "session_continuity": record.get("session_continuity", ""),
        "extraction_phase": record.get("extraction_phase", ""),
        "final_session_boundary": bool(record.get("final_session_boundary", False)),
        "profile_summary_current": bool(record.get("profile_summary_current", False)),
        "profile_memory_class": record.get("profile_memory_class", ""),
        "profile_memory_kind": record.get("profile_memory_kind", ""),
        "profile_promotion_policy": record.get("profile_promotion_policy", ""),
        "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
        "access_decision": "allowed_by_registry_scope_before_scoring",
        "access_scope": candidate_access_scope(record),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(text),
        "recall_path": "primary_summary",
    }


def resource_skill_candidate(
    record: Json,
    *,
    ref_type: str,
    ref_hash: int,
    resource_hash: int,
    source_locator: str,
    resource_version: str,
    supersedes_chunk_hash: object,
    version_state: str,
    stale_or_superseded: bool,
    citation: str,
    metadata: Json,
    business_type: str,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    meta = record.get("embedding_meta")
    if isinstance(meta, dict) and meta:
        record = {**meta, **record}
    return {
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
        "resource_hash": resource_hash,
        "source_locator": source_locator,
        "resource_type": record.get("resource_type", ""),
        "resource_version": resource_version,
        "supersedes_chunk_hash": supersedes_chunk_hash,
        "version_state": version_state,
        "stale_or_superseded": stale_or_superseded,
        "access_decision": "allowed_by_registry_scope_before_scoring",
        "access_scope": candidate_access_scope(record),
        "deployment_scope": record.get("deployment_scope", "local"),
        "citation": citation,
        "metadata": metadata,
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(text),
        "recall_path": "primary_resource_skill",
    }
