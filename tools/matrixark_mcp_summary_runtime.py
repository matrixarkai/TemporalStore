#!/usr/bin/env python3
"""Context summary runtime helpers for MatrixArk MCP adapters."""

from __future__ import annotations

from typing import Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        candidate_access_scope,
        embedding_for_text,
        embedding_model_name,
        integer_arg,
        now_ms,
        node_l1_generation_policy,
        optional_object,
        scope_matches,
        stable_hash,
        summarize_text,
        synthesize_context_node_summary,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        candidate_access_scope,
        embedding_for_text,
        embedding_model_name,
        integer_arg,
        now_ms,
        node_l1_generation_policy,
        optional_object,
        scope_matches,
        stable_hash,
        summarize_text,
        synthesize_context_node_summary,
    )

try:
    from tools.matrixark_mcp_tree import node_path_tuple, starts_with_path
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_tree import node_path_tuple, starts_with_path


try:
    from tools.matrixark_mcp_summary_dirty import (
        ContextEventTime,
        mark_node_summary_dirty,
        node_summary_dirty_records,
        pending_dirty_node_records,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summary_dirty import (
        ContextEventTime,
        mark_node_summary_dirty,
        node_summary_dirty_records,
        pending_dirty_node_records,
    )


def node_summary_source_records(
    *,
    records: list[Json],
    node_path: list[str],
    scope: Json,
    node_hash: int | None = None,
    max_events: int = 8,
    max_child_summaries: int = 8,
    max_entity_states: int = 6,
    max_operator_states: int = 4,
) -> tuple[list[Json], list[Json], list[Json], list[Json], Json]:
    prefix = node_path_tuple(node_path)
    target_node_hash = int(node_hash) if node_hash is not None else stable_hash("/".join(node_path))
    direct_child_hashes: set[int] = set()
    for record in records:
        if record.get("record_type") != "context_child_ref":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            parent_hash = int(record.get("parent_hash") or 0)
            child_hash = int(record.get("child_hash") or 0)
        except (TypeError, ValueError):
            continue
        if parent_hash == target_node_hash and child_hash:
            direct_child_hashes.add(child_hash)

    child_summaries: list[Json] = []
    entity_states: list[Json] = []
    operator_states: list[Json] = []
    same_node_events: list[Json] = []
    seen_summary_keys: set[tuple[int, str]] = set()
    seen_entity_hashes: set[int] = set()
    seen_operator_hashes: set[int] = set()
    summary_types = {"node_l0", "node_l1", "batch_l0", "session_l0", "resource_l0", "skill_l0"}
    operator_record_types = {"context_compression_event"}
    for record in reversed(records):
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        record_type = str(record.get("record_type") or "")
        try:
            record_node_hash = int(record.get("node_hash") or 0)
        except (TypeError, ValueError):
            record_node_hash = 0
        record_path = node_path_tuple(record.get("node_path", []))
        is_same_node = record_node_hash == target_node_hash or (bool(record_path) and record_path == prefix)
        is_direct_child = record_node_hash in direct_child_hashes or (
            bool(record_path) and starts_with_path(record_path, prefix) and len(record_path) == len(prefix) + 1
        )
        if record_type == "context_summary" and record.get("summary_type") in summary_types:
            if len(child_summaries) >= max_child_summaries or not is_direct_child:
                continue
            key = (record_node_hash, str(record.get("summary_type", "")))
            if key in seen_summary_keys:
                continue
            seen_summary_keys.add(key)
            child_summaries.append(record)
            continue
        if record_type == "context_entity" and (is_same_node or is_direct_child):
            if len(entity_states) >= max_entity_states:
                continue
            try:
                entity_hash = int(record.get("entity_hash") or 0)
            except (TypeError, ValueError):
                entity_hash = 0
            if entity_hash and entity_hash in seen_entity_hashes:
                continue
            if entity_hash:
                seen_entity_hashes.add(entity_hash)
            entity_states.append(record)
            continue
        if record_type in operator_record_types and (is_same_node or is_direct_child):
            if len(operator_states) >= max_operator_states:
                continue
            try:
                operator_hash = int(record.get("compression_id_hash") or record.get("ref_hash") or 0)
            except (TypeError, ValueError):
                operator_hash = 0
            if operator_hash and operator_hash in seen_operator_hashes:
                continue
            if operator_hash:
                seen_operator_hashes.add(operator_hash)
            operator_states.append(record)
            continue
        if record_type == "context_event" and is_same_node and len(same_node_events) < max_events:
            same_node_events.append(record)

    use_direct_events = not child_summaries
    events = same_node_events if use_direct_events else []
    policy = {
        "source_policy": "child_summaries_plus_state" if child_summaries else "direct_events_fallback",
        "raw_recursive_leaf_event_scan": False,
        "direct_child_count": len(direct_child_hashes),
        "used_direct_event_count": len(events),
        "used_child_summary_count": len(child_summaries),
        "used_entity_state_count": len(entity_states),
        "used_operator_state_count": len(operator_states),
    }
    return (
        list(reversed(events[:max_events])),
        list(reversed(child_summaries[:max_child_summaries])),
        list(reversed(entity_states[:max_entity_states])),
        list(reversed(operator_states[:max_operator_states])),
        policy,
    )


def build_node_summary_refresh_records(
    *,
    node_path: list[str],
    node_hash: int,
    scope: Json,
    events: list[Json],
    child_summaries: list[Json],
    entity_states: list[Json],
    operator_states: list[Json],
    summary_source_policy: Json,
    dirty_hash: int | None,
    refreshed_at_ms: int,
) -> Json:
    event_texts = [str(record.get("text", "")) for record in events if record.get("text")]
    child_summary_texts = [
        str(record.get("summary_text", ""))
        for record in child_summaries
        if record.get("summary_text")
    ]
    entity_state_texts = [
        summarize_text(
            f"{record.get('entity_type', 'entity')} {record.get('entity_name', '')}: {record.get('state', '')}",
            limit=240,
        )
        for record in entity_states
        if record.get("state")
    ]
    operator_state_texts = [
        summarize_text(
            f"{record.get('operator', 'operator')}: {record.get('summary_text') or record.get('text') or ''}",
            limit=260,
        )
        for record in operator_states
        if record.get("summary_text") or record.get("text")
    ]
    source_text = " ".join(child_summary_texts + entity_state_texts + operator_state_texts + event_texts)
    if not source_text:
        source_text = " ".join(node_path)
    prefix_label = " / ".join(node_path)
    source_event_ids = [int(record["event_id_hash"]) for record in events if record.get("event_id_hash") is not None]
    source_summary_hashes = [
        int(record.get("summary_hash") or record.get("node_hash"))
        for record in child_summaries
        if record.get("summary_hash") is not None or record.get("node_hash") is not None
    ]
    source_entity_hashes = [
        int(record.get("entity_hash"))
        for record in entity_states
        if record.get("entity_hash") is not None
    ]
    source_entity_types = sorted(
        {
            str(record.get("entity_type"))
            for record in entity_states
            if str(record.get("entity_type") or "").strip()
        }
    )
    source_records = events + entity_states + child_summaries

    def source_values(list_field: str, fallback_field: str = "") -> list[str]:
        values: set[str] = set()
        for record in source_records:
            raw_values = record.get(list_field) if isinstance(record.get(list_field), list) else []
            for value in raw_values:
                text = str(value or "").strip()
                if text:
                    values.add(text)
            fallback = str(record.get(fallback_field) or "").strip() if fallback_field else ""
            if fallback:
                values.add(fallback)
        return sorted(values)

    def source_count_map(count_field: str, list_field: str, fallback_field: str = "") -> Json:
        counts: Json = {}
        for record in source_records:
            raw_counts = record.get(count_field) if isinstance(record.get(count_field), dict) else {}
            if raw_counts:
                for key, raw_count in raw_counts.items():
                    text = str(key or "").strip()
                    if not text:
                        continue
                    try:
                        count = int(raw_count or 0)
                    except (TypeError, ValueError):
                        count = 0
                    if count > 0:
                        counts[text] = int(counts.get(text, 0)) + count
                continue
            raw_values = record.get(list_field) if isinstance(record.get(list_field), list) else []
            fallback_values = [record.get(fallback_field)] if fallback_field else []
            for value in list(raw_values) + fallback_values:
                text = str(value or "").strip()
                if text:
                    counts[text] = int(counts.get(text, 0)) + 1
        return dict(sorted(counts.items()))

    source_roles = source_values("source_roles", "source_role")
    source_role_counts = source_count_map("source_role_counts", "source_roles", "source_role")
    source_hook_types = source_values("source_hook_types", "hook_type")
    source_hook_type_counts = source_count_map("source_hook_type_counts", "source_hook_types", "hook_type")
    source_codex_events = source_values("source_codex_events", "codex_event")
    source_codex_event_counts = source_count_map("source_codex_event_counts", "source_codex_events", "codex_event")
    source_memory_scopes = sorted(
        {
            str(record.get("memory_scope") or "").strip()
            for record in entity_states + child_summaries
            if str(record.get("memory_scope") or "").strip()
        }
    )
    source_session_continuities = sorted(
        {
            str(record.get("session_continuity") or "").strip()
            for record in entity_states + child_summaries
            if str(record.get("session_continuity") or "").strip()
        }
    )
    source_extraction_phases = sorted(
        {
            str(record.get("extraction_phase") or "").strip()
            for record in entity_states + child_summaries + events
            if str(record.get("extraction_phase") or "").strip()
        }
    )
    source_final_session_boundary_count = sum(
        1
        for record in entity_states + child_summaries + events
        if bool(record.get("final_session_boundary"))
    )
    source_operator_hashes = [
        int(record.get("compression_id_hash") or record.get("ref_hash"))
        for record in operator_states
        if record.get("compression_id_hash") is not None or record.get("ref_hash") is not None
    ]
    l1_policy = node_l1_generation_policy(
        source_text=source_text,
        event_count=len(source_event_ids),
        child_summary_count=len(source_summary_hashes),
    )
    l1_policy = {**l1_policy, **summary_source_policy}
    l0_summary, l0_provider_meta = synthesize_context_node_summary(
        level="node_l0",
        node_path=node_path,
        source_text=source_text,
        fallback_text=f"{prefix_label} :: {source_text}",
        max_chars=220,
        policy=l1_policy,
    )
    summary_specs = [("node_l0", l0_summary, "node_l0", l0_provider_meta)]
    if l1_policy["generate_l1"]:
        l1_summary, l1_provider_meta = synthesize_context_node_summary(
            level="node_l1",
            node_path=node_path,
            source_text=source_text,
            fallback_text=(
                f"Context node {prefix_label}. Rich overview: {source_text}. "
                f"This node belongs to path {prefix_label} and should be used for tree-first retrieval before leaf event/entity recall."
            ),
            max_chars=1200,
            policy=l1_policy,
        )
        summary_specs.append(("node_l1", l1_summary, "node_l1", l1_provider_meta))

    records: list[Json] = []
    for level, summary_text, embedding_type, provider_meta in summary_specs:
        summary_hash = stable_hash(f"context_summary:{level}:{node_hash}")
        summary_policy = {**l1_policy, **provider_meta}
        summary_vector = embedding_for_text(summary_text)
        records.append(
            {
                "record_type": "context_summary",
                "summary_type": level,
                "summary_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "depth": len(node_path),
                "summary_text": summary_text,
                "source_event_ids": source_event_ids,
                "source_summary_hashes": source_summary_hashes,
                "source_entity_hashes": source_entity_hashes,
                "source_entity_types": source_entity_types,
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_scopes": source_memory_scopes,
                "source_session_continuities": source_session_continuities,
                "source_extraction_phases": source_extraction_phases,
                "source_final_session_boundary_count": source_final_session_boundary_count,
                "memory_scope": "user_profile" if "user_profile" in source_memory_scopes else ("session" if "session" in source_memory_scopes else ""),
                "session_continuity": "cross_session" if "cross_session" in source_session_continuities else ("same_session" if "same_session" in source_session_continuities else ""),
                "extraction_phase": "final" if "final" in source_extraction_phases else ("provisional" if "provisional" in source_extraction_phases else ""),
                "final_session_boundary": source_final_session_boundary_count > 0,
                "source_operator_hashes": source_operator_hashes,
                "summary_generation_policy": summary_policy,
                "dirty_hash": dirty_hash,
                "scope": scope,
                "updated_at_ms": refreshed_at_ms,
            }
        )
        records.append(
            {
                "record_type": "context_embedding",
                "embedding_type": embedding_type,
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "depth": len(node_path),
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "summary_generation_policy": summary_policy,
                "dirty_hash": dirty_hash,
                "scope": scope,
                "updated_at_ms": refreshed_at_ms,
            }
        )
    return {
        "records": records,
        "source_event_ids": source_event_ids,
        "source_summary_hashes": source_summary_hashes,
        "source_entity_hashes": source_entity_hashes,
        "source_entity_types": source_entity_types,
        "source_operator_hashes": source_operator_hashes,
        "source_roles": source_roles,
        "source_role_counts": source_role_counts,
        "source_hook_types": source_hook_types,
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_events": source_codex_events,
        "source_codex_event_counts": source_codex_event_counts,
        "source_memory_scopes": source_memory_scopes,
        "source_session_continuities": source_session_continuities,
        "source_extraction_phases": source_extraction_phases,
        "source_final_session_boundary_count": source_final_session_boundary_count,
        "generated_summary_types": [spec[0] for spec in summary_specs],
        "summary_generation_policy": l1_policy,
    }


MarkNodeSummaryDirty = Callable[..., list[int]]


def append_node_summary_embeddings(
    *,
    mark_node_summary_dirty: MarkNodeSummaryDirty,
    node_path: list[str],
    source_text: str,
    scope: Json,
    updated_at_ms: int,
    source_hash_field: str,
    source_hash: int,
) -> Json:
    del source_text
    dirty_hashes = mark_node_summary_dirty(
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


def async_summary_progress_records(
    *,
    records: list[Json],
    scope: Json,
    source_event_ids: list[int],
    source_entity_hashes: list[int],
    dirty_hash: object,
    node_hash: int,
    node_path: list[str],
    generated_summary_types: list[str],
    refreshed_at_ms: int,
    completed_followup_stages: list[str] | None = None,
) -> list[Json]:
    def compatible_scope(candidate: Json, requested: Json) -> bool:
        if scope_matches(candidate, requested):
            return True
        for key in ("account_id", "tenant_id", "user_id"):
            requested_value = str(requested.get(key) or "")
            candidate_value = str(candidate.get(key) or "")
            if requested_value and candidate_value and requested_value != candidate_value:
                return False
        requested_session = str(requested.get("session_id") or "")
        candidate_session = str(candidate.get("session_id") or "")
        return not requested_session or not candidate_session or requested_session == candidate_session

    source_event_set = {int(event_id) for event_id in source_event_ids if event_id is not None}
    source_entity_set = {int(entity_hash) for entity_hash in source_entity_hashes if entity_hash is not None}
    if source_entity_set:
        for record in records:
            if record.get("record_type") != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash"))
            except (TypeError, ValueError):
                continue
            if entity_hash not in source_entity_set:
                continue
            if not compatible_scope(candidate_access_scope(record), scope):
                continue
            entity_source_event_ids = record.get("source_event_ids")
            if not isinstance(entity_source_event_ids, list):
                continue
            for event_id in entity_source_event_ids:
                try:
                    source_event_set.add(int(event_id))
                except (TypeError, ValueError):
                    continue
    if not source_event_set:
        return []
    followup_stages = [
        str(stage)
        for stage in (completed_followup_stages or ["summary"])
        if str(stage or "")
    ]
    progress_records: list[Json] = []
    for record in records:
        if record.get("record_type") != "matrixark_async_pipeline_task":
            continue
        if str(record.get("status") or "") != "extraction_committed":
            continue
        try:
            event_id_hash = int(record.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        if event_id_hash not in source_event_set:
            continue
        if not compatible_scope(candidate_access_scope(record), scope):
            continue
        completed = [
            str(stage)
            for stage in (record.get("completed_stages") if isinstance(record.get("completed_stages"), list) else [])
            if str(stage or "")
        ]
        for stage in followup_stages:
            if stage not in completed:
                completed.append(stage)
        completed_stage_set = set(completed)
        remaining = [
            str(stage)
            for stage in (record.get("remaining_stages") if isinstance(record.get("remaining_stages"), list) else [])
            if str(stage or "") and str(stage) not in completed_stage_set
        ]
        progress_records.append(
            {
                **record,
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": record.get("task_hash", stable_hash(f"async_pipeline:{event_id_hash}")),
                "status": "summary_completed",
                "completed_stages": completed,
                "remaining_stages": remaining,
                "summary_completed": True,
                "embedding_completed": "embedding" in completed_stage_set,
                "compression_completed": "compression" in completed_stage_set,
                "summary_dirty_hash": dirty_hash,
                "summary_node_hash": node_hash,
                "summary_node_path": node_path,
                "generated_summary_types": generated_summary_types,
                "summary_completed_at_ms": refreshed_at_ms,
                "updated_at_ms": refreshed_at_ms,
            }
        )
    return progress_records


def refresh_dirty_node_summaries(
    adapter: object,
    *,
    scope: Json,
    limit: int = 64,
    refreshed_at_ms: int | None = None,
    max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
    compression_window_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
    min_compression_events: int = TIME_COMPRESSION_MIN_EVENTS,
    max_compression_windows_per_node: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
    min_compression_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
    raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
) -> Json:
    refreshed_at_ms = refreshed_at_ms or now_ms()
    records = adapter.read_all()
    pending_by_node = pending_dirty_node_records(
        records=records,
        scope=scope,
        limit=limit,
        refreshed_at_ms=refreshed_at_ms,
        max_raw_events_per_node=max_raw_events_per_node,
        min_compression_event_age_ms=min_compression_event_age_ms,
        context_event_ingestion_time_ms=adapter.context_event_ingestion_time_ms,
    )
    refreshed = []
    for dirty in sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]:
        node_path = [str(part) for part in dirty.get("node_path", [])]
        if not node_path:
            continue
        node_hash = int(dirty["node_hash"])
        events, child_summaries, entity_states, operator_states, summary_source_policy = adapter.node_summary_source_records(
            records=records,
            node_path=node_path,
            scope=dirty.get("scope", scope),
            node_hash=node_hash,
        )
        summary_refresh_records = build_node_summary_refresh_records(
            node_path=node_path,
            node_hash=node_hash,
            scope=dirty.get("scope", scope),
            events=events,
            child_summaries=child_summaries,
            entity_states=entity_states,
            operator_states=operator_states,
            summary_source_policy=summary_source_policy,
            dirty_hash=dirty.get("dirty_hash"),
            refreshed_at_ms=refreshed_at_ms,
        )
        adapter.append_many(summary_refresh_records["records"])
        source_event_ids = summary_refresh_records["source_event_ids"]
        source_summary_hashes = summary_refresh_records["source_summary_hashes"]
        source_entity_hashes = summary_refresh_records["source_entity_hashes"]
        source_entity_types = summary_refresh_records["source_entity_types"]
        source_operator_hashes = summary_refresh_records["source_operator_hashes"]
        source_roles = summary_refresh_records.get("source_roles", [])
        source_hook_types = summary_refresh_records.get("source_hook_types", [])
        source_codex_events = summary_refresh_records.get("source_codex_events", [])
        source_memory_scopes = summary_refresh_records.get("source_memory_scopes", [])
        source_session_continuities = summary_refresh_records.get("source_session_continuities", [])
        source_extraction_phases = summary_refresh_records.get("source_extraction_phases", [])
        source_final_session_boundary_count = int(summary_refresh_records.get("source_final_session_boundary_count") or 0)
        generated_summary_types = summary_refresh_records["generated_summary_types"]
        l1_policy = summary_refresh_records["summary_generation_policy"]
        compression_refresh = adapter.auto_time_compress_node_events(
            records=records,
            scope=dirty.get("scope", scope),
            node_hash=node_hash,
            node_path=node_path,
            compressed_time_ms=refreshed_at_ms,
            max_raw_events_per_node=max_raw_events_per_node,
            max_source_events=compression_window_events,
            min_source_events=min_compression_events,
            max_windows=max_compression_windows_per_node,
            min_event_age_ms=min_compression_event_age_ms,
            raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
        )
        adapter.append(
            {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty.get("dirty_hash"),
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": dirty.get("scope", scope),
                "status": "completed",
                "created_at_ms": refreshed_at_ms,
                "updated_at_ms": refreshed_at_ms,
                "completed_at_ms": refreshed_at_ms,
            }
        )
        if ENABLE_SUMMARY_REFRESH_AUDIT:
            adapter.append(
                {
                    "record_type": "context_summary_refresh_audit",
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": stable_hash(
                        f"summary_refresh:{node_hash}:{dirty.get('dirty_hash')}:{refreshed_at_ms}"
                    ),
                    "source_event_ids": source_event_ids,
                    "source_summary_hashes": source_summary_hashes,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                    "source_entity_count": len(source_entity_hashes),
                    "source_entity_types": source_entity_types,
                    "source_roles": source_roles,
                    "source_hook_types": source_hook_types,
                    "source_codex_events": source_codex_events,
                    "source_memory_scopes": source_memory_scopes,
                    "source_session_continuities": source_session_continuities,
                    "source_extraction_phases": source_extraction_phases,
                    "source_final_session_boundary_count": source_final_session_boundary_count,
                    "generated_summary_types": generated_summary_types,
                    "summary_generation_policy": l1_policy,
                    "time_compression_policy": {
                        "automatic": True,
                        "max_raw_events_per_node": max_raw_events_per_node,
                        "compression_window_events": compression_window_events,
                        "min_compression_events": min_compression_events,
                        "max_compression_windows_per_node": max_compression_windows_per_node,
                        "min_compression_event_age_ms": min_compression_event_age_ms,
                        "raw_event_ttl_after_compression_ms": raw_event_ttl_after_compression_ms,
                    },
                    "time_compression": compression_refresh,
                    "status": "refreshed",
                    "worker": "matrixark-local-async-summary-worker",
                    "refreshed_at_ms": refreshed_at_ms,
                    "scope": dirty.get("scope", scope),
                }
            )
        summary_progress_records = async_summary_progress_records(
            records=records,
            scope=dirty.get("scope", scope),
            source_event_ids=source_event_ids,
            source_entity_hashes=source_entity_hashes,
            dirty_hash=dirty.get("dirty_hash"),
            node_hash=node_hash,
            node_path=node_path,
            generated_summary_types=generated_summary_types,
            refreshed_at_ms=refreshed_at_ms,
            completed_followup_stages=["summary", "embedding", "compression"],
        )
        if summary_progress_records:
            adapter.append_many(summary_progress_records)
        refreshed.append(
            {
                "dirty_hash": dirty.get("dirty_hash"),
                "node_hash": node_hash,
                "node_path": node_path,
                "source_event_count": len(source_event_ids),
                "source_summary_count": len(source_summary_hashes),
                "source_entity_count": len(source_entity_hashes),
                "source_entity_types": source_entity_types,
                "source_roles": source_roles,
                "source_hook_types": source_hook_types,
                "source_codex_events": source_codex_events,
                "source_memory_scopes": source_memory_scopes,
                "source_session_continuities": source_session_continuities,
                "source_extraction_phases": source_extraction_phases,
                "source_final_session_boundary_count": source_final_session_boundary_count,
                "source_operator_count": len(source_operator_hashes),
                "generated_summary_types": generated_summary_types,
                "summary_generation_policy": l1_policy,
                "time_compression": compression_refresh,
                "async_summary_progress_count": len(summary_progress_records),
            }
        )
    return {
        "status": "ok",
        "refreshed_count": len(refreshed),
        "compression_created_count": sum(int(item.get("time_compression", {}).get("created_count", 0)) for item in refreshed),
        "refreshed": refreshed,
    }


def refresh_summaries(adapter: object, args: Json) -> Json:
    scope = optional_object(args, "scope")
    limit = args.get("limit", 64)
    if not isinstance(limit, int) or limit <= 0:
        raise MatrixArkError("limit must be a positive integer")
    refreshed_at_ms = args.get("refreshed_at_ms")
    if refreshed_at_ms is not None and not isinstance(refreshed_at_ms, int):
        raise MatrixArkError("refreshed_at_ms must be an integer")
    return adapter.refresh_dirty_node_summaries(
        scope=scope,
        limit=limit,
        refreshed_at_ms=refreshed_at_ms,
        max_raw_events_per_node=integer_arg(
            args,
            "max_raw_events_per_node",
            TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
            minimum=1,
        ),
        compression_window_events=integer_arg(
            args,
            "compression_window_events",
            TIME_COMPRESSION_WINDOW_EVENTS,
            minimum=1,
        ),
        min_compression_events=integer_arg(
            args,
            "min_compression_events",
            TIME_COMPRESSION_MIN_EVENTS,
            minimum=1,
        ),
        max_compression_windows_per_node=integer_arg(
            args,
            "max_compression_windows_per_node",
            TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
            minimum=0,
        ),
        min_compression_event_age_ms=integer_arg(
            args,
            "min_compression_event_age_ms",
            TIME_COMPRESSION_MIN_EVENT_AGE_MS,
            minimum=0,
        ),
        raw_event_ttl_after_compression_ms=integer_arg(
            args,
            "raw_event_ttl_after_compression_ms",
            TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
            minimum=0,
        ),
    )
