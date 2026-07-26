#!/usr/bin/env python3
"""Local MatrixArk serving-model recovery checks."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_latest_values import compact_latest_value_records
    from tools.matrixark_mcp_retrieval_records import RETRIEVAL_HOT_RECORD_TYPES, filter_retrieval_records
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_latest_values import compact_latest_value_records
    from matrixark_mcp_retrieval_records import RETRIEVAL_HOT_RECORD_TYPES, filter_retrieval_records


PRIMARY_RECOVERY_RECORD_TYPES = {
    "agent_message",
    "context_batch_commit",
    "context_entity",
    "context_event",
    "context_segment",
    "resource_chunk",
    "resource_manifest",
    "skill_registry_update",
    "skill_section",
}
DERIVED_RECOVERY_RECORD_TYPES = {
    "context_compression_event",
    "context_embedding",
    "context_index",
    "context_summary",
    "context_summary_dirty",
}
EMBEDDING_SOURCE_TYPES = {"context_entity", "context_event", "context_segment", "context_summary"}


def _record_identity(record: Json) -> tuple[str, Any] | None:
    record_type = str(record.get("record_type") or "")
    for field in (
        "event_id_hash",
        "entity_hash",
        "segment_hash",
        "summary_hash",
        "resource_hash",
        "chunk_hash",
        "skill_hash",
        "node_hash",
    ):
        value = record.get(field)
        if value not in (None, ""):
            return record_type, value
    return None


def _index_ref_hashes(record: Json) -> list[Any]:
    refs = record.get("ref_hashes")
    if isinstance(refs, list):
        return [ref for ref in refs if ref not in (None, "")]
    ref = record.get("ref_hash")
    return [] if ref in (None, "") else [ref]


def load_jsonl_records_for_recovery(path: Path) -> tuple[list[Json], list[Json]]:
    records: list[Json] = []
    errors: list[Json] = []
    lines = path.read_text(encoding="utf-8").splitlines()
    last_non_empty_line = 0
    for line_number, line in enumerate(lines, start=1):
        if line.strip():
            last_non_empty_line = line_number
    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            errors.append(
                {
                    "line": line_number,
                    "message": str(exc),
                    "corrupt_tail": line_number == last_non_empty_line,
                }
            )
            continue
        if isinstance(value, dict):
            records.append(value)
        else:
            errors.append(
                {
                    "line": line_number,
                    "message": "JSONL record must be an object",
                    "corrupt_tail": line_number == last_non_empty_line,
                }
            )
    return records, errors


def _retrieval_smoke_from_compacted_records(compacted: list[Json], scope: Json | None) -> Json:
    if not scope:
        return {"enabled": False, "reason": "scope_not_supplied"}
    filtered, stats = filter_retrieval_records(
        compacted,
        scope=scope,
        allowed_types=RETRIEVAL_HOT_RECORD_TYPES,
    )
    counts = Counter(str(record.get("record_type") or "unknown") for record in filtered)
    session_entities = [
        record for record in filtered
        if record.get("record_type") == "context_entity" and record.get("memory_scope") == "session"
    ]
    profile_entities = [
        record for record in filtered
        if record.get("record_type") == "context_entity" and record.get("memory_scope") == "user_profile"
    ]
    memory_scopes = Counter(
        str(record.get("memory_scope") or "unscoped")
        for record in filtered
        if record.get("record_type") in {"context_entity", "context_summary"}
    )
    session_continuities = Counter(
        str(record.get("session_continuity") or "neutral")
        for record in filtered
        if record.get("record_type") in {"context_entity", "context_summary"}
    )
    extraction_phases = Counter(
        str(record.get("extraction_phase") or "unknown")
        for record in filtered
        if record.get("record_type") in {"context_entity", "context_summary", "context_event"}
    )
    return {
        "enabled": True,
        "status": "ok" if filtered else "no_records",
        "scope": scope,
        "scan_stats": stats,
        "returned_record_counts": dict(sorted(counts.items())),
        "session_entity_count": len(session_entities),
        "profile_entity_count": len(profile_entities),
        "profile_entity_bridge_rebuildable": bool(profile_entities),
        "profile_cross_session_bridge_rebuildable": any(
            str(record.get("session_continuity") or "") == "cross_session"
            for record in profile_entities
        ),
        "memory_scope_counts": dict(sorted(memory_scopes.items())),
        "session_continuity_counts": dict(sorted(session_continuities.items())),
        "extraction_phase_counts": dict(sorted(extraction_phases.items())),
        "final_session_boundary_ref_count": sum(
            1 for record in filtered if bool(record.get("final_session_boundary"))
        ),
        "context_event_count": int(counts.get("context_event", 0)),
        "context_index_count": int(counts.get("context_index", 0)),
        "context_summary_count": int(counts.get("context_summary", 0)),
    }


def _derived_view_readiness(
    *,
    source_refs: set[tuple[str, Any]],
    embedding_refs: set[tuple[str, Any]],
    indexed_ref_hashes: set[Any],
    dirty_summary_count: int,
    summary_count: int,
) -> Json:
    missing_embeddings = source_refs - embedding_refs
    warnings: list[str] = []
    actions: list[str] = []
    if missing_embeddings:
        warnings.append("derived:embeddings_missing_or_stale")
        actions.append("rebuild context_embedding rows for source events/entities/segments/summaries")
    if not indexed_ref_hashes and source_refs:
        warnings.append("derived:indexes_missing")
        actions.append("rebuild context_index postings from persisted context models")
    if dirty_summary_count:
        warnings.append("derived:summaries_dirty")
        actions.append("run matrixark_refresh_summaries for dirty context nodes")
    if not summary_count and source_refs:
        warnings.append("derived:summaries_missing")
        actions.append("refresh or regenerate context_summary rows from durable source records")
    return {
        "status": "rebuild_required" if warnings else "ready",
        "warnings": warnings,
        "actions": actions,
        "missing_embedding_source_ref_count": len(missing_embeddings),
        "indexed_ref_count": len(indexed_ref_hashes),
        "dirty_summary_count": dirty_summary_count,
        "summary_count": summary_count,
    }


def matrixark_local_recovery_report(
    records: list[Json],
    *,
    parse_errors: list[Json] | None = None,
    scope: Json | None = None,
) -> Json:
    parse_errors = parse_errors or []
    compacted = compact_latest_value_records(records)
    record_counts = Counter(str(record.get("record_type") or "unknown") for record in records)
    compacted_counts = Counter(str(record.get("record_type") or "unknown") for record in compacted)
    hot_counts = {record_type: int(record_counts.get(record_type, 0)) for record_type in sorted(RETRIEVAL_HOT_RECORD_TYPES)}
    compacted_hot_counts = {record_type: int(compacted_counts.get(record_type, 0)) for record_type in sorted(RETRIEVAL_HOT_RECORD_TYPES)}

    source_refs: set[tuple[str, Any]] = set()
    embedding_refs: set[tuple[str, Any]] = set()
    indexed_ref_hashes: set[Any] = set()
    for record in compacted:
        record_type = str(record.get("record_type") or "")
        identity = _record_identity(record)
        if identity is not None and record_type in EMBEDDING_SOURCE_TYPES:
            source_refs.add(identity)
        if record_type == "context_embedding":
            ref_type = str(record.get("ref_type") or "")
            ref_hash = record.get("ref_hash")
            if ref_type and ref_hash not in (None, ""):
                embedding_refs.add((f"context_{ref_type}" if not ref_type.startswith("context_") else ref_type, ref_hash))
        if record_type == "context_index":
            indexed_ref_hashes.update(_index_ref_hashes(record))

    session_entities = [
        record for record in compacted
        if record.get("record_type") == "context_entity" and record.get("memory_scope") == "session"
    ]
    profile_entities = [
        record for record in compacted
        if record.get("record_type") == "context_entity" and record.get("memory_scope") == "user_profile"
    ]
    memory_hierarchy_records = [
        record for record in compacted
        if record.get("record_type") in {"context_entity", "context_summary"}
    ]
    memory_scope_counts = Counter(str(record.get("memory_scope") or "unscoped") for record in memory_hierarchy_records)
    session_continuity_counts = Counter(
        str(record.get("session_continuity") or "neutral") for record in memory_hierarchy_records
    )
    extraction_phase_counts = Counter(
        str(record.get("extraction_phase") or "unknown")
        for record in compacted
        if record.get("record_type") in {"context_entity", "context_summary", "context_event"}
    )
    source_roles = sorted(
        {
            str(role)
            for record in memory_hierarchy_records
            for role in (record.get("source_roles") if isinstance(record.get("source_roles"), list) else [])
            if str(role or "")
        }
    )
    source_hook_types = sorted(
        {
            str(hook_type)
            for record in memory_hierarchy_records
            for hook_type in (record.get("source_hook_types") if isinstance(record.get("source_hook_types"), list) else [])
            if str(hook_type or "")
        }
    )
    source_codex_events = sorted(
        {
            str(codex_event)
            for record in memory_hierarchy_records
            for codex_event in (record.get("source_codex_events") if isinstance(record.get("source_codex_events"), list) else [])
            if str(codex_event or "")
        }
    )
    dirty_summaries = [
        record for record in compacted
        if record.get("record_type") == "context_summary_dirty" and str(record.get("status") or "dirty") != "completed"
    ]
    telemetry_records = [
        record for record in records
        if record.get("record_type") == "context_pack_telemetry"
    ]
    audit_records = [
        record for record in records
        if record.get("record_type") == "context_pack_audit"
    ]
    telemetry_lifecycle_stages = sorted(
        {
            str(metadata.get("lifecycle_stage"))
            for record in telemetry_records
            for metadata in [record.get("retrieval_request_metadata")]
            if isinstance(metadata, dict) and str(metadata.get("lifecycle_stage") or "")
        }
    )
    hook_retrieval_telemetry = [
        record for record in telemetry_records
        if isinstance(record.get("retrieval_request_metadata"), dict)
        and str(record["retrieval_request_metadata"].get("retrieval_source") or record["retrieval_request_metadata"].get("source") or "") == "codex_hook_retrieve"
    ]
    profile_dirty_summaries = [
        record for record in dirty_summaries
        if str(record.get("dirty_reason") or "") == "profile_entity_promoted"
        or "profile:long_term_memory" in {str(part) for part in record.get("node_path", [])}
    ]
    session_dirty_summaries = [
        record for record in dirty_summaries
        if record not in profile_dirty_summaries
    ]
    corrupt_tail_count = sum(1 for item in parse_errors if item.get("corrupt_tail"))
    middle_parse_error_count = len(parse_errors) - corrupt_tail_count
    blockers: list[str] = []
    if middle_parse_error_count:
        blockers.append("recovery:middle_parse_errors")
    if corrupt_tail_count:
        blockers.append("recovery:corrupt_tail_detected")
    if records and not any(record_type in RETRIEVAL_HOT_RECORD_TYPES for record_type in record_counts):
        blockers.append("recovery:no_hot_serving_records")

    retrieval_smoke = _retrieval_smoke_from_compacted_records(compacted, scope)
    derived_readiness = _derived_view_readiness(
        source_refs=source_refs,
        embedding_refs=embedding_refs,
        indexed_ref_hashes=indexed_ref_hashes,
        dirty_summary_count=len(dirty_summaries),
        summary_count=int(record_counts.get("context_summary", 0)),
    )
    recovery_status = "empty" if not records else ("repair_required" if blockers else derived_readiness["status"])
    return {
        "status": recovery_status,
        "record_count": len(records),
        "compacted_record_count": len(compacted),
        "record_counts": dict(sorted(record_counts.items())),
        "compacted_record_counts": dict(sorted(compacted_counts.items())),
        "hot_memory_persisted": any(count > 0 for count in hot_counts.values()),
        "hot_record_counts": hot_counts,
        "compacted_hot_record_counts": compacted_hot_counts,
        "primary_record_counts": {
            record_type: int(record_counts.get(record_type, 0))
            for record_type in sorted(PRIMARY_RECOVERY_RECORD_TYPES)
        },
        "derived_record_counts": {
            record_type: int(record_counts.get(record_type, 0))
            for record_type in sorted(DERIVED_RECOVERY_RECORD_TYPES)
        },
        "cache_rebuild": {
            "read_cache_rebuildable_from_durable_log": bool(records) and not blockers,
            "retrieval_cache_rebuildable_from_hot_records": any(count > 0 for count in compacted_hot_counts.values()) and not blockers,
            "retrieval_visibility_rebuildable_from_durable_log": bool(telemetry_records or audit_records) and not blockers,
            "latest_value_compaction_rebuilt_records": len(compacted),
            "hot_record_types": sorted(RETRIEVAL_HOT_RECORD_TYPES),
        },
        "memory_hierarchy": {
            "session_entity_count": len(session_entities),
            "profile_entity_count": len(profile_entities),
            "session_dirty_summary_count": len(session_dirty_summaries),
            "profile_dirty_summary_count": len(profile_dirty_summaries),
            "profile_node_paths": sorted({"/".join(str(part) for part in record.get("node_path", [])) for record in profile_entities}),
            "source_session_ids": sorted({str(session_id) for record in profile_entities for session_id in record.get("source_session_ids", []) if str(session_id)}),
            "memory_scope_counts": dict(sorted(memory_scope_counts.items())),
            "session_continuity_counts": dict(sorted(session_continuity_counts.items())),
            "extraction_phase_counts": dict(sorted(extraction_phase_counts.items())),
            "final_session_boundary_ref_count": sum(
                1 for record in compacted if bool(record.get("final_session_boundary"))
            ),
            "source_roles": source_roles,
            "source_hook_types": source_hook_types,
            "source_codex_events": source_codex_events,
            "profile_cross_session_bridge_rebuildable": any(
                str(record.get("session_continuity") or "") == "cross_session"
                for record in profile_entities
            ),
        },
        "derived_views": {
            "index_posting_count": int(record_counts.get("context_index", 0)),
            "indexed_ref_count": len(indexed_ref_hashes),
            "embedding_count": int(record_counts.get("context_embedding", 0)),
            "embedding_source_ref_count": len(source_refs),
            "missing_embedding_source_ref_count": len(source_refs - embedding_refs),
            "dirty_summary_count": len(dirty_summaries),
            "session_dirty_summary_count": len(session_dirty_summaries),
            "profile_dirty_summary_count": len(profile_dirty_summaries),
            "summary_count": int(record_counts.get("context_summary", 0)),
            "readiness": derived_readiness,
        },
        "retrieval_visibility": {
            "telemetry_count": len(telemetry_records),
            "audit_count": len(audit_records),
            "hook_retrieval_telemetry_count": len(hook_retrieval_telemetry),
            "telemetry_rebuildable_from_durable_log": bool(telemetry_records) and not blockers,
            "context_pack_ids": sorted(
                {
                    str(record.get("context_pack_id"))
                    for record in telemetry_records + audit_records
                    if str(record.get("context_pack_id") or "")
                }
            ),
            "lifecycle_stages": telemetry_lifecycle_stages,
            "memory_layer_budget_record_count": sum(1 for record in telemetry_records if isinstance(record.get("memory_layer_budget"), dict)),
            "selected_ref_count": sum(int(record.get("selected_ref_count") or 0) for record in telemetry_records),
            "dropped_ref_count": sum(int(record.get("dropped_ref_count") or 0) for record in telemetry_records),
            "max_remote_context_budget_tokens": max(
                [int(record.get("remote_context_budget_tokens") or 0) for record in telemetry_records] or [0]
            ),
        },
        "retrieval_smoke": retrieval_smoke,
        "parse_errors": parse_errors,
        "blockers": blockers,
        "warnings": derived_readiness["warnings"] if not blockers else [],
        "recovery_actions": (
            ["repair durable log blockers before serving rebuild"] + derived_readiness["actions"]
            if blockers
            else derived_readiness["actions"]
        ),
        "rebuild_plan": [
            "scan durable MatrixArk records",
            "truncate or quarantine corrupt tail if reported",
            "compact latest-value serving records",
            "rebuild read/retrieval/entity hot caches from compacted records",
            "rebuild context_index and context_embedding views when stale or missing",
            "run refresh_summaries for context_summary_dirty records",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", type=Path, required=True, help="Path to the local MatrixArk JSONL event log.")
    parser.add_argument("--out", type=Path, help="Optional JSON report output path.")
    parser.add_argument("--scope-json", help="Optional retrieval scope JSON for a rebuild smoke check.")
    args = parser.parse_args()
    scope: Json | None = None
    if args.scope_json:
        loaded_scope = json.loads(args.scope_json)
        if not isinstance(loaded_scope, dict):
            raise SystemExit("--scope-json must decode to an object")
        scope = loaded_scope
    records, errors = load_jsonl_records_for_recovery(args.event_log)
    report = matrixark_local_recovery_report(records, parse_errors=errors, scope=scope)
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    if report["status"] in {"ready", "empty"}:
        return 0
    if report["status"] == "rebuild_required":
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
