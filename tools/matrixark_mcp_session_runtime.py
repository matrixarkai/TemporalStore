#!/usr/bin/env python3
"""Session buffer commit runtime for MatrixArk MCP adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_storage_route,
        messages_from_event_record,
        normalize_message_role,
        normalize_storage_options,
        now_ms,
        optional_object,
        optional_string,
        ordered_unique_any,
        session_buffer_key_from_scope,
        session_buffer_key,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_storage_route,
        messages_from_event_record,
        normalize_message_role,
        normalize_storage_options,
        now_ms,
        optional_object,
        optional_string,
        ordered_unique_any,
        session_buffer_key_from_scope,
        session_buffer_key,
        stable_hash,
    )


def _event_source_count_summary(envelope: Json, hook: Json | None) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    hook = hook if isinstance(hook, dict) else {}
    role_counts: Json = {}
    hook_type_counts: Json = {}
    codex_event_counts: Json = {}
    selection_policy_counts: Json = {}
    selection_lossy_count = 0
    selection_complete_count = 0
    selection_dropped_text_chars = 0
    selection_dropped_line_count = 0
    selection_retained_text_ratio_sum = 0.0
    selection_retained_line_ratio_sum = 0.0
    selection_ratio_count = 0

    def add_count(bucket: Json, key: object, amount: object = 1, *, normalize_role_value: bool = False) -> None:
        name = normalize_message_role(key) if normalize_role_value else str(key or "").strip()
        if not name:
            return
        try:
            count = max(0, int(amount or 0))
        except (TypeError, ValueError):
            count = 0
        if count:
            bucket[name] = int(bucket.get(name, 0)) + count

    def add_values(bucket: Json, values: object, *, normalize_role_value: bool = False) -> None:
        if isinstance(values, list):
            for value in values:
                add_count(bucket, value, normalize_role_value=normalize_role_value)
        else:
            add_count(bucket, values, normalize_role_value=normalize_role_value)

    metadata_role_counts = metadata.get("source_role_counts") if isinstance(metadata.get("source_role_counts"), dict) else {}
    if metadata_role_counts:
        for role, count in metadata_role_counts.items():
            add_count(role_counts, role, count, normalize_role_value=True)
    else:
        for message in envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []:
            if isinstance(message, dict):
                add_count(role_counts, message.get("role"), normalize_role_value=True)
        add_values(role_counts, metadata.get("source_roles"), normalize_role_value=True)

    metadata_hook_type_counts = metadata.get("source_hook_type_counts") if isinstance(metadata.get("source_hook_type_counts"), dict) else {}
    if metadata_hook_type_counts:
        for hook_type, count in metadata_hook_type_counts.items():
            add_count(hook_type_counts, hook_type, count)
    else:
        add_values(hook_type_counts, envelope.get("hook_type"))
        add_values(hook_type_counts, metadata.get("hook_type"))
        add_values(hook_type_counts, metadata.get("source_hook_types"))
        add_values(hook_type_counts, hook.get("hook_type"))

    metadata_codex_event_counts = metadata.get("source_codex_event_counts") if isinstance(metadata.get("source_codex_event_counts"), dict) else {}
    if metadata_codex_event_counts:
        for codex_event, count in metadata_codex_event_counts.items():
            add_count(codex_event_counts, codex_event, count)
    else:
        add_values(codex_event_counts, envelope.get("codex_event"))
        add_values(codex_event_counts, metadata.get("codex_event"))
        add_values(codex_event_counts, metadata.get("source_codex_events"))
        add_values(codex_event_counts, hook.get("codex_event"))
        add_values(codex_event_counts, hook.get("trigger"))

    metadata_selection_counts = (
        metadata.get("source_memory_selection_policy_counts")
        if isinstance(metadata.get("source_memory_selection_policy_counts"), dict)
        else {}
    )
    if metadata_selection_counts:
        for policy, count in metadata_selection_counts.items():
            add_count(selection_policy_counts, policy, count)
    else:
        add_values(selection_policy_counts, metadata.get("source_memory_selection_policies"))
        selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
        add_count(selection_policy_counts, selection.get("policy"))
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    if selection:
        selection_lossy_count += 1 if bool(selection.get("selection_lossy")) else 0
        selection_complete_count += 0 if bool(selection.get("selection_lossy")) else 1
        try:
            selection_dropped_text_chars += max(0, int(selection.get("dropped_text_chars") or 0))
        except (TypeError, ValueError):
            pass
        try:
            selection_dropped_line_count += max(0, int(selection.get("dropped_line_count") or 0))
        except (TypeError, ValueError):
            pass
        text_ratio: float | None = None
        try:
            text_ratio = float(selection.get("retained_text_ratio"))
            selection_retained_text_ratio_sum += text_ratio
            selection_ratio_count += 1
        except (TypeError, ValueError):
            pass
        try:
            selection_retained_line_ratio_sum += float(selection.get("retained_line_ratio"))
        except (TypeError, ValueError):
            if text_ratio is not None:
                selection_retained_line_ratio_sum += text_ratio
    for field in [
        "source_memory_selection_lossy_count",
        "source_memory_selection_complete_count",
        "source_memory_selection_dropped_text_chars",
        "source_memory_selection_dropped_line_count",
    ]:
        try:
            value = max(0, int(metadata.get(field) or 0))
        except (TypeError, ValueError):
            value = 0
        if field == "source_memory_selection_lossy_count":
            selection_lossy_count += value
        elif field == "source_memory_selection_complete_count":
            selection_complete_count += value
        elif field == "source_memory_selection_dropped_text_chars":
            selection_dropped_text_chars += value
        elif field == "source_memory_selection_dropped_line_count":
            selection_dropped_line_count += value
    result = {
        "source_roles": sorted(role_counts),
        "source_role_counts": dict(sorted(role_counts.items())),
        "source_hook_types": sorted(hook_type_counts),
        "source_hook_type_counts": dict(sorted(hook_type_counts.items())),
        "source_codex_events": sorted(codex_event_counts),
        "source_codex_event_counts": dict(sorted(codex_event_counts.items())),
    }
    if selection_policy_counts:
        result["source_memory_selection_policies"] = sorted(selection_policy_counts)
        result["source_memory_selection_policy_counts"] = dict(sorted(selection_policy_counts.items()))
    if selection_lossy_count:
        result["source_memory_selection_lossy_count"] = selection_lossy_count
    if selection_complete_count:
        result["source_memory_selection_complete_count"] = selection_complete_count
    if selection_dropped_text_chars:
        result["source_memory_selection_dropped_text_chars"] = selection_dropped_text_chars
    if selection_dropped_line_count:
        result["source_memory_selection_dropped_line_count"] = selection_dropped_line_count
    if selection_ratio_count:
        result["source_memory_selection_retained_text_ratio_avg"] = round(
            selection_retained_text_ratio_sum / selection_ratio_count,
            6,
        )
        result["source_memory_selection_retained_line_ratio_avg"] = round(
            selection_retained_line_ratio_sum / selection_ratio_count,
            6,
        )
    return result


def append_session_buffer_event(
    adapter: object,
    *,
    envelope: Json,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    hook: Json | None,
) -> None:
    key = session_buffer_key(envelope)
    source_counts = _event_source_count_summary(envelope, hook)
    adapter.append(
        {
            "record_type": "session_buffer_event",
            "buffer_key_hash": stable_hash(":".join(key)),
            "buffer_key": list(key),
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "node_path": node_path,
            "scope": envelope["scope"],
            "status": "pending",
            "envelope": envelope,
            "agent_hook": hook,
            **source_counts,
            "created_at_ms": envelope["ingestion_time_ms"],
        }
    )


def session_commit_memory_layers_written(
    batch_result: Json,
    *,
    extraction_phase: str,
    final_session_boundary: bool,
    source_roles: list[str] | None = None,
    source_hook_types: list[str] | None = None,
    source_codex_events: list[str] | None = None,
) -> Json:
    entities_written = int(batch_result.get("entities_written") or 0)
    profile_entities_written = int(batch_result.get("profile_entities_written") or 0)
    summary_refresh = batch_result.get("summary_refresh") if isinstance(batch_result.get("summary_refresh"), dict) else {}
    summary_dirty_hashes = summary_refresh.get("dirty_hashes") if isinstance(summary_refresh.get("dirty_hashes"), list) else []
    layers: Json = {
        "context_events": int(batch_result.get("events_written") or 0),
        "segments": int(batch_result.get("segments_written") or 0),
        "session_entities": entities_written,
        "profile_entities": profile_entities_written,
        "same_session_entities": entities_written,
        "cross_session_entities": profile_entities_written,
        "secondary_indexes": int(batch_result.get("indexes_written") or 0),
        "summary_dirty_nodes": len(summary_dirty_hashes),
        "summary_refresh_status": summary_refresh.get("status"),
        "extraction_phase": extraction_phase,
        "final_session_boundary": final_session_boundary,
        "source_roles": source_roles,
        "source_hook_types": source_hook_types,
        "source_codex_events": source_codex_events,
    }
    return {key: value for key, value in layers.items() if value not in (None, "", [], {})}


def _pending_source_count_summary(records: list[Json]) -> Json:
    role_counts: Json = {}
    hook_type_counts: Json = {}
    codex_event_counts: Json = {}
    selection_policy_counts: Json = {}
    selection_lossy_count = 0
    selection_complete_count = 0
    selection_dropped_text_chars = 0
    selection_dropped_line_count = 0
    selection_retained_text_ratio_sum = 0.0
    selection_retained_line_ratio_sum = 0.0
    selection_ratio_count = 0

    def add_count(bucket: Json, key: object, amount: object = 1, *, normalize_role: bool = False) -> None:
        name = normalize_message_role(key) if normalize_role else str(key or "").strip()
        if not name:
            return
        try:
            count = max(0, int(amount or 0))
        except (TypeError, ValueError):
            count = 0
        if count:
            bucket[name] = int(bucket.get(name, 0)) + count

    def add_values(bucket: Json, values: object) -> None:
        if isinstance(values, list):
            for value in values:
                add_count(bucket, value)
        else:
            add_count(bucket, values)

    def add_int(value: object) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def add_ratio(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    for record in records:
        event_envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        event_metadata = event_envelope.get("metadata", {}) if isinstance(event_envelope.get("metadata"), dict) else {}
        event_hook = record.get("agent_hook", {}) if isinstance(record.get("agent_hook"), dict) else {}
        existing_role_counts = record.get("source_role_counts") if isinstance(record.get("source_role_counts"), dict) else {}
        if existing_role_counts:
            for role, count in existing_role_counts.items():
                add_count(role_counts, role, count, normalize_role=True)
        else:
            for message in messages_from_event_record(record):
                add_count(role_counts, message.get("role"), normalize_role=True)
            for role in event_metadata.get("source_roles") if isinstance(event_metadata.get("source_roles"), list) else []:
                add_count(role_counts, role, normalize_role=True)
        existing_hook_counts = record.get("source_hook_type_counts") if isinstance(record.get("source_hook_type_counts"), dict) else {}
        if existing_hook_counts:
            for hook_type, count in existing_hook_counts.items():
                add_count(hook_type_counts, hook_type, count)
        else:
            add_values(hook_type_counts, event_envelope.get("hook_type"))
            add_values(hook_type_counts, event_metadata.get("hook_type"))
            add_values(hook_type_counts, event_metadata.get("source_hook_types"))
            add_values(hook_type_counts, event_hook.get("hook_type"))
        existing_codex_counts = record.get("source_codex_event_counts") if isinstance(record.get("source_codex_event_counts"), dict) else {}
        if existing_codex_counts:
            for codex_event, count in existing_codex_counts.items():
                add_count(codex_event_counts, codex_event, count)
        else:
            add_values(codex_event_counts, event_envelope.get("codex_event"))
            add_values(codex_event_counts, event_metadata.get("codex_event"))
            add_values(codex_event_counts, event_metadata.get("source_codex_events"))
            add_values(codex_event_counts, event_hook.get("codex_event"))
            add_values(codex_event_counts, event_hook.get("trigger"))
        existing_selection_counts = (
            record.get("source_memory_selection_policy_counts")
            if isinstance(record.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        metadata_selection_counts = (
            event_metadata.get("source_memory_selection_policy_counts")
            if isinstance(event_metadata.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        if existing_selection_counts or metadata_selection_counts:
            for counts in [metadata_selection_counts, existing_selection_counts]:
                for policy, count in counts.items():
                    amount = add_int(count)
                    if amount:
                        selection_policy_counts[str(policy)] = int(selection_policy_counts.get(str(policy), 0)) + amount
        else:
            add_values(selection_policy_counts, record.get("source_memory_selection_policies"))
            add_values(selection_policy_counts, event_metadata.get("source_memory_selection_policies"))
            selection = (
                record.get("codex_memory_selection")
                if isinstance(record.get("codex_memory_selection"), dict)
                else event_envelope.get("codex_memory_selection")
                if isinstance(event_envelope.get("codex_memory_selection"), dict)
                else event_metadata.get("codex_memory_selection")
                if isinstance(event_metadata.get("codex_memory_selection"), dict)
                else {}
            )
            add_count(selection_policy_counts, selection.get("policy"))
        selection_lossy_count += add_int(record.get("source_memory_selection_lossy_count"))
        selection_lossy_count += add_int(event_metadata.get("source_memory_selection_lossy_count"))
        selection_complete_count += add_int(record.get("source_memory_selection_complete_count"))
        selection_complete_count += add_int(event_metadata.get("source_memory_selection_complete_count"))
        selection_dropped_text_chars += add_int(record.get("source_memory_selection_dropped_text_chars"))
        selection_dropped_text_chars += add_int(event_metadata.get("source_memory_selection_dropped_text_chars"))
        selection_dropped_line_count += add_int(record.get("source_memory_selection_dropped_line_count"))
        selection_dropped_line_count += add_int(event_metadata.get("source_memory_selection_dropped_line_count"))
        selection = (
            record.get("codex_memory_selection")
            if isinstance(record.get("codex_memory_selection"), dict)
            else event_envelope.get("codex_memory_selection")
            if isinstance(event_envelope.get("codex_memory_selection"), dict)
            else event_metadata.get("codex_memory_selection")
            if isinstance(event_metadata.get("codex_memory_selection"), dict)
            else {}
        )
        if selection:
            if "source_memory_selection_lossy_count" not in record and "source_memory_selection_lossy_count" not in event_metadata:
                selection_lossy_count += 1 if bool(selection.get("selection_lossy")) else 0
            if "source_memory_selection_complete_count" not in record and "source_memory_selection_complete_count" not in event_metadata:
                selection_complete_count += 0 if bool(selection.get("selection_lossy")) else 1
            selection_dropped_text_chars += add_int(selection.get("dropped_text_chars"))
            selection_dropped_line_count += add_int(selection.get("dropped_line_count"))
            text_ratio = add_ratio(selection.get("retained_text_ratio"))
            line_ratio = add_ratio(selection.get("retained_line_ratio"))
            if text_ratio is not None:
                selection_retained_text_ratio_sum += text_ratio
                selection_retained_line_ratio_sum += line_ratio if line_ratio is not None else text_ratio
                selection_ratio_count += 1
        else:
            text_ratio = add_ratio(record.get("source_memory_selection_retained_text_ratio_avg"))
            if text_ratio is None:
                text_ratio = add_ratio(event_metadata.get("source_memory_selection_retained_text_ratio_avg"))
            line_ratio = add_ratio(record.get("source_memory_selection_retained_line_ratio_avg"))
            if line_ratio is None:
                line_ratio = add_ratio(event_metadata.get("source_memory_selection_retained_line_ratio_avg"))
            if text_ratio is not None:
                selection_retained_text_ratio_sum += text_ratio
                selection_retained_line_ratio_sum += line_ratio if line_ratio is not None else text_ratio
                selection_ratio_count += 1

    result = {
        "source_role_counts": dict(sorted(role_counts.items())),
        "source_hook_type_counts": dict(sorted(hook_type_counts.items())),
        "source_codex_event_counts": dict(sorted(codex_event_counts.items())),
    }
    if selection_policy_counts:
        result["source_memory_selection_policies"] = sorted(selection_policy_counts)
        result["source_memory_selection_policy_counts"] = dict(sorted(selection_policy_counts.items()))
    if selection_lossy_count:
        result["source_memory_selection_lossy_count"] = selection_lossy_count
    if selection_complete_count:
        result["source_memory_selection_complete_count"] = selection_complete_count
    if selection_dropped_text_chars:
        result["source_memory_selection_dropped_text_chars"] = selection_dropped_text_chars
    if selection_dropped_line_count:
        result["source_memory_selection_dropped_line_count"] = selection_dropped_line_count
    if selection_ratio_count:
        result["source_memory_selection_retained_text_ratio_avg"] = round(
            selection_retained_text_ratio_sum / selection_ratio_count,
            6,
        )
        result["source_memory_selection_retained_line_ratio_avg"] = round(
            selection_retained_line_ratio_sum / selection_ratio_count,
            6,
        )
    return result


def session_event_message_count(records: list[Json]) -> int:
    return sum(len(messages_from_event_record(record)) for record in records)


def session_events_by_message_limit(records: list[Json], limit: int | None) -> list[Json]:
    if limit is None:
        return records
    selected: list[Json] = []
    message_count = 0
    for record in records:
        selected.append(record)
        message_count += max(1, len(messages_from_event_record(record)))
        if message_count >= limit:
            break
    return selected


def append_session_commit_task_progress(
    adapter: object,
    *,
    source_event_ids: list[int],
    source_roles: list[str],
    source_role_counts: Json,
    source_hook_types: list[str],
    source_hook_type_counts: Json,
    source_codex_events: list[str],
    source_codex_event_counts: Json,
    commit_id_hash: int,
    batch_id_hash: int | None,
    scope: Json,
    trigger_policy: str,
    extraction_phase: str,
    final_session_boundary: bool,
    memory_layers_written: Json,
    updated_at_ms: int,
    source_memory_selection_policies: list[str] | None = None,
    source_memory_selection_policy_counts: Json | None = None,
    source_memory_selection_retention: Json | None = None,
) -> None:
    records: list[Json] = []
    source_memory_selection_policies = source_memory_selection_policies or []
    source_memory_selection_policy_counts = source_memory_selection_policy_counts or {}
    source_memory_selection_retention = source_memory_selection_retention or {}
    for event_id in source_event_ids:
        records.append(
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": stable_hash(f"async_pipeline:{event_id}"),
                "event_id_hash": event_id,
                "commit_id_hash": commit_id_hash,
                "batch_id_hash": batch_id_hash,
                "scope": scope,
                "status": "extraction_committed",
                "stages": ["extraction", "summary", "compression", "embedding"],
                "completed_stages": ["extraction"],
                "remaining_stages": ["summary", "compression", "embedding"],
                "reason": "session_buffer_commit",
                "trigger_policy": trigger_policy,
                "extraction_phase": extraction_phase,
                "final_session_boundary": final_session_boundary,
                "source_roles": source_roles,
                "source_role_counts": source_role_counts,
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": source_hook_type_counts,
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": source_codex_event_counts,
                "source_memory_selection_policies": source_memory_selection_policies,
                "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
                **source_memory_selection_retention,
                "summary_refresh_status": memory_layers_written.get("summary_refresh_status"),
                "summary_dirty_nodes": memory_layers_written.get("summary_dirty_nodes", 0),
                "memory_layers_written": memory_layers_written,
                "updated_at_ms": updated_at_ms,
            }
        )
    if not records:
        return
    append_many = getattr(adapter, "append_many", None)
    if callable(append_many):
        append_many(records)
        return
    append = getattr(adapter, "append", None)
    if callable(append):
        for record in records:
            append(record)


def _append_records(adapter: object, records: list[Json]) -> None:
    if not records:
        return
    append_many = getattr(adapter, "append_many", None)
    if callable(append_many):
        append_many(records)
        return
    append = getattr(adapter, "append", None)
    if callable(append):
        for record in records:
            append(record)


def _source_lineage_summary(records: list[Json]) -> Json:
    counts = _pending_source_count_summary(records)
    lineage: Json = {
        "source_roles": sorted(counts.get("source_role_counts", {})),
        "source_role_counts": counts.get("source_role_counts", {}),
        "source_hook_types": sorted(counts.get("source_hook_type_counts", {})),
        "source_hook_type_counts": counts.get("source_hook_type_counts", {}),
        "source_codex_events": sorted(counts.get("source_codex_event_counts", {})),
        "source_codex_event_counts": counts.get("source_codex_event_counts", {}),
    }
    for field in [
        "source_memory_selection_policies",
        "source_memory_selection_policy_counts",
        "source_memory_selection_lossy_count",
        "source_memory_selection_complete_count",
        "source_memory_selection_dropped_text_chars",
        "source_memory_selection_dropped_line_count",
        "source_memory_selection_retained_text_ratio_avg",
        "source_memory_selection_retained_line_ratio_avg",
    ]:
        value = counts.get(field)
        if value not in (None, "", [], {}):
            lineage[field] = value
    extraction_phases = sorted({
        str(record.get("extraction_phase") or "").strip()
        for record in records
        if str(record.get("extraction_phase") or "").strip()
    })
    if extraction_phases:
        lineage["source_extraction_phases"] = extraction_phases
    return {key: value for key, value in lineage.items() if value not in (None, "", [], {})}


def session_commit(adapter: object, args: Json, *, hook: Json | None = None) -> Json:
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
    pending_all = adapter.pending_session_events(scope)
    pending_event_count = len(pending_all)
    pending_message_count = session_event_message_count(pending_all)
    idle_elapsed_ms = 0
    idle_ready = False
    if pending_all and idle_timeout_ms is not None:
        latest_event_time = max(
            int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            for record in pending_all
        )
        idle_elapsed_ms = max(0, now_ms() - latest_event_time)
        idle_ready = idle_elapsed_ms >= idle_timeout_ms
    threshold_ready = pending_event_count >= threshold or pending_message_count >= threshold
    trigger_evidence: Json = {
        "pending_event_count": pending_event_count,
        "pending_message_count": pending_message_count,
        "threshold_messages": threshold,
        "threshold_ready": threshold_ready,
        "idle_timeout_ms": idle_timeout_ms,
        "idle_elapsed_ms": idle_elapsed_ms,
        "idle_ready": idle_ready,
        "force": force,
        "commit_reason": commit_reason,
    }
    if not force and not threshold_ready and not idle_ready:
        return {
            "status": "deferred",
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "commit_reason": commit_reason,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "trigger_evidence": trigger_evidence,
            "reason": "session buffer below extraction threshold and idle timeout not reached",
        }
    trigger_policy = "force" if force else "idle_timeout" if idle_ready else "threshold"
    extraction_phase = "final" if force else "provisional"
    final_session_boundary = extraction_phase == "final"
    if max_messages is not None:
        commit_limit = max_messages
    elif force or idle_ready:
        commit_limit = None
    else:
        commit_limit = threshold
    pending = session_events_by_message_limit(pending_all, commit_limit)
    messages = []
    source_event_ids = []
    pending_source_roles: set[str] = set()
    pending_source_hook_types: set[str] = set()
    pending_source_codex_events: set[str] = set()
    for record in pending:
        event_envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        event_metadata = event_envelope.get("metadata", {}) if isinstance(event_envelope.get("metadata"), dict) else {}
        event_hook = record.get("agent_hook", {}) if isinstance(record.get("agent_hook"), dict) else {}
        for value in [event_envelope.get("hook_type"), event_metadata.get("hook_type"), event_hook.get("hook_type")]:
            if str(value or "").strip():
                pending_source_hook_types.add(str(value).strip())
        for values in [event_metadata.get("source_hook_types")]:
            if isinstance(values, list):
                pending_source_hook_types.update(str(value).strip() for value in values if str(value or "").strip())
        for value in [event_envelope.get("codex_event"), event_metadata.get("codex_event"), event_hook.get("codex_event"), event_hook.get("trigger")]:
            if str(value or "").strip():
                pending_source_codex_events.add(str(value).strip())
        for values in [event_metadata.get("source_codex_events")]:
            if isinstance(values, list):
                pending_source_codex_events.update(str(value).strip() for value in values if str(value or "").strip())
        record_messages = messages_from_event_record(record)
        if not record_messages:
            continue
        for message in record_messages:
            role = normalize_message_role(message.get("role"))
            if role:
                pending_source_roles.add(role)
            messages.append(message)
        for values in [event_metadata.get("source_roles")]:
            if isinstance(values, list):
                pending_source_roles.update(str(value).strip() for value in values if str(value or "").strip())
        source_event_ids.append(record["event_id_hash"])
    if not messages:
        if force and final_session_boundary:
            session_key = session_buffer_key_from_scope(scope)
            read_all = getattr(adapter, "read_all", None)
            records = read_all() if callable(read_all) else []
            prior_commits = [
                record
                for record in records
                if record.get("record_type") == "context_batch_commit"
                and session_buffer_key_from_scope(record.get("scope", {})) == session_key
            ]
            already_finalized = any(
                (
                    record.get("record_type") == "context_session_boundary"
                    and bool(record.get("final_session_boundary"))
                    and session_buffer_key_from_scope(record.get("scope", {})) == session_key
                )
                or (
                    record.get("record_type") == "context_batch_commit"
                    and bool(record.get("final_session_boundary"))
                    and session_buffer_key_from_scope(record.get("scope", {})) == session_key
                )
                for record in records
            )
            if prior_commits and not already_finalized:
                node_path = adapter.default_session_node_path(scope)
                node_hash = stable_hash("/".join(node_path))
                committed_event_ids: list[int] = []
                for commit in prior_commits:
                    for event_id in commit.get("source_event_ids", []):
                        try:
                            committed_event_ids.append(int(event_id))
                        except (TypeError, ValueError):
                            continue
                unique_event_ids = ordered_unique_any(committed_event_ids)
                source_lineage = _source_lineage_summary(prior_commits)
                finalized_at_ms = now_ms()
                boundary_hash = stable_hash(f"session_boundary:{session_key}:{unique_event_ids}:final")
                boundary_record: Json = {
                    "record_type": "context_session_boundary",
                    "boundary_hash": boundary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "summary_text": (
                        f"Session finalized after {len(prior_commits)} provisional commit(s) "
                        f"covering {len(unique_event_ids)} event(s)."
                    ),
                    "status": "finalized",
                    "commit_reason": commit_reason,
                    "trigger_policy": trigger_policy,
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "prior_commit_count": len(prior_commits),
                    "prior_committed_event_count": len(unique_event_ids),
                    "source_event_count": len(unique_event_ids),
                    **source_lineage,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "created_at_ms": finalized_at_ms,
                    "updated_at_ms": finalized_at_ms,
                }
                dirty_hashes: list[int] = []
                dirty_records: list[Json] = []
                node_summary_dirty_records = getattr(adapter, "node_summary_dirty_records", None)
                if callable(node_summary_dirty_records):
                    dirty_hashes, dirty_records = node_summary_dirty_records(
                        node_path=node_path,
                        scope=scope,
                        updated_at_ms=finalized_at_ms,
                        source_ref_type="session_boundary",
                        source_hash_field="source_boundary_hash",
                        source_hash=boundary_hash,
                        dirty_reason="session_finalized",
                        propagate_depth=0,
                        source_lineage=boundary_record,
                    )
                _append_records(adapter, [boundary_record, *dirty_records])
                return {
                    "status": "finalized",
                    "pending_event_count": pending_event_count,
                    "pending_message_count": pending_message_count,
                    "threshold_messages": threshold,
                    "commit_reason": commit_reason,
                    "trigger_policy": trigger_policy,
                    "idle_timeout_ms": idle_timeout_ms,
                    "idle_elapsed_ms": idle_elapsed_ms,
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "boundary_hash": boundary_hash,
                    "prior_commit_count": len(prior_commits),
                    "prior_committed_event_count": len(unique_event_ids),
                    "summary_refresh": {
                        "status": "dirty_marked" if dirty_hashes else "not_available",
                        "dirty_hashes": dirty_hashes,
                        "dirty_reason": "session_finalized",
                    },
                    "trigger_evidence": {
                        **trigger_evidence,
                        "already_finalized": False,
                        "prior_commit_count": len(prior_commits),
                    },
                }
            if already_finalized:
                trigger_evidence = {
                    **trigger_evidence,
                    "already_finalized": True,
                    "prior_commit_count": len(prior_commits),
                }
        return {
            "status": "empty",
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "commit_reason": commit_reason,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "trigger_evidence": trigger_evidence,
        }
    try:
        overlap_limit = int(args.get("extraction_context_overlap_messages", 2))
    except (TypeError, ValueError):
        overlap_limit = 2
    if force:
        overlap_limit = 0
    overlap_limit = max(0, overlap_limit)
    current_source_event_ids = {int(event_id) for event_id in source_event_ids}
    committed_event_ids: set[int] = set()
    session_key = session_buffer_key_from_scope(scope)
    records_for_overlap = adapter.read_all() if overlap_limit else []
    for record in records_for_overlap:
        if record.get("record_type") != "context_batch_commit" or session_buffer_key_from_scope(record.get("scope", {})) != session_key:
            continue
        for event_id in record.get("source_event_ids", []):
            try:
                committed_event_ids.add(int(event_id))
            except (TypeError, ValueError):
                continue
    overlap_records: list[Json] = []
    for record in records_for_overlap:
        if record.get("record_type") != "context_event":
            continue
        try:
            event_id = int(record.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        if event_id in current_source_event_ids or event_id not in committed_event_ids:
            continue
        overlap_records.append(record)
    overlap_records = overlap_records[-overlap_limit:]
    extraction_context_messages = [
        message
        for record in overlap_records
        for message in messages_from_event_record(record)
        if message
    ]
    extraction_context_event_ids = [
        int(record["event_id_hash"])
        for record in overlap_records
        if record.get("event_id_hash") is not None
    ]
    metadata = optional_object(args, "metadata")
    source_roles = sorted(pending_source_roles)
    source_hook_types = sorted(pending_source_hook_types)
    source_codex_events = sorted(pending_source_codex_events)
    source_counts = _pending_source_count_summary(pending)
    source_role_counts = source_counts["source_role_counts"]
    source_hook_type_counts = source_counts["source_hook_type_counts"]
    source_codex_event_counts = source_counts["source_codex_event_counts"]
    source_memory_selection_policies = source_counts.get("source_memory_selection_policies", [])
    source_memory_selection_policy_counts = source_counts.get("source_memory_selection_policy_counts", {})
    source_memory_selection_retention: Json = {
        key: source_counts.get(key)
        for key in [
            "source_memory_selection_lossy_count",
            "source_memory_selection_complete_count",
            "source_memory_selection_dropped_text_chars",
            "source_memory_selection_dropped_line_count",
            "source_memory_selection_retained_text_ratio_avg",
            "source_memory_selection_retained_line_ratio_avg",
        ]
        if source_counts.get(key) not in (None, "", [], {})
    }
    if source_roles:
        metadata = {**metadata, "source_roles": source_roles, "source_role_counts": source_role_counts}
    if pending_source_hook_types:
        metadata = {**metadata, "source_hook_types": source_hook_types, "source_hook_type_counts": source_hook_type_counts}
        if "hook_type" not in metadata and len(pending_source_hook_types) == 1:
            metadata["hook_type"] = next(iter(pending_source_hook_types))
    if pending_source_codex_events:
        metadata = {**metadata, "source_codex_events": source_codex_events, "source_codex_event_counts": source_codex_event_counts}
        if "codex_event" not in metadata and len(pending_source_codex_events) == 1:
            metadata["codex_event"] = next(iter(pending_source_codex_events))
    if source_memory_selection_policies:
        metadata = {
            **metadata,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            **source_memory_selection_retention,
        }
    storage_options = normalize_storage_options(args, metadata)
    if "node_path" not in metadata:
        metadata = {**metadata, "node_path": adapter.default_session_node_path(scope)}
    batch_result = adapter.batch_extract(
        {
            "messages": messages,
            "scope": scope,
            "metadata": metadata,
            "storage_options": storage_options,
            "threshold_messages": threshold,
            "force": True,
            "derive_from_existing_events": True,
            "source_event_ids": source_event_ids,
            "extraction_context_messages": extraction_context_messages,
            "extraction_context_event_ids": extraction_context_event_ids,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
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
    memory_layers_written = session_commit_memory_layers_written(
        batch_result,
        extraction_phase=extraction_phase,
        final_session_boundary=final_session_boundary,
        source_roles=source_roles,
        source_hook_types=source_hook_types,
        source_codex_events=source_codex_events,
    )
    if source_memory_selection_policies:
        memory_layers_written["source_memory_selection_policies"] = source_memory_selection_policies
        memory_layers_written["source_memory_selection_policy_counts"] = source_memory_selection_policy_counts
        memory_layers_written.update(source_memory_selection_retention)
    committed_at_ms = now_ms()
    adapter.append(
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
            "trigger_policy": trigger_policy,
            "extraction_phase": extraction_phase,
            "final_session_boundary": final_session_boundary,
            "pending_event_count_before_commit": pending_event_count,
            "pending_message_count_before_commit": pending_message_count,
            "committed_event_count": len(source_event_ids),
            "extraction_context_event_ids": extraction_context_event_ids,
            "extraction_context_event_count": len(extraction_context_event_ids),
            "source_roles": source_roles,
            "source_role_counts": source_role_counts,
            "source_hook_types": source_hook_types,
            "source_hook_type_counts": source_hook_type_counts,
            "source_codex_events": source_codex_events,
            "source_codex_event_counts": source_codex_event_counts,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            **source_memory_selection_retention,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "trigger_evidence": trigger_evidence,
            "memory_layers_written": memory_layers_written,
            "agent_hook": hook,
            "storage_options": storage_options,
            "storage_route": canonical_storage_route(storage_options),
            "created_at_ms": committed_at_ms,
        }
    )
    append_session_commit_task_progress(
        adapter,
        source_event_ids=source_event_ids,
        source_roles=source_roles,
        source_role_counts=source_role_counts,
        source_hook_types=source_hook_types,
        source_hook_type_counts=source_hook_type_counts,
        source_codex_events=source_codex_events,
        source_codex_event_counts=source_codex_event_counts,
        source_memory_selection_policies=source_memory_selection_policies,
        source_memory_selection_policy_counts=source_memory_selection_policy_counts,
        source_memory_selection_retention=source_memory_selection_retention,
        commit_id_hash=commit_id_hash,
        batch_id_hash=batch_result.get("batch_id_hash"),
        scope=scope,
        trigger_policy=trigger_policy,
        extraction_phase=extraction_phase,
        final_session_boundary=final_session_boundary,
        memory_layers_written=memory_layers_written,
        updated_at_ms=committed_at_ms,
    )
    return {
        **batch_result,
        "status": "committed",
        "commit_id_hash": commit_id_hash,
        "storage_options": storage_options,
        "storage_route": canonical_storage_route(storage_options),
        "pending_event_count": pending_event_count,
        "pending_message_count": pending_message_count,
        "committed_event_count": len(source_event_ids),
        "source_event_ids": source_event_ids,
        "extraction_context_event_ids": extraction_context_event_ids,
        "extraction_context_event_count": len(extraction_context_event_ids),
        "source_roles": source_roles,
        "source_role_counts": source_role_counts,
        "source_hook_types": source_hook_types,
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_events": source_codex_events,
        "source_codex_event_counts": source_codex_event_counts,
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
        **source_memory_selection_retention,
        "commit_reason": commit_reason,
        "trigger_policy": trigger_policy,
        "extraction_phase": extraction_phase,
        "final_session_boundary": final_session_boundary,
        "idle_timeout_ms": idle_timeout_ms,
        "idle_elapsed_ms": idle_elapsed_ms,
        "trigger_evidence": trigger_evidence,
        "memory_layers_written": memory_layers_written,
        "raw_events_duplicated": False,
    }
